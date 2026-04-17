"""Storage adapter interfaces for formal audit/replay writes."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any, Protocol

from audit_eval.contracts.audit_record import AuditRecord
from audit_eval.contracts.replay_record import ReplayRecord


_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_MAX_RELATION_PARTS = 3


class AuditStorageError(RuntimeError):
    """Raised when formal audit storage is not configured or cannot append."""


class AuditPersistenceError(RuntimeError):
    """Raised when a formal audit persistence operation fails."""

    def __init__(
        self,
        operation: str,
        partial_ids: Sequence[str] | None = None,
        message: str | None = None,
    ) -> None:
        self.operation = operation
        self.partial_ids = list(partial_ids or [])
        detail = (
            f"{message or f'{operation} failed'} "
            f"(operation={operation}, partial_ids={self.partial_ids!r})"
        )
        super().__init__(detail)


class FormalAuditStorageAdapter(Protocol):
    """Append-only storage boundary owned by the data-platform integration."""

    def append_audit_records(self, records: Sequence[AuditRecord]) -> list[str]:
        """Append validated formal audit records and return persisted ids."""

    def append_replay_records(self, records: Sequence[ReplayRecord]) -> list[str]:
        """Append validated formal replay records and return persisted ids."""


class InMemoryFormalAuditStorageAdapter:
    """In-memory formal audit storage adapter for tests and Lite workflows."""

    def __init__(self) -> None:
        self.audit_rows: list[dict[str, Any]] = []
        self.replay_rows: list[dict[str, Any]] = []

    def append_audit_records(self, records: Sequence[AuditRecord]) -> list[str]:
        rows = [record.model_dump(mode="json") for record in records]
        self.audit_rows.extend(rows)
        return [record.record_id for record in records]

    def append_replay_records(self, records: Sequence[ReplayRecord]) -> list[str]:
        rows = [record.model_dump(mode="json") for record in records]
        self.replay_rows.extend(rows)
        return [record.replay_id for record in records]


class DuckDBFormalAuditStorageAdapter:
    """Append-only DuckDB adapter for existing formal audit/replay tables."""

    def __init__(
        self,
        connection: Any,
        audit_table: str,
        replay_table: str,
    ) -> None:
        if connection is None:
            raise AuditStorageError("DuckDB connection must be provided")
        if not audit_table:
            raise AuditStorageError("audit_table must be provided")
        if not replay_table:
            raise AuditStorageError("replay_table must be provided")

        self.connection = connection
        self.audit_table = _quote_relation_name(audit_table, "audit_table")
        self.replay_table = _quote_relation_name(replay_table, "replay_table")

    def append_audit_records(self, records: Sequence[AuditRecord]) -> list[str]:
        rows = [record.model_dump(mode="json") for record in records]
        self._append_rows(self.audit_table, rows)
        return [record.record_id for record in records]

    def append_replay_records(self, records: Sequence[ReplayRecord]) -> list[str]:
        rows = [record.model_dump(mode="json") for record in records]
        self._append_rows(self.replay_table, rows)
        return [record.replay_id for record in records]

    def _append_rows(self, table_name: str, rows: Sequence[dict[str, Any]]) -> None:
        if not rows:
            return

        columns = tuple(rows[0])
        column_sql = ", ".join(_quote_identifier(column) for column in columns)
        placeholders = ", ".join("?" for _ in columns)
        sql = f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})"
        parameters = [tuple(row[column] for column in columns) for row in rows]

        try:
            self.connection.executemany(sql, parameters)
        except Exception as exc:  # pragma: no cover - exercised through writer tests
            raise AuditStorageError(f"Failed to append rows to {table_name}") from exc


def _quote_identifier(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _quote_relation_name(relation_name: str, config_name: str) -> str:
    parts = relation_name.split(".")
    if (
        not parts
        or len(parts) > _MAX_RELATION_PARTS
        or any(not _IDENTIFIER_RE.fullmatch(part) for part in parts)
    ):
        raise AuditStorageError(
            f"{config_name} must be 1-{_MAX_RELATION_PARTS} dot-separated "
            "SQL identifiers matching [A-Za-z_][A-Za-z0-9_]*"
        )
    return ".".join(_quote_identifier(part) for part in parts)


__all__ = [
    "AuditPersistenceError",
    "AuditStorageError",
    "DuckDBFormalAuditStorageAdapter",
    "FormalAuditStorageAdapter",
    "InMemoryFormalAuditStorageAdapter",
]
