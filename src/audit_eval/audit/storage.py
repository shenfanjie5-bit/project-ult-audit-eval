"""Formal audit/replay storage adapter boundaries."""

from __future__ import annotations

import os
import re
from collections.abc import Sequence
from typing import Any, Protocol

from audit_eval.contracts.audit_record import AuditRecord
from audit_eval.contracts.replay_record import ReplayRecord


class AuditStorageError(RuntimeError):
    """Raised when no explicit formal audit storage adapter is available."""


class AuditPersistenceError(RuntimeError):
    """Raised when a storage adapter fails during a persistence operation."""

    def __init__(
        self,
        operation: str,
        partial_ids: Sequence[str] | None = None,
        message: str | None = None,
    ) -> None:
        self.operation = operation
        self.partial_ids = list(partial_ids or [])
        detail = message or f"{operation} failed"
        super().__init__(
            f"{detail} (operation={operation}, partial_ids={self.partial_ids})"
        )


class FormalAuditStorageAdapter(Protocol):
    """Append-only boundary for data-platform owned formal audit tables."""

    def append_audit_records(self, records: Sequence[AuditRecord]) -> list[str]:
        """Append formal audit records and return persisted record IDs."""

    def append_replay_records(self, records: Sequence[ReplayRecord]) -> list[str]:
        """Append formal replay records and return persisted replay IDs."""


class InMemoryFormalAuditStorageAdapter:
    """In-memory formal audit adapter for tests and Lite-mode fixtures."""

    def __init__(self) -> None:
        self.audit_rows: list[dict[str, Any]] = []
        self.replay_rows: list[dict[str, Any]] = []

    def append_audit_records(self, records: Sequence[AuditRecord]) -> list[str]:
        rows = [record.model_dump(mode="json") for record in records]
        self.audit_rows.extend(rows)
        return [row["record_id"] for row in rows]

    def append_replay_records(self, records: Sequence[ReplayRecord]) -> list[str]:
        rows = [record.model_dump(mode="json") for record in records]
        self.replay_rows.extend(rows)
        return [row["replay_id"] for row in rows]


class DuckDBFormalAuditStorageAdapter:
    """Append-only DuckDB adapter for existing data-platform owned tables."""

    def __init__(self, connection: Any, audit_table: str, replay_table: str) -> None:
        self.connection = connection
        self.audit_table = audit_table
        self.replay_table = replay_table

    def append_audit_records(self, records: Sequence[AuditRecord]) -> list[str]:
        rows = [record.model_dump(mode="json") for record in records]
        self._append_rows(self.audit_table, rows, AuditRecord.model_fields)
        return [record.record_id for record in records]

    def append_replay_records(self, records: Sequence[ReplayRecord]) -> list[str]:
        rows = [record.model_dump(mode="json") for record in records]
        self._append_rows(self.replay_table, rows, ReplayRecord.model_fields)
        return [record.replay_id for record in records]

    def _append_rows(
        self,
        table_name: str,
        rows: Sequence[dict[str, Any]],
        columns: dict[str, Any],
    ) -> None:
        if not rows:
            return

        column_names = list(columns)
        quoted_columns = ", ".join(_quote_identifier(column) for column in column_names)
        placeholders = ", ".join("?" for _ in column_names)
        sql = (
            f"INSERT INTO {_quote_table_name(table_name)} "
            f"({quoted_columns}) VALUES ({placeholders})"
        )
        values = [tuple(row[column] for column in column_names) for row in rows]
        self.connection.executemany(sql, values)


def get_default_storage_adapter() -> FormalAuditStorageAdapter:
    """Build the default adapter only from explicit local configuration."""

    database = os.getenv("AUDIT_EVAL_DUCKDB_DATABASE")
    audit_table = os.getenv("AUDIT_EVAL_AUDIT_TABLE")
    replay_table = os.getenv("AUDIT_EVAL_REPLAY_TABLE")
    if not database or not audit_table or not replay_table:
        raise AuditStorageError(
            "No default audit storage adapter configured. Provide an explicit "
            "storage adapter or set AUDIT_EVAL_DUCKDB_DATABASE, "
            "AUDIT_EVAL_AUDIT_TABLE, and AUDIT_EVAL_REPLAY_TABLE."
        )

    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - depends on optional runtime
        raise AuditStorageError(
            "AUDIT_EVAL_DUCKDB_DATABASE is configured but duckdb is not installed"
        ) from exc

    return DuckDBFormalAuditStorageAdapter(
        connection=duckdb.connect(database),
        audit_table=audit_table,
        replay_table=replay_table,
    )


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_table_name(table_name: str) -> str:
    parts = table_name.split(".")
    if not parts or any(not part for part in parts):
        raise AuditStorageError(f"Invalid DuckDB table name: {table_name!r}")
    return ".".join(_quote_identifier(part) for part in parts)


def _quote_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(identifier):
        raise AuditStorageError(f"Invalid DuckDB identifier: {identifier!r}")
    return f'"{identifier}"'


__all__ = [
    "AuditPersistenceError",
    "AuditStorageError",
    "DuckDBFormalAuditStorageAdapter",
    "FormalAuditStorageAdapter",
    "InMemoryFormalAuditStorageAdapter",
    "get_default_storage_adapter",
]
