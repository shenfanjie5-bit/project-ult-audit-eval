"""Formal audit/replay writer entrypoints."""

from __future__ import annotations

from collections.abc import Callable, Sequence
import os

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.audit.storage import (
    AuditPersistenceError,
    AuditStorageError,
    DEFAULT_MANAGED_AUDIT_TABLE,
    DEFAULT_MANAGED_REPLAY_TABLE,
    FormalAuditStorageAdapter,
    ManagedDuckDBFormalAuditStorageAdapter,
)
from audit_eval.contracts import AuditRecord, AuditWriteBundle, ReplayRecord

AUDIT_EVAL_DUCKDB_PATH_ENV = "AUDIT_EVAL_DUCKDB_PATH"
AUDIT_EVAL_AUDIT_TABLE_ENV = "AUDIT_EVAL_AUDIT_TABLE"
AUDIT_EVAL_REPLAY_TABLE_ENV = "AUDIT_EVAL_REPLAY_TABLE"


def get_default_storage_adapter() -> FormalAuditStorageAdapter:
    """Return configured formal audit storage, or fail closed."""

    duckdb_path = os.environ.get(AUDIT_EVAL_DUCKDB_PATH_ENV)
    if duckdb_path:
        return ManagedDuckDBFormalAuditStorageAdapter(
            duckdb_path,
            audit_table=os.environ.get(
                AUDIT_EVAL_AUDIT_TABLE_ENV,
                DEFAULT_MANAGED_AUDIT_TABLE,
            ),
            replay_table=os.environ.get(
                AUDIT_EVAL_REPLAY_TABLE_ENV,
                DEFAULT_MANAGED_REPLAY_TABLE,
            ),
        )

    raise AuditStorageError(
        "No default formal audit storage adapter is configured; pass storage=... "
        f"or set {AUDIT_EVAL_DUCKDB_PATH_ENV}"
    )


def persist_audit_records(
    write_bundle: AuditWriteBundle,
    storage: FormalAuditStorageAdapter | None = None,
) -> list[str]:
    """Persist formal AuditRecord rows through the configured storage adapter."""

    bundle = _revalidate_bundle(write_bundle)
    records = bundle.audit_records
    for index, record in enumerate(records):
        assert_no_forbidden_write(
            record.model_dump(mode="json"),
            path=f"$.audit_records[{index}]",
        )

    adapter = storage or get_default_storage_adapter()
    return _append_with_persistence_error(
        operation="append_audit_records",
        append=lambda: adapter.append_audit_records(records),
    )


def persist_replay_records(
    write_bundle: AuditWriteBundle,
    storage: FormalAuditStorageAdapter | None = None,
) -> list[str]:
    """Persist formal ReplayRecord rows through the configured storage adapter."""

    bundle = _revalidate_bundle(write_bundle)
    _validate_replay_records(bundle.replay_records, bundle.audit_records_by_id())

    records = bundle.replay_records
    for index, record in enumerate(records):
        assert_no_forbidden_write(
            record.model_dump(mode="json"),
            path=f"$.replay_records[{index}]",
        )

    adapter = storage or get_default_storage_adapter()
    return _append_with_persistence_error(
        operation="append_replay_records",
        append=lambda: adapter.append_replay_records(records),
    )


def _revalidate_bundle(write_bundle: AuditWriteBundle) -> AuditWriteBundle:
    return AuditWriteBundle.model_validate(write_bundle.model_dump(mode="python"))


def _validate_replay_records(
    replay_records: Sequence[ReplayRecord],
    audit_records_by_id: dict[str, AuditRecord],
) -> None:
    for replay_record in replay_records:
        if not replay_record.manifest_cycle_id:
            raise ValueError("ReplayRecord.manifest_cycle_id must not be empty")
        if replay_record.replay_mode != "read_history":
            raise ValueError('ReplayRecord.replay_mode must be "read_history"')

        missing_record_ids = [
            record_id
            for record_id in replay_record.audit_record_ids
            if record_id not in audit_records_by_id
        ]
        if missing_record_ids:
            missing = ", ".join(missing_record_ids)
            raise ValueError(
                "ReplayRecord.audit_record_ids reference missing "
                f"AuditRecord.record_id values: {missing}"
            )


def _append_with_persistence_error(
    operation: str,
    append: Callable[[], list[str]],
) -> list[str]:
    try:
        return append()
    except AuditPersistenceError:
        raise
    except Exception as exc:
        partial_ids = getattr(exc, "partial_ids", [])
        raise AuditPersistenceError(
            operation=operation,
            partial_ids=partial_ids,
            message=f"{operation} failed: {exc}",
        ) from exc


__all__ = [
    "AUDIT_EVAL_AUDIT_TABLE_ENV",
    "AUDIT_EVAL_DUCKDB_PATH_ENV",
    "AUDIT_EVAL_REPLAY_TABLE_ENV",
    "AuditPersistenceError",
    "get_default_storage_adapter",
    "persist_audit_records",
    "persist_replay_records",
]
