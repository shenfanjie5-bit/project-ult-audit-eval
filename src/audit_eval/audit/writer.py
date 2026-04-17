"""Formal audit/replay persistence writer functions."""

from __future__ import annotations

from collections.abc import Sequence

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.audit.storage import (
    AuditPersistenceError,
    FormalAuditStorageAdapter,
    get_default_storage_adapter,
)
from audit_eval.contracts.replay_record import ReplayRecord
from audit_eval.contracts.write_bundle import AuditWriteBundle


def persist_audit_records(
    write_bundle: AuditWriteBundle,
    storage: FormalAuditStorageAdapter | None = None,
) -> list[str]:
    """Persist formal audit records through an explicit storage adapter."""

    validated_bundle = _revalidate_write_bundle(write_bundle)
    _assert_write_bundle_boundary(validated_bundle)
    _validate_write_bundle_for_persistence(validated_bundle)
    records = list(validated_bundle.audit_records)

    adapter = storage if storage is not None else get_default_storage_adapter()
    try:
        return adapter.append_audit_records(records)
    except Exception as exc:
        raise _persistence_error("append_audit_records", exc) from exc


def persist_replay_records(
    write_bundle: AuditWriteBundle,
    storage: FormalAuditStorageAdapter | None = None,
) -> list[str]:
    """Persist formal replay records through an explicit storage adapter."""

    validated_bundle = _revalidate_write_bundle(write_bundle)
    _assert_write_bundle_boundary(validated_bundle)
    _validate_write_bundle_for_persistence(validated_bundle)
    records = list(validated_bundle.replay_records)

    adapter = storage if storage is not None else get_default_storage_adapter()
    try:
        return adapter.append_replay_records(records)
    except Exception as exc:
        raise _persistence_error("append_replay_records", exc) from exc


def _revalidate_write_bundle(write_bundle: AuditWriteBundle) -> AuditWriteBundle:
    return AuditWriteBundle.model_validate(write_bundle.model_dump(mode="python"))


def _assert_write_bundle_boundary(write_bundle: AuditWriteBundle) -> None:
    assert_no_forbidden_write(write_bundle.model_dump(mode="json"))


def _validate_write_bundle_for_persistence(write_bundle: AuditWriteBundle) -> None:
    audit_record_ids = set(write_bundle.audit_records_by_id())
    for record in write_bundle.replay_records:
        _validate_replay_record_for_write(
            replay_record=record,
            bundle_manifest_cycle_id=write_bundle.manifest_cycle_id,
            audit_record_ids=audit_record_ids,
        )


def _validate_replay_record_for_write(
    replay_record: ReplayRecord,
    bundle_manifest_cycle_id: str,
    audit_record_ids: set[str],
) -> None:
    if not replay_record.manifest_cycle_id:
        raise ValueError("ReplayRecord.manifest_cycle_id must not be empty")
    if replay_record.manifest_cycle_id != bundle_manifest_cycle_id:
        raise ValueError(
            "ReplayRecord.manifest_cycle_id must match "
            "AuditWriteBundle.manifest_cycle_id"
        )
    if replay_record.replay_mode != "read_history":
        raise ValueError("ReplayRecord.replay_mode must be read_history")

    missing_record_ids = [
        record_id
        for record_id in replay_record.audit_record_ids
        if record_id not in audit_record_ids
    ]
    if missing_record_ids:
        missing = ", ".join(missing_record_ids)
        raise ValueError(
            "ReplayRecord.audit_record_ids reference missing "
            f"AuditRecord.record_id values: {missing}"
        )


def _persistence_error(operation: str, exc: Exception) -> AuditPersistenceError:
    if isinstance(exc, AuditPersistenceError):
        return exc
    partial_ids = getattr(exc, "partial_ids", [])
    if not isinstance(partial_ids, Sequence) or isinstance(partial_ids, (str, bytes)):
        partial_ids = []
    return AuditPersistenceError(
        operation=operation,
        partial_ids=list(partial_ids),
        message=str(exc) or None,
    )


__all__ = [
    "AuditPersistenceError",
    "persist_audit_records",
    "persist_replay_records",
]
