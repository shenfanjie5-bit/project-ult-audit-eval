"""Audit record and replay record persistence interfaces."""

from audit_eval.audit.storage import (
    AuditPersistenceError,
    AuditStorageError,
    DuckDBFormalAuditStorageAdapter,
    FormalAuditStorageAdapter,
    InMemoryFormalAuditStorageAdapter,
    get_default_storage_adapter,
)
from audit_eval.audit.writer import persist_audit_records, persist_replay_records

__all__ = [
    "AuditPersistenceError",
    "AuditStorageError",
    "DuckDBFormalAuditStorageAdapter",
    "FormalAuditStorageAdapter",
    "InMemoryFormalAuditStorageAdapter",
    "get_default_storage_adapter",
    "persist_audit_records",
    "persist_replay_records",
]
