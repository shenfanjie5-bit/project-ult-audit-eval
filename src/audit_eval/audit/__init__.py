"""Audit record persistence and replay query interfaces."""

from audit_eval.audit.errors import (
    AuditRecordMissing,
    DagsterSummaryMissing,
    GraphSnapshotMissing,
    ManifestBindingError,
    ReplayModeError,
    ReplayQueryError,
    ReplayRecordNotFound,
    SnapshotLoadError,
)
from audit_eval.audit.manifest_gateway import (
    FormalSnapshotGateway,
    ManifestGateway,
)
from audit_eval.audit.query import (
    DagsterRunGateway,
    GraphSnapshotGateway,
    ReplayQueryContext,
    ReplayRepository,
    replay_cycle_object,
)
from audit_eval.audit.replay_view import ReplayView
from audit_eval.audit.storage import (
    AuditPersistenceError,
    AuditStorageError,
    DuckDBFormalAuditStorageAdapter,
    FormalAuditStorageAdapter,
    InMemoryFormalAuditStorageAdapter,
)
from audit_eval.audit.lite import (
    InMemoryReplayRepository,
    build_in_memory_replay_query_context,
)
from audit_eval.audit.writer import (
    get_default_storage_adapter,
    persist_audit_records,
    persist_replay_records,
)

__all__ = [
    "AuditRecordMissing",
    "AuditPersistenceError",
    "AuditStorageError",
    "DagsterRunGateway",
    "DagsterSummaryMissing",
    "DuckDBFormalAuditStorageAdapter",
    "FormalSnapshotGateway",
    "FormalAuditStorageAdapter",
    "GraphSnapshotGateway",
    "GraphSnapshotMissing",
    "InMemoryFormalAuditStorageAdapter",
    "InMemoryReplayRepository",
    "ManifestBindingError",
    "ManifestGateway",
    "ReplayModeError",
    "ReplayQueryContext",
    "ReplayQueryError",
    "ReplayRecordNotFound",
    "ReplayRepository",
    "ReplayView",
    "SnapshotLoadError",
    "build_in_memory_replay_query_context",
    "get_default_storage_adapter",
    "persist_audit_records",
    "persist_replay_records",
    "replay_cycle_object",
]
