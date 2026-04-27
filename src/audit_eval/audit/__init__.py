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
from audit_eval.audit.real_cycle import (
    DataPlatformBindingError,
    DataPlatformFormalSnapshotGateway,
    DataPlatformManifestGateway,
    RealCycleSmokeDagsterGateway,
    RealCycleSmokeRepository,
    build_data_platform_replay_query_context,
    data_platform_snapshot_ref,
    formal_object_ref,
    parse_data_platform_snapshot_ref,
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
    "DataPlatformBindingError",
    "DataPlatformFormalSnapshotGateway",
    "DataPlatformManifestGateway",
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
    "RealCycleSmokeDagsterGateway",
    "RealCycleSmokeRepository",
    "SnapshotLoadError",
    "build_in_memory_replay_query_context",
    "build_data_platform_replay_query_context",
    "data_platform_snapshot_ref",
    "formal_object_ref",
    "get_default_storage_adapter",
    "parse_data_platform_snapshot_ref",
    "persist_audit_records",
    "persist_replay_records",
    "replay_cycle_object",
]
