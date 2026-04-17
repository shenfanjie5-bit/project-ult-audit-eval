"""Typed errors for read-history replay queries."""


class ReplayQueryError(RuntimeError):
    """Base class for replay query failures."""


class ReplayRecordNotFound(ReplayQueryError):
    """Raised when no replay_record row exists for the requested object."""


class ReplayModeError(ReplayQueryError):
    """Raised when a replay_record is not configured for read-history replay."""


class AuditRecordMissing(ReplayQueryError):
    """Raised when a replay_record references unavailable audit_record rows."""


class ManifestBindingError(ReplayQueryError):
    """Raised when replay_record bindings do not match the publish manifest."""


class SnapshotLoadError(ReplayQueryError):
    """Raised when a manifest-bound formal snapshot cannot be loaded."""


class DagsterSummaryMissing(ReplayQueryError):
    """Raised when the Dagster run summary required for replay is unavailable."""


class GraphSnapshotMissing(ReplayQueryError):
    """Raised when the graph snapshot required for replay is unavailable."""


__all__ = [
    "AuditRecordMissing",
    "DagsterSummaryMissing",
    "GraphSnapshotMissing",
    "ManifestBindingError",
    "ReplayModeError",
    "ReplayQueryError",
    "ReplayRecordNotFound",
    "SnapshotLoadError",
]
