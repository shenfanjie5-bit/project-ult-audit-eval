"""Retrospective analytical computation interfaces."""

from audit_eval.retro.alert import (
    AlertLevel,
    AlertState,
    evaluate_cumulative_alert,
)
from audit_eval.retro.compute import (
    UnsupportedRetrospectiveHorizon,
    calculate_deviation,
    compute_retrospective,
    extract_retrospective_seed,
)
from audit_eval.retro.schema import (
    DeviationResult,
    MarketOutcome,
    RetroWindow,
    RetrospectiveSeed,
    RetrospectiveSummary,
    RetrospectiveTarget,
)
from audit_eval.retro.storage import (
    InMemoryRetrospectiveCurrentViewStorage,
    InMemoryRetrospectiveEvaluationReader,
    InMemoryRetrospectiveEvaluationStorage,
    RetrospectiveCurrentViewStorage,
    RetrospectiveEvaluationReader,
    RetrospectiveEvaluationStorage,
    RetrospectiveInputError,
    RetrospectiveInputGateway,
    RetrospectiveStorageError,
    get_default_current_view_storage,
    get_default_evaluation_reader,
    get_default_evaluation_storage,
    get_default_input_gateway,
)
from audit_eval.retro.summary import (
    RetrospectiveSummaryError,
    build_retrospective_summary,
)

__all__ = [
    "AlertLevel",
    "AlertState",
    "DeviationResult",
    "InMemoryRetrospectiveCurrentViewStorage",
    "InMemoryRetrospectiveEvaluationReader",
    "InMemoryRetrospectiveEvaluationStorage",
    "MarketOutcome",
    "RetroWindow",
    "RetrospectiveCurrentViewStorage",
    "RetrospectiveEvaluationReader",
    "RetrospectiveEvaluationStorage",
    "RetrospectiveInputError",
    "RetrospectiveInputGateway",
    "RetrospectiveSeed",
    "RetrospectiveSummary",
    "RetrospectiveSummaryError",
    "RetrospectiveStorageError",
    "RetrospectiveTarget",
    "UnsupportedRetrospectiveHorizon",
    "build_retrospective_summary",
    "calculate_deviation",
    "compute_retrospective",
    "evaluate_cumulative_alert",
    "extract_retrospective_seed",
    "get_default_current_view_storage",
    "get_default_evaluation_reader",
    "get_default_evaluation_storage",
    "get_default_input_gateway",
]
