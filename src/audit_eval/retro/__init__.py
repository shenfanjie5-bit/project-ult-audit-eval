"""Retrospective analytical computation interfaces."""

from audit_eval.retro.compute import (
    UnsupportedRetrospectiveHorizon,
    calculate_deviation,
    compute_retrospective,
    extract_retrospective_seed,
)
from audit_eval.retro.schema import (
    DeviationResult,
    MarketOutcome,
    RetrospectiveSeed,
    RetrospectiveTarget,
)
from audit_eval.retro.storage import (
    InMemoryRetrospectiveEvaluationStorage,
    RetrospectiveEvaluationStorage,
    RetrospectiveInputError,
    RetrospectiveInputGateway,
    RetrospectiveStorageError,
    get_default_evaluation_storage,
    get_default_input_gateway,
)

__all__ = [
    "DeviationResult",
    "InMemoryRetrospectiveEvaluationStorage",
    "MarketOutcome",
    "RetrospectiveEvaluationStorage",
    "RetrospectiveInputError",
    "RetrospectiveInputGateway",
    "RetrospectiveSeed",
    "RetrospectiveStorageError",
    "RetrospectiveTarget",
    "UnsupportedRetrospectiveHorizon",
    "calculate_deviation",
    "compute_retrospective",
    "extract_retrospective_seed",
    "get_default_evaluation_storage",
    "get_default_input_gateway",
]
