"""Storage and input boundaries for retrospective evaluation."""

from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from datetime import date
from typing import Protocol

from audit_eval.contracts.common import RetrospectiveHorizon
from audit_eval.contracts.retrospective import RetrospectiveEvaluation
from audit_eval.retro.schema import MarketOutcome, RetrospectiveTarget


class RetrospectiveStorageError(RuntimeError):
    """Raised when retrospective analytical storage is unavailable or fails."""


class RetrospectiveInputError(RuntimeError):
    """Raised when retrospective input data is unavailable or invalid."""


class RetrospectiveInputGateway(Protocol):
    """Input boundary for target discovery and realized market outcomes."""

    def list_targets(
        self,
        horizon: RetrospectiveHorizon,
        date_ref: date,
    ) -> Sequence[RetrospectiveTarget]:
        """Return cycle/object targets to evaluate for horizon/date_ref."""

    def load_market_outcome(
        self,
        target: RetrospectiveTarget,
        horizon: RetrospectiveHorizon,
        date_ref: date,
    ) -> MarketOutcome:
        """Return realized market outcome for one target/horizon/date_ref."""


class RetrospectiveEvaluationStorage(Protocol):
    """Append-only analytical storage boundary for retrospective evaluations."""

    def append_evaluations(
        self,
        evaluations: Sequence[RetrospectiveEvaluation],
    ) -> list[str]:
        """Append validated retrospective evaluations and return ids."""


class InMemoryRetrospectiveEvaluationStorage:
    """In-memory retrospective analytical storage for tests and Lite workflows."""

    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def append_evaluations(
        self,
        evaluations: Sequence[RetrospectiveEvaluation],
    ) -> list[str]:
        rows = [evaluation.model_dump(mode="json") for evaluation in evaluations]
        self.rows.extend(deepcopy(rows))
        return [evaluation.evaluation_id for evaluation in evaluations]


def get_default_input_gateway() -> RetrospectiveInputGateway:
    """Return configured retrospective input gateway, or fail closed."""

    raise RetrospectiveInputError(
        "No default retrospective input gateway is configured; "
        "pass input_gateway=..."
    )


def get_default_evaluation_storage() -> RetrospectiveEvaluationStorage:
    """Return configured retrospective analytical storage, or fail closed."""

    raise RetrospectiveStorageError(
        "No default retrospective evaluation storage is configured; pass storage=..."
    )


__all__ = [
    "InMemoryRetrospectiveEvaluationStorage",
    "RetrospectiveEvaluationStorage",
    "RetrospectiveInputError",
    "RetrospectiveInputGateway",
    "RetrospectiveStorageError",
    "get_default_evaluation_storage",
    "get_default_input_gateway",
]
