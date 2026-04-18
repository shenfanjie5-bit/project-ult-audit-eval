"""Storage and input boundaries for retrospective evaluation."""

from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from dataclasses import asdict
from datetime import date
from typing import Protocol

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.contracts.common import RetrospectiveHorizon
from audit_eval.contracts.retrospective import RetrospectiveEvaluation
from audit_eval.retro.alert import AlertState
from audit_eval.retro.schema import (
    MarketOutcome,
    RetroWindow,
    RetrospectiveSummary,
    RetrospectiveTarget,
)


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


class RetrospectiveEvaluationReader(Protocol):
    """Read boundary for analytical retrospective evaluations."""

    def load_evaluations(
        self,
        window: RetroWindow,
    ) -> list[RetrospectiveEvaluation]:
        """Load retrospective evaluations for a bounded summary window."""


class RetrospectiveCurrentViewStorage(Protocol):
    """Current-view write boundary for summaries and cumulative alerts."""

    def upsert_summary_and_alert_state(
        self,
        summary: RetrospectiveSummary,
        alert_state: AlertState,
    ) -> tuple[str, str]:
        """Atomically upsert summary and alert state, committing both or neither."""


class InMemoryRetrospectiveEvaluationStorage:
    """In-memory retrospective analytical storage for tests and Lite workflows."""

    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def append_evaluations(
        self,
        evaluations: Sequence[RetrospectiveEvaluation],
    ) -> list[str]:
        rows = [evaluation.model_dump(mode="json") for evaluation in evaluations]
        for index, row in enumerate(rows):
            assert_no_forbidden_write(row, path=f"$.evaluations[{index}]")
        self.rows.extend(deepcopy(rows))
        return [evaluation.evaluation_id for evaluation in evaluations]

    def load_evaluations(
        self,
        window: RetroWindow,
    ) -> list[RetrospectiveEvaluation]:
        evaluations = [
            RetrospectiveEvaluation.model_validate(row) for row in self.rows
        ]
        return _filter_evaluations(evaluations, window)


class InMemoryRetrospectiveEvaluationReader:
    """In-memory retrospective reader for summary tests and Lite workflows."""

    def __init__(self, evaluations: Sequence[RetrospectiveEvaluation]) -> None:
        self.evaluations = list(evaluations)
        self.loaded_windows: list[RetroWindow] = []

    def load_evaluations(
        self,
        window: RetroWindow,
    ) -> list[RetrospectiveEvaluation]:
        self.loaded_windows.append(window)
        return _filter_evaluations(self.evaluations, window)


class InMemoryRetrospectiveCurrentViewStorage:
    """In-memory current-view storage for summary tests and Lite workflows."""

    def __init__(self) -> None:
        self.summary_rows: list[dict[str, object]] = []
        self.alert_state_rows: list[dict[str, object]] = []

    def upsert_summary_and_alert_state(
        self,
        summary: RetrospectiveSummary,
        alert_state: AlertState,
    ) -> tuple[str, str]:
        summary_rows_snapshot = deepcopy(self.summary_rows)
        alert_state_rows_snapshot = deepcopy(self.alert_state_rows)
        try:
            summary_id = self.upsert_summary(summary)
            alert_state_id = self.upsert_alert_state(alert_state)
        except Exception:
            self.summary_rows = summary_rows_snapshot
            self.alert_state_rows = alert_state_rows_snapshot
            raise
        return summary_id, alert_state_id

    def upsert_summary(self, summary: RetrospectiveSummary) -> str:
        row = deepcopy(asdict(summary))
        assert_no_forbidden_write(row, path="$.summary")
        self.summary_rows.append(row)
        return summary.date_window

    def upsert_alert_state(self, alert_state: AlertState) -> str:
        row = deepcopy(asdict(alert_state))
        assert_no_forbidden_write(row, path="$.alert_state")
        self.alert_state_rows.append(row)
        return (
            "alert-"
            f"{alert_state.window_start.isoformat()}-"
            f"{alert_state.window_end.isoformat()}"
        )


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


def get_default_evaluation_reader() -> RetrospectiveEvaluationReader:
    """Return configured retrospective reader, or fail closed."""

    raise RetrospectiveStorageError(
        "No default retrospective evaluation reader is configured; pass reader=..."
    )


def get_default_current_view_storage() -> RetrospectiveCurrentViewStorage:
    """Return configured retrospective current-view storage, or fail closed."""

    raise RetrospectiveStorageError(
        "No default retrospective current-view storage is configured; "
        "pass current_view=..."
    )


def _filter_evaluations(
    evaluations: Sequence[RetrospectiveEvaluation],
    window: RetroWindow,
) -> list[RetrospectiveEvaluation]:
    return [
        evaluation
        for evaluation in evaluations
        if evaluation.horizon == window.horizon
        and window.start <= evaluation.evaluated_at.date() <= window.end
        and (
            window.object_ref is None
            or evaluation.object_ref == window.object_ref
        )
    ]


__all__ = [
    "InMemoryRetrospectiveCurrentViewStorage",
    "InMemoryRetrospectiveEvaluationReader",
    "InMemoryRetrospectiveEvaluationStorage",
    "RetrospectiveCurrentViewStorage",
    "RetrospectiveEvaluationReader",
    "RetrospectiveEvaluationStorage",
    "RetrospectiveInputError",
    "RetrospectiveInputGateway",
    "RetrospectiveStorageError",
    "get_default_current_view_storage",
    "get_default_evaluation_reader",
    "get_default_evaluation_storage",
    "get_default_input_gateway",
]
