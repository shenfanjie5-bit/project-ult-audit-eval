"""Storage and input boundaries for retrospective evaluation."""

from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import date
from threading import Lock
from typing import Protocol

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.contracts.common import RetrospectiveHorizon
from audit_eval.contracts.retrospective import RetrospectiveEvaluation
from audit_eval.retro.alert import AlertState
from audit_eval.retro.dates import filter_evaluations_for_window
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


@dataclass(frozen=True)
class RetrospectiveEvaluationWriteResult:
    """Atomic retrospective evaluation write accounting."""

    written_evaluation_ids: tuple[str, ...]
    skipped_existing_ids: tuple[str, ...]


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
    """Analytical storage boundary for retrospective evaluations."""

    def append_evaluations(
        self,
        evaluations: Sequence[RetrospectiveEvaluation],
    ) -> list[str]:
        """Append validated retrospective evaluations and return ids."""

    def upsert_evaluations_by_id(
        self,
        evaluations: Sequence[RetrospectiveEvaluation],
    ) -> RetrospectiveEvaluationWriteResult:
        """Atomically insert missing evaluations by evaluation_id."""


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
        self._lock = Lock()

    def append_evaluations(
        self,
        evaluations: Sequence[RetrospectiveEvaluation],
    ) -> list[str]:
        rows = [evaluation.model_dump(mode="json") for evaluation in evaluations]
        for index, row in enumerate(rows):
            assert_no_forbidden_write(row, path=f"$.evaluations[{index}]")
        with self._lock:
            self.rows.extend(deepcopy(rows))
        return [evaluation.evaluation_id for evaluation in evaluations]

    def upsert_evaluations_by_id(
        self,
        evaluations: Sequence[RetrospectiveEvaluation],
    ) -> RetrospectiveEvaluationWriteResult:
        rows = [evaluation.model_dump(mode="json") for evaluation in evaluations]
        for index, row in enumerate(rows):
            assert_no_forbidden_write(row, path=f"$.evaluations[{index}]")

        written_ids: list[str] = []
        skipped_ids: list[str] = []
        rows_to_write: list[dict[str, object]] = []
        with self._lock:
            existing_ids = {
                row["evaluation_id"]
                for row in self.rows
                if isinstance(row.get("evaluation_id"), str)
            }
            for evaluation, row in zip(evaluations, rows, strict=True):
                evaluation_id = evaluation.evaluation_id
                if evaluation_id in existing_ids:
                    skipped_ids.append(evaluation_id)
                    continue
                existing_ids.add(evaluation_id)
                written_ids.append(evaluation_id)
                rows_to_write.append(row)
            self.rows.extend(deepcopy(rows_to_write))

        return RetrospectiveEvaluationWriteResult(
            written_evaluation_ids=tuple(written_ids),
            skipped_existing_ids=tuple(skipped_ids),
        )

    def load_evaluations(
        self,
        window: RetroWindow,
    ) -> list[RetrospectiveEvaluation]:
        with self._lock:
            rows = deepcopy(self.rows)
        evaluations = [
            RetrospectiveEvaluation.model_validate(row) for row in rows
        ]
        return filter_evaluations_for_window(evaluations, window)


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
        return filter_evaluations_for_window(self.evaluations, window)


class InMemoryRetrospectiveCurrentViewStorage:
    """In-memory current-view storage for summary tests and Lite workflows."""

    def __init__(self) -> None:
        self.summary_rows: list[dict[str, object]] = []
        self.alert_state_rows: list[dict[str, object]] = []
        self._summary_keys: list[tuple[object, ...]] = []
        self._alert_state_keys: list[tuple[object, ...]] = []

    def upsert_summary_and_alert_state(
        self,
        summary: RetrospectiveSummary,
        alert_state: AlertState,
    ) -> tuple[str, str]:
        summary_rows_snapshot = deepcopy(self.summary_rows)
        alert_state_rows_snapshot = deepcopy(self.alert_state_rows)
        summary_keys_snapshot = deepcopy(self._summary_keys)
        alert_state_keys_snapshot = deepcopy(self._alert_state_keys)
        try:
            summary_id = self.upsert_summary(summary)
            self._active_summary_key = (summary.date_window, summary.horizon)
            try:
                alert_state_id = self.upsert_alert_state(alert_state)
            finally:
                del self._active_summary_key
        except Exception:
            self.summary_rows = summary_rows_snapshot
            self.alert_state_rows = alert_state_rows_snapshot
            self._summary_keys = summary_keys_snapshot
            self._alert_state_keys = alert_state_keys_snapshot
            raise
        return summary_id, alert_state_id

    def upsert_summary(self, summary: RetrospectiveSummary) -> str:
        return self._upsert_summary(summary)

    def _upsert_summary(self, summary: RetrospectiveSummary) -> str:
        row = deepcopy(asdict(summary))
        assert_no_forbidden_write(row, path="$.summary")
        _upsert_row(
            self.summary_rows,
            self._summary_keys,
            row,
            key=(summary.date_window, summary.horizon),
        )
        return summary.date_window

    def upsert_alert_state(self, alert_state: AlertState) -> str:
        summary_key = getattr(self, "_active_summary_key", None)
        return self._upsert_alert_state(alert_state, summary_key=summary_key)

    def _upsert_alert_state(
        self,
        alert_state: AlertState,
        *,
        summary_key: tuple[str, str] | None = None,
    ) -> str:
        row = deepcopy(asdict(alert_state))
        assert_no_forbidden_write(row, path="$.alert_state")
        alert_state_id = (
            "alert-"
            f"{alert_state.window_start.isoformat()}-"
            f"{alert_state.window_end.isoformat()}"
        )
        _upsert_row(
            self.alert_state_rows,
            self._alert_state_keys,
            row,
            key=summary_key or (alert_state_id,),
        )
        return alert_state_id


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


def _upsert_row(
    rows: list[dict[str, object]],
    keys: list[tuple[object, ...]],
    row: dict[str, object],
    *,
    key: tuple[object, ...],
) -> None:
    for index, existing_key in enumerate(keys):
        if existing_key == key:
            rows[index] = deepcopy(row)
            return
    rows.append(deepcopy(row))
    keys.append(key)


__all__ = [
    "InMemoryRetrospectiveCurrentViewStorage",
    "InMemoryRetrospectiveEvaluationReader",
    "InMemoryRetrospectiveEvaluationStorage",
    "RetrospectiveCurrentViewStorage",
    "RetrospectiveEvaluationReader",
    "RetrospectiveEvaluationStorage",
    "RetrospectiveEvaluationWriteResult",
    "RetrospectiveInputError",
    "RetrospectiveInputGateway",
    "RetrospectiveStorageError",
    "get_default_current_view_storage",
    "get_default_evaluation_reader",
    "get_default_evaluation_storage",
    "get_default_input_gateway",
]
