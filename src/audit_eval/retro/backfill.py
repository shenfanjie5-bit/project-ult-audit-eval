"""Retrospective multi-horizon backfill orchestration."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import date
from typing import cast

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.audit.query import ReplayQueryContext
from audit_eval.contracts.common import RetrospectiveHorizon
from audit_eval.contracts.retrospective import RetrospectiveEvaluation
from audit_eval.retro.compute import compute_retrospective
from audit_eval.retro.horizon import HORIZONS, horizon_to_days, require_mature_horizon
from audit_eval.retro.schema import MarketOutcome, RetroWindow, RetrospectiveTarget
from audit_eval.retro.storage import (
    RetrospectiveEvaluationReader,
    RetrospectiveEvaluationStorage,
    RetrospectiveInputGateway,
    get_default_evaluation_reader,
    get_default_evaluation_storage,
    get_default_input_gateway,
)


@dataclass(frozen=True)
class RetrospectiveJob:
    """Backfill request for one business date and a set of horizons."""

    date_ref: date
    horizons: tuple[RetrospectiveHorizon, ...] = HORIZONS
    object_ref: str | None = None


@dataclass(frozen=True)
class HorizonCoverageReport:
    """Coverage status for object/horizon retrospective evaluations."""

    expected_horizons: tuple[RetrospectiveHorizon, ...]
    object_refs: tuple[str, ...]
    covered_horizons_by_object: dict[str, tuple[RetrospectiveHorizon, ...]]
    missing_horizons_by_object: dict[str, tuple[RetrospectiveHorizon, ...]]
    covered_count: int
    expected_count: int
    coverage_ratio: float
    is_complete: bool


@dataclass(frozen=True)
class RetrospectiveBackfillResult:
    """Result of one idempotent retrospective backfill run."""

    job: RetrospectiveJob
    written_evaluation_ids: tuple[str, ...]
    skipped_existing_ids: tuple[str, ...]
    coverage: HorizonCoverageReport


def run_backfill(
    date_ref: date,
    horizons: Sequence[RetrospectiveHorizon] = HORIZONS,
    *,
    replay_context: ReplayQueryContext | None = None,
    input_gateway: RetrospectiveInputGateway | None = None,
    storage: RetrospectiveEvaluationStorage | None = None,
    reader: RetrospectiveEvaluationReader | None = None,
    as_of_date: date | None = None,
) -> RetrospectiveBackfillResult:
    """Run an idempotent backfill for all requested mature horizons."""

    requested_horizons = _normalize_horizons(horizons)
    effective_as_of_date = as_of_date or date.today()
    for horizon in requested_horizons:
        require_mature_horizon(horizon, date_ref, effective_as_of_date)

    gateway = input_gateway or get_default_input_gateway()
    evaluation_storage = storage or get_default_evaluation_storage()
    evaluation_reader = _resolve_reader(reader, evaluation_storage)

    targets_by_horizon = _load_targets_by_horizon(
        gateway,
        requested_horizons,
        date_ref,
    )
    existing_evaluations_by_id = _load_existing_evaluations_by_id(
        evaluation_reader,
        targets_by_horizon,
    )
    missing_targets_by_horizon: dict[
        RetrospectiveHorizon, list[RetrospectiveTarget]
    ] = {horizon: [] for horizon in requested_horizons}
    skipped_existing_ids: list[str] = []

    for horizon in requested_horizons:
        for target in targets_by_horizon[horizon]:
            evaluation_id = _evaluation_id(target, horizon)
            if evaluation_id in existing_evaluations_by_id:
                skipped_existing_ids.append(evaluation_id)
            else:
                missing_targets_by_horizon[horizon].append(target)

    collector = _CollectingEvaluationStorage()
    filtered_gateway = _BackfillInputGateway(
        delegate=gateway,
        targets_by_horizon=missing_targets_by_horizon,
    )
    for horizon in requested_horizons:
        if not missing_targets_by_horizon[horizon]:
            continue
        compute_retrospective(
            horizon,
            date_ref,
            replay_context=replay_context,
            input_gateway=filtered_gateway,
            storage=collector,
            as_of_date=effective_as_of_date,
        )

    if collector.evaluations:
        written_evaluation_ids = tuple(
            evaluation_storage.append_evaluations(collector.evaluations)
        )
    else:
        written_evaluation_ids = ()

    object_refs = _object_refs_for_targets(targets_by_horizon, requested_horizons)
    coverage = check_horizon_coverage(
        [
            *existing_evaluations_by_id.values(),
            *collector.evaluations,
        ],
        expected_horizons=requested_horizons,
        object_refs=object_refs,
    )
    return RetrospectiveBackfillResult(
        job=RetrospectiveJob(date_ref=date_ref, horizons=requested_horizons),
        written_evaluation_ids=written_evaluation_ids,
        skipped_existing_ids=tuple(skipped_existing_ids),
        coverage=coverage,
    )


def check_horizon_coverage(
    evaluations: Sequence[RetrospectiveEvaluation],
    *,
    expected_horizons: Sequence[RetrospectiveHorizon] = HORIZONS,
    object_refs: Sequence[str] | None = None,
) -> HorizonCoverageReport:
    """Report whether each object_ref has all expected horizon evaluations."""

    requested_horizons = _normalize_horizons(expected_horizons)
    requested_horizon_set = set(requested_horizons)
    if object_refs is None:
        report_object_refs = tuple(
            sorted({evaluation.object_ref for evaluation in evaluations})
        )
    else:
        report_object_refs = tuple(dict.fromkeys(object_refs))

    covered_by_object: dict[str, tuple[RetrospectiveHorizon, ...]] = {}
    missing_by_object: dict[str, tuple[RetrospectiveHorizon, ...]] = {}
    for object_ref in report_object_refs:
        present_horizons = {
            evaluation.horizon
            for evaluation in evaluations
            if evaluation.object_ref == object_ref
            and evaluation.horizon in requested_horizon_set
        }
        covered = tuple(
            horizon for horizon in requested_horizons if horizon in present_horizons
        )
        missing = tuple(
            horizon for horizon in requested_horizons if horizon not in present_horizons
        )
        covered_by_object[object_ref] = covered
        missing_by_object[object_ref] = missing

    expected_count = len(report_object_refs) * len(requested_horizons)
    covered_count = sum(len(horizons) for horizons in covered_by_object.values())
    coverage_ratio = 1.0 if expected_count == 0 else covered_count / expected_count
    return HorizonCoverageReport(
        expected_horizons=requested_horizons,
        object_refs=report_object_refs,
        covered_horizons_by_object=covered_by_object,
        missing_horizons_by_object=missing_by_object,
        covered_count=covered_count,
        expected_count=expected_count,
        coverage_ratio=coverage_ratio,
        is_complete=covered_count == expected_count,
    )


class _CollectingEvaluationStorage:
    def __init__(self) -> None:
        self.evaluations: list[RetrospectiveEvaluation] = []

    def append_evaluations(
        self,
        evaluations: Sequence[RetrospectiveEvaluation],
    ) -> list[str]:
        self.evaluations.extend(evaluations)
        return [evaluation.evaluation_id for evaluation in evaluations]


@dataclass(frozen=True)
class _BackfillInputGateway:
    delegate: RetrospectiveInputGateway
    targets_by_horizon: dict[RetrospectiveHorizon, list[RetrospectiveTarget]]

    def list_targets(
        self,
        horizon: RetrospectiveHorizon,
        date_ref: date,
    ) -> Sequence[RetrospectiveTarget]:
        return self.targets_by_horizon[horizon]

    def load_market_outcome(
        self,
        target: RetrospectiveTarget,
        horizon: RetrospectiveHorizon,
        date_ref: date,
    ) -> MarketOutcome:
        return self.delegate.load_market_outcome(target, horizon, date_ref)


def _normalize_horizons(
    horizons: Sequence[RetrospectiveHorizon],
) -> tuple[RetrospectiveHorizon, ...]:
    normalized = tuple(dict.fromkeys(horizons))
    for horizon in normalized:
        horizon_to_days(horizon)
    return normalized


def _resolve_reader(
    reader: RetrospectiveEvaluationReader | None,
    storage: RetrospectiveEvaluationStorage,
) -> RetrospectiveEvaluationReader:
    if reader is not None:
        return reader
    if hasattr(storage, "load_evaluations"):
        return cast(RetrospectiveEvaluationReader, storage)
    return get_default_evaluation_reader()


def _load_targets_by_horizon(
    gateway: RetrospectiveInputGateway,
    horizons: Sequence[RetrospectiveHorizon],
    date_ref: date,
) -> dict[RetrospectiveHorizon, list[RetrospectiveTarget]]:
    targets_by_horizon: dict[RetrospectiveHorizon, list[RetrospectiveTarget]] = {}
    for horizon in horizons:
        targets = list(gateway.list_targets(horizon, date_ref))
        for index, target in enumerate(targets):
            assert_no_forbidden_write(
                asdict(target),
                path=f"$.targets[{horizon}][{index}]",
            )
        targets_by_horizon[horizon] = targets
    return targets_by_horizon


def _load_existing_evaluations_by_id(
    reader: RetrospectiveEvaluationReader,
    targets_by_horizon: dict[RetrospectiveHorizon, list[RetrospectiveTarget]],
) -> dict[str, RetrospectiveEvaluation]:
    expected_ids: set[str] = set()
    windows: set[tuple[RetrospectiveHorizon, str]] = set()
    for horizon, targets in targets_by_horizon.items():
        for target in targets:
            expected_ids.add(_evaluation_id(target, horizon))
            windows.add((horizon, target.object_ref))

    existing: dict[str, RetrospectiveEvaluation] = {}
    for horizon, object_ref in sorted(windows):
        for evaluation in reader.load_evaluations(
            RetroWindow(
                start=date.min,
                end=date.max,
                horizon=horizon,
                object_ref=object_ref,
            )
        ):
            if evaluation.evaluation_id in expected_ids:
                existing.setdefault(evaluation.evaluation_id, evaluation)
    return existing


def _object_refs_for_targets(
    targets_by_horizon: dict[RetrospectiveHorizon, list[RetrospectiveTarget]],
    horizons: Sequence[RetrospectiveHorizon],
) -> tuple[str, ...]:
    object_refs: dict[str, None] = {}
    for horizon in horizons:
        for target in targets_by_horizon[horizon]:
            object_refs.setdefault(target.object_ref, None)
    return tuple(object_refs)


def _evaluation_id(
    target: RetrospectiveTarget,
    horizon: RetrospectiveHorizon,
) -> str:
    return f"retro-{target.cycle_id}-{target.object_ref}-{horizon}"


__all__ = [
    "HorizonCoverageReport",
    "RetrospectiveBackfillResult",
    "RetrospectiveJob",
    "check_horizon_coverage",
    "run_backfill",
]
