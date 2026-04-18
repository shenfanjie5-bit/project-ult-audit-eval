"""T+1 retrospective evaluation computation."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from numbers import Real
from typing import Any

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.audit import query as audit_query
from audit_eval.audit.query import ReplayQueryContext
from audit_eval.audit.replay_view import ReplayView
from audit_eval.contracts.common import RetrospectiveHorizon
from audit_eval.contracts.retrospective import RetrospectiveEvaluation
from audit_eval.retro.schema import (
    DeviationResult,
    MarketOutcome,
    RetrospectiveSeed,
    RetrospectiveTarget,
)
from audit_eval.retro.storage import (
    RetrospectiveEvaluationStorage,
    RetrospectiveInputError,
    RetrospectiveInputGateway,
    RetrospectiveStorageError,
    get_default_evaluation_storage,
    get_default_input_gateway,
)

_SUPPORTED_HORIZON: RetrospectiveHorizon = "T+1"
_KNOWN_HORIZONS: frozenset[str] = frozenset({"T+1", "T+5", "T+20"})


class UnsupportedRetrospectiveHorizon(RetrospectiveInputError):
    """Raised when a requested retrospective horizon is not implemented."""


def compute_retrospective(
    horizon: RetrospectiveHorizon,
    date_ref: date,
    *,
    replay_context: ReplayQueryContext | None = None,
    input_gateway: RetrospectiveInputGateway | None = None,
    storage: RetrospectiveEvaluationStorage | None = None,
    as_of_date: date | None = None,
) -> list[RetrospectiveEvaluation]:
    """Compute and append T+1 retrospective evaluations for one date."""

    _require_supported_horizon(horizon)
    _require_mature_outcome(date_ref=date_ref, as_of_date=as_of_date)

    gateway = input_gateway or get_default_input_gateway()
    evaluation_storage = storage or get_default_evaluation_storage()

    evaluations: list[RetrospectiveEvaluation] = []
    targets = list(gateway.list_targets(horizon, date_ref))
    for target in targets:
        assert_no_forbidden_write(asdict(target), path="$.targets[]")
        replay_view = audit_query.replay_cycle_object(
            target.cycle_id,
            target.object_ref,
            context=replay_context,
        )
        seed = extract_retrospective_seed(replay_view)
        outcome = gateway.load_market_outcome(target, horizon, date_ref)
        assert_no_forbidden_write(asdict(outcome), path="$.outcome")
        _validate_outcome_binding(outcome, target, horizon)

        deviation = calculate_deviation(seed, outcome)
        evaluation = _build_evaluation(
            target=target,
            horizon=horizon,
            deviation=deviation,
        )
        assert_no_forbidden_write(
            evaluation.model_dump(mode="python"),
            path="$.evaluations[]",
        )
        evaluations.append(evaluation)

    try:
        evaluation_storage.append_evaluations(evaluations)
    except RetrospectiveStorageError:
        raise
    except Exception as exc:
        raise RetrospectiveStorageError(
            f"append_evaluations failed: {exc}"
        ) from exc
    return evaluations


def extract_retrospective_seed(replay_view: ReplayView) -> RetrospectiveSeed:
    """Extract the first valid retrospective seed from canonical audit records."""

    for audit_record in replay_view.audit_records:
        for source_name, payload in (
            ("parsed_result", audit_record.parsed_result),
            ("params_snapshot", audit_record.params_snapshot),
        ):
            if not isinstance(payload, Mapping):
                continue
            seed_payload = payload.get("retrospective_seed")
            if seed_payload is None:
                continue
            if not isinstance(seed_payload, Mapping):
                raise RetrospectiveInputError(
                    f"AuditRecord.{source_name}.retrospective_seed must be an object"
                )
            assert_no_forbidden_write(
                seed_payload,
                path=f"$.audit_records[{audit_record.record_id}].{source_name}"
                ".retrospective_seed",
            )
            trend_score = _require_finite_number(
                seed_payload.get("trend_score"),
                field_path="retrospective_seed.trend_score",
            )
            risk_score = _require_finite_number(
                seed_payload.get("risk_score"),
                field_path="retrospective_seed.risk_score",
            )
            breakdown = seed_payload.get("baseline_vs_llm_breakdown", {})
            if not isinstance(breakdown, dict):
                raise RetrospectiveInputError(
                    "retrospective_seed.baseline_vs_llm_breakdown must be an object"
                )
            return RetrospectiveSeed(
                cycle_id=replay_view.cycle_id,
                object_ref=replay_view.object_ref,
                expected_trend_score=trend_score,
                expected_risk_score=risk_score,
                baseline_vs_llm_breakdown=dict(breakdown),
            )

    raise RetrospectiveInputError(
        "ReplayView.audit_records did not contain a retrospective_seed with "
        "numeric trend_score and risk_score"
    )


def calculate_deviation(
    seed: RetrospectiveSeed,
    outcome: MarketOutcome,
) -> DeviationResult:
    """Calculate T+1 absolute trend/risk deviations."""

    return DeviationResult(
        trend_deviation=abs(
            seed.expected_trend_score - outcome.realized_trend_score
        ),
        risk_deviation=abs(seed.expected_risk_score - outcome.realized_risk_score),
        hit_rate_rel=outcome.hit_rate_rel,
        baseline_vs_llm_breakdown=dict(outcome.baseline_vs_llm_breakdown),
    )


def _require_supported_horizon(horizon: str) -> None:
    if horizon not in _KNOWN_HORIZONS:
        raise UnsupportedRetrospectiveHorizon(
            f"Unsupported retrospective horizon {horizon!r}"
        )
    if horizon != _SUPPORTED_HORIZON:
        raise UnsupportedRetrospectiveHorizon(
            f"Retrospective horizon {horizon!r} is not implemented yet"
        )


def _require_mature_outcome(date_ref: date, as_of_date: date | None) -> None:
    effective_as_of_date = as_of_date or date.today()
    if date_ref + timedelta(days=1) > effective_as_of_date:
        raise RetrospectiveInputError(
            "T+1 market outcome is not mature for "
            f"date_ref={date_ref.isoformat()} as_of_date="
            f"{effective_as_of_date.isoformat()}"
        )


def _validate_outcome_binding(
    outcome: MarketOutcome,
    target: RetrospectiveTarget,
    horizon: RetrospectiveHorizon,
) -> None:
    if outcome.cycle_id != target.cycle_id:
        raise RetrospectiveInputError("MarketOutcome.cycle_id does not match target")
    if outcome.object_ref != target.object_ref:
        raise RetrospectiveInputError("MarketOutcome.object_ref does not match target")
    if outcome.horizon != horizon:
        raise RetrospectiveInputError("MarketOutcome.horizon does not match request")
    _require_finite_number(
        outcome.realized_trend_score,
        field_path="MarketOutcome.realized_trend_score",
    )
    _require_finite_number(
        outcome.realized_risk_score,
        field_path="MarketOutcome.realized_risk_score",
    )
    if outcome.hit_rate_rel is not None:
        _require_finite_number(
            outcome.hit_rate_rel,
            field_path="MarketOutcome.hit_rate_rel",
        )


def _require_finite_number(value: Any, *, field_path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise RetrospectiveInputError(f"{field_path} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise RetrospectiveInputError(f"{field_path} must be finite")
    return number


def _build_evaluation(
    *,
    target: RetrospectiveTarget,
    horizon: RetrospectiveHorizon,
    deviation: DeviationResult,
) -> RetrospectiveEvaluation:
    trend_deviation = deviation.trend_deviation
    risk_deviation = deviation.risk_deviation
    alert_score = RetrospectiveEvaluation.derive_alert_score(
        trend_deviation,
        risk_deviation,
    )
    return RetrospectiveEvaluation(
        evaluation_id=_evaluation_id(target, horizon),
        cycle_id=target.cycle_id,
        object_ref=target.object_ref,
        horizon=horizon,
        trend_deviation=trend_deviation,
        risk_deviation=risk_deviation,
        alert_score=alert_score,
        learning_score=RetrospectiveEvaluation.derive_learning_score(
            trend_deviation,
            risk_deviation,
        ),
        deviation_level=_deviation_level(alert_score),
        hit_rate_rel=deviation.hit_rate_rel,
        baseline_vs_llm_breakdown=deviation.baseline_vs_llm_breakdown,
        evaluated_at=datetime.now(timezone.utc),
    )


def _evaluation_id(
    target: RetrospectiveTarget,
    horizon: RetrospectiveHorizon,
) -> str:
    return f"retro-{target.cycle_id}-{target.object_ref}-{horizon}"


def _deviation_level(alert_score: float) -> int:
    return min(4, int(math.floor(alert_score)))


__all__ = [
    "UnsupportedRetrospectiveHorizon",
    "calculate_deviation",
    "compute_retrospective",
    "extract_retrospective_seed",
]
