"""Runtime schema objects for retrospective computation."""

from __future__ import annotations

from dataclasses import dataclass

from audit_eval.contracts.common import JsonObject, RetrospectiveHorizon


@dataclass(frozen=True)
class RetrospectiveTarget:
    """One cycle/object binding eligible for retrospective evaluation."""

    cycle_id: str
    object_ref: str


@dataclass(frozen=True)
class RetrospectiveSeed:
    """Historical prediction seed extracted from manifest-bound replay records."""

    cycle_id: str
    object_ref: str
    expected_trend_score: float
    expected_risk_score: float
    baseline_vs_llm_breakdown: JsonObject


@dataclass(frozen=True)
class MarketOutcome:
    """Realized market outcome for a retrospective target and horizon."""

    cycle_id: str
    object_ref: str
    horizon: RetrospectiveHorizon
    realized_trend_score: float
    realized_risk_score: float
    hit_rate_rel: float | None
    baseline_vs_llm_breakdown: JsonObject


@dataclass(frozen=True)
class DeviationResult:
    """Deviation values produced from a seed/outcome pair."""

    trend_deviation: float
    risk_deviation: float
    hit_rate_rel: float | None
    baseline_vs_llm_breakdown: JsonObject


__all__ = [
    "DeviationResult",
    "MarketOutcome",
    "RetrospectiveSeed",
    "RetrospectiveTarget",
]
