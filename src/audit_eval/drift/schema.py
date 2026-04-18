"""Runtime schema objects for drift reporting."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from audit_eval.contracts.common import JsonObject

RegimeWarningLevel = Literal["none", "warning", "critical"]


@dataclass(frozen=True)
class DriftedFeature:
    """Structured feature-level drift evidence from Evidently output."""

    name: str
    score: float | None
    statistic: float | None
    threshold: float | None
    drifted: bool


@dataclass(frozen=True)
class EvidentlyRunResult:
    """Normalized result returned by an Evidently runner adapter."""

    report_json: JsonObject
    drifted_features: tuple[DriftedFeature, ...]
    dataset_drift: bool = False
    feature_count: int | None = None


@dataclass(frozen=True)
class DriftAlertPayload:
    """Third-layer structural warning payload for orchestrators and dashboards."""

    report_id: str
    regime_warning_level: RegimeWarningLevel
    drifted_features: tuple[str, ...]
    evidently_json_ref: str


__all__ = [
    "DriftAlertPayload",
    "DriftedFeature",
    "EvidentlyRunResult",
    "RegimeWarningLevel",
]
