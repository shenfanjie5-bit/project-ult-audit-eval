"""Runtime schema objects for drift reporting."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from numbers import Real
from typing import Any, Literal

from audit_eval.contracts.common import JsonObject

RegimeWarningLevel = Literal["none", "warning", "critical"]


@dataclass(frozen=True)
class DriftedFeature:
    """One feature-level drift result extracted from an Evidently report."""

    name: str
    score: float
    threshold: float
    drifted: bool
    statistic: float | None = None
    details: JsonObject = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("DriftedFeature.name must not be empty")
        _require_finite_number(self.score, field_path="DriftedFeature.score")
        _require_finite_number(
            self.threshold,
            field_path="DriftedFeature.threshold",
        )
        if self.statistic is not None:
            _require_finite_number(
                self.statistic,
                field_path="DriftedFeature.statistic",
            )


@dataclass(frozen=True)
class EvidentlyRunResult:
    """Evidently report JSON plus structured feature drift summaries."""

    evidently_json: JsonObject
    drifted_features: tuple[DriftedFeature, ...]
    feature_count: int | None = None

    def __post_init__(self) -> None:
        if self.feature_count is not None and self.feature_count < 0:
            raise ValueError("EvidentlyRunResult.feature_count must be non-negative")


@dataclass(frozen=True)
class DriftAlertPayload:
    """Third-layer structural drift warning payload."""

    report_id: str
    regime_warning_level: RegimeWarningLevel
    drifted_features: tuple[str, ...]
    evidently_json_ref: str


def _require_finite_number(value: Any, *, field_path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{field_path} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field_path} must be finite")
    return number


__all__ = [
    "DriftAlertPayload",
    "DriftedFeature",
    "EvidentlyRunResult",
    "RegimeWarningLevel",
]
