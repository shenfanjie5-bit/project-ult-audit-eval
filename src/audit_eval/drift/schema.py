"""Runtime schema objects for drift reporting."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from numbers import Real
from typing import Literal

from audit_eval.contracts.common import JsonObject

RegimeWarningLevel = Literal["none", "warning", "critical"]


@dataclass(frozen=True)
class DriftedFeature:
    """Structured drift evidence for one feature."""

    name: str
    score: float | None
    threshold: float | None
    drifted: bool
    statistic: float | None = None
    metadata: JsonObject = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("DriftedFeature.name must not be empty")
        _validate_optional_number(self.score, "DriftedFeature.score")
        _validate_optional_number(self.threshold, "DriftedFeature.threshold")
        _validate_optional_number(self.statistic, "DriftedFeature.statistic")


@dataclass(frozen=True)
class EvidentlyRunResult:
    """Normalized output from an Evidently drift run."""

    report_json: JsonObject
    drifted_features: tuple[DriftedFeature, ...] = ()
    total_feature_count: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.report_json, dict):
            raise TypeError("EvidentlyRunResult.report_json must be a JSON object")
        if self.total_feature_count is not None:
            if (
                isinstance(self.total_feature_count, bool)
                or self.total_feature_count < len(self.drifted_features)
            ):
                raise ValueError(
                    "EvidentlyRunResult.total_feature_count must cover features"
                )

    @property
    def feature_count(self) -> int:
        """Return the total feature count represented by this run."""

        return self.total_feature_count or len(self.drifted_features)


@dataclass(frozen=True)
class DriftAlertPayload:
    """Third-layer structural warning payload for orchestration and dashboards."""

    report_id: str
    regime_warning_level: RegimeWarningLevel
    drifted_features: tuple[str, ...]
    evidently_json_ref: str


def _validate_optional_number(value: float | None, field_name: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"{field_name} must be numeric when provided")
    if not math.isfinite(float(value)):
        raise ValueError(f"{field_name} must be finite when provided")


__all__ = [
    "DriftAlertPayload",
    "DriftedFeature",
    "EvidentlyRunResult",
    "RegimeWarningLevel",
]
