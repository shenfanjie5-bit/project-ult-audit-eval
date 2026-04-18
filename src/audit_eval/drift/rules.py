"""Versioned drift alert rules."""

from __future__ import annotations

import math
from dataclasses import dataclass

from audit_eval.drift.schema import DriftedFeature, EvidentlyRunResult, RegimeWarningLevel

ALERT_RULES_VERSION = "drift-alert-rules-v1"


@dataclass(frozen=True)
class DriftRuleConfig:
    """Thresholds for structural regime warning classification."""

    warning_drift_share: float = 0.2
    critical_drift_share: float = 0.5
    warning_drifted_feature_count: int = 1
    critical_drifted_feature_count: int = 3

    def __post_init__(self) -> None:
        for field_name in ("warning_drift_share", "critical_drift_share"):
            value = getattr(self, field_name)
            if not math.isfinite(value) or not 0 <= value <= 1:
                raise ValueError(f"{field_name} must be between 0 and 1")

        if self.warning_drift_share > self.critical_drift_share:
            raise ValueError(
                "warning_drift_share must be less than or equal to "
                "critical_drift_share"
            )
        if self.warning_drifted_feature_count < 1:
            raise ValueError("warning_drifted_feature_count must be positive")
        if (
            self.critical_drifted_feature_count
            < self.warning_drifted_feature_count
        ):
            raise ValueError(
                "critical_drifted_feature_count must be greater than or equal "
                "to warning_drifted_feature_count"
            )


@dataclass(frozen=True)
class DriftRuleDecision:
    """Outcome of applying versioned drift alert rules."""

    regime_warning_level: RegimeWarningLevel
    drifted_features: tuple[DriftedFeature, ...]
    drifted_feature_count: int
    drift_share: float
    alert_rules_version: str = ALERT_RULES_VERSION


DEFAULT_DRIFT_RULE_CONFIG = DriftRuleConfig()


def classify_regime_warning(
    result: EvidentlyRunResult,
    *,
    rules: DriftRuleConfig = DEFAULT_DRIFT_RULE_CONFIG,
) -> DriftRuleDecision:
    """Classify Evidently output into none, warning, or critical."""

    features = tuple(result.drifted_features)
    drifted_features = tuple(feature for feature in features if feature.drifted)
    drifted_feature_count = len(drifted_features)
    denominator = _feature_count_denominator(result.feature_count, len(features))
    drift_share = 0.0 if denominator == 0 else drifted_feature_count / denominator

    if (
        drifted_feature_count >= rules.critical_drifted_feature_count
        or drift_share >= rules.critical_drift_share
    ):
        level: RegimeWarningLevel = "critical"
    elif (
        drifted_feature_count >= rules.warning_drifted_feature_count
        or drift_share >= rules.warning_drift_share
    ):
        level = "warning"
    else:
        level = "none"

    return DriftRuleDecision(
        regime_warning_level=level,
        drifted_features=drifted_features,
        drifted_feature_count=drifted_feature_count,
        drift_share=drift_share,
    )


def _feature_count_denominator(
    reported_count: int | None,
    observed_count: int,
) -> int:
    if reported_count is None:
        return observed_count
    if reported_count < 0:
        raise ValueError("EvidentlyRunResult.feature_count must be non-negative")
    return max(reported_count, observed_count)


__all__ = [
    "ALERT_RULES_VERSION",
    "DEFAULT_DRIFT_RULE_CONFIG",
    "DriftRuleConfig",
    "DriftRuleDecision",
    "classify_regime_warning",
]
