"""Versioned structural drift warning rules."""

from __future__ import annotations

from dataclasses import dataclass

from audit_eval.drift.schema import (
    DriftedFeature,
    EvidentlyRunResult,
    RegimeWarningLevel,
)

ALERT_RULES_VERSION = "drift-regime-rules-v1"


@dataclass(frozen=True)
class DriftRuleConfig:
    """Thresholds for third-layer structural drift warnings."""

    warning_drifted_feature_count: int = 1
    critical_drifted_feature_count: int = 3
    warning_drift_share: float = 0.20
    critical_drift_share: float = 0.50

    def __post_init__(self) -> None:
        if self.warning_drifted_feature_count < 0:
            raise ValueError("warning_drifted_feature_count must be non-negative")
        if self.critical_drifted_feature_count < self.warning_drifted_feature_count:
            raise ValueError(
                "critical_drifted_feature_count must be greater than or equal "
                "to warning_drifted_feature_count"
            )
        if not 0.0 <= self.warning_drift_share <= 1.0:
            raise ValueError("warning_drift_share must be between 0 and 1")
        if not self.warning_drift_share <= self.critical_drift_share <= 1.0:
            raise ValueError(
                "critical_drift_share must be greater than or equal to "
                "warning_drift_share and at most 1"
            )


@dataclass(frozen=True)
class DriftRuleDecision:
    """Rule decision plus the metrics used to derive it."""

    regime_warning_level: RegimeWarningLevel
    drifted_features: tuple[DriftedFeature, ...]
    drifted_feature_count: int
    feature_count: int
    drift_share: float
    alert_rules_version: str = ALERT_RULES_VERSION


DEFAULT_DRIFT_RULE_CONFIG = DriftRuleConfig()


def classify_regime_warning(
    result: EvidentlyRunResult,
    *,
    rules: DriftRuleConfig = DEFAULT_DRIFT_RULE_CONFIG,
) -> DriftRuleDecision:
    """Classify Evidently feature drift into none, warning, or critical."""

    drifted_features = tuple(
        feature for feature in result.drifted_features if feature.drifted
    )
    drifted_count = len(drifted_features)
    feature_count = result.feature_count
    if feature_count is None:
        feature_count = len(result.drifted_features)
    feature_count = max(feature_count, drifted_count)
    drift_share = drifted_count / feature_count if feature_count else 0.0

    level: RegimeWarningLevel = "none"
    if (
        drifted_count >= rules.critical_drifted_feature_count
        or drift_share >= rules.critical_drift_share
    ):
        level = "critical"
    elif (
        drifted_count >= rules.warning_drifted_feature_count
        or drift_share >= rules.warning_drift_share
    ):
        level = "warning"

    return DriftRuleDecision(
        regime_warning_level=level,
        drifted_features=drifted_features,
        drifted_feature_count=drifted_count,
        feature_count=feature_count,
        drift_share=drift_share,
    )


__all__ = [
    "ALERT_RULES_VERSION",
    "DEFAULT_DRIFT_RULE_CONFIG",
    "DriftRuleConfig",
    "DriftRuleDecision",
    "classify_regime_warning",
]
