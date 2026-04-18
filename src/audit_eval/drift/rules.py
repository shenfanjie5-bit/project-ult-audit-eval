"""Versioned structural drift warning rules."""

from __future__ import annotations

from dataclasses import dataclass

from audit_eval.drift.schema import (
    DriftedFeature,
    EvidentlyRunResult,
    RegimeWarningLevel,
)

ALERT_RULES_VERSION = "drift-rules-v1"


@dataclass(frozen=True)
class DriftRuleConfig:
    """Thresholds for mapping Evidently drift evidence to structural warnings."""

    warning_drifted_feature_count: int = 1
    critical_drifted_feature_count: int = 3
    warning_drifted_share: float = 0.20
    critical_drifted_share: float = 0.50
    version: str = ALERT_RULES_VERSION

    def __post_init__(self) -> None:
        if self.warning_drifted_feature_count < 1:
            raise ValueError("warning_drifted_feature_count must be positive")
        if self.critical_drifted_feature_count < self.warning_drifted_feature_count:
            raise ValueError(
                "critical_drifted_feature_count must be at least warning count"
            )
        if not 0 <= self.warning_drifted_share <= self.critical_drifted_share <= 1:
            raise ValueError(
                "drifted share thresholds must satisfy 0 <= warning <= critical <= 1"
            )
        if not self.version:
            raise ValueError("version must not be empty")


@dataclass(frozen=True)
class DriftRuleDecision:
    """Regime warning decision and the evidence persisted with the report."""

    regime_warning_level: RegimeWarningLevel
    drifted_features: tuple[DriftedFeature, ...]
    drifted_feature_count: int
    total_feature_count: int
    drifted_share: float
    alert_rules_version: str


DEFAULT_DRIFT_RULE_CONFIG = DriftRuleConfig()


def classify_regime_warning(
    result: EvidentlyRunResult,
    *,
    rules: DriftRuleConfig = DEFAULT_DRIFT_RULE_CONFIG,
) -> DriftRuleDecision:
    """Classify Evidently evidence into none, warning, or critical."""

    drifted_features = tuple(
        feature for feature in result.drifted_features if feature.drifted
    )
    drifted_feature_count = len(drifted_features)
    total_feature_count = result.feature_count
    drifted_share = (
        drifted_feature_count / total_feature_count
        if total_feature_count > 0
        else 0.0
    )

    level: RegimeWarningLevel = "none"
    if drifted_feature_count:
        if (
            drifted_feature_count >= rules.critical_drifted_feature_count
            or drifted_share >= rules.critical_drifted_share
        ):
            level = "critical"
        elif (
            drifted_feature_count >= rules.warning_drifted_feature_count
            or drifted_share >= rules.warning_drifted_share
        ):
            level = "warning"

    return DriftRuleDecision(
        regime_warning_level=level,
        drifted_features=drifted_features,
        drifted_feature_count=drifted_feature_count,
        total_feature_count=total_feature_count,
        drifted_share=drifted_share,
        alert_rules_version=rules.version,
    )


__all__ = [
    "ALERT_RULES_VERSION",
    "DEFAULT_DRIFT_RULE_CONFIG",
    "DriftRuleConfig",
    "DriftRuleDecision",
    "classify_regime_warning",
]
