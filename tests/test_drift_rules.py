import pytest

from audit_eval.drift import (
    ALERT_RULES_VERSION,
    DriftRuleConfig,
    DriftedFeature,
    EvidentlyRunResult,
    classify_regime_warning,
)


def _feature(name: str, *, drifted: bool) -> DriftedFeature:
    return DriftedFeature(
        name=name,
        score=0.4 if drifted else 0.1,
        threshold=0.3,
        drifted=drifted,
    )


def _result(drifted_flags: list[bool]) -> EvidentlyRunResult:
    return EvidentlyRunResult(
        evidently_json={"metrics": []},
        drifted_features=tuple(
            _feature(f"feature_{index}", drifted=drifted)
            for index, drifted in enumerate(drifted_flags)
        ),
        feature_count=len(drifted_flags),
    )


def test_classify_regime_warning_none_below_thresholds() -> None:
    rules = DriftRuleConfig(
        warning_drifted_feature_count=2,
        critical_drifted_feature_count=4,
        warning_drift_share=0.50,
        critical_drift_share=0.75,
    )

    decision = classify_regime_warning(_result([True, False, False, False]), rules=rules)

    assert decision.regime_warning_level == "none"
    assert decision.drifted_feature_count == 1
    assert decision.feature_count == 4
    assert decision.drift_share == 0.25
    assert decision.alert_rules_version == ALERT_RULES_VERSION


def test_classify_regime_warning_warning_on_count_and_share_boundary() -> None:
    rules = DriftRuleConfig(
        warning_drifted_feature_count=2,
        critical_drifted_feature_count=4,
        warning_drift_share=0.50,
        critical_drift_share=0.75,
    )

    decision = classify_regime_warning(_result([True, True, False, False]), rules=rules)

    assert decision.regime_warning_level == "warning"
    assert tuple(feature.name for feature in decision.drifted_features) == (
        "feature_0",
        "feature_1",
    )
    assert decision.drift_share == 0.50


def test_classify_regime_warning_critical_overrides_warning_boundary() -> None:
    rules = DriftRuleConfig(
        warning_drifted_feature_count=2,
        critical_drifted_feature_count=4,
        warning_drift_share=0.50,
        critical_drift_share=0.75,
    )

    decision = classify_regime_warning(_result([True, True, True, False]), rules=rules)

    assert decision.regime_warning_level == "critical"
    assert decision.drifted_feature_count == 3
    assert decision.drift_share == 0.75


def test_classify_regime_warning_uses_feature_count_lower_bound() -> None:
    result = EvidentlyRunResult(
        evidently_json={"metrics": []},
        drifted_features=(_feature("a", drifted=True), _feature("b", drifted=True)),
        feature_count=1,
    )

    decision = classify_regime_warning(
        result,
        rules=DriftRuleConfig(
            warning_drifted_feature_count=1,
            critical_drifted_feature_count=3,
            warning_drift_share=0.20,
            critical_drift_share=0.90,
        ),
    )

    assert decision.feature_count == 2
    assert decision.drift_share == 1.0
    assert decision.regime_warning_level == "critical"


def test_drift_rule_config_rejects_invalid_thresholds() -> None:
    with pytest.raises(ValueError, match="critical_drifted_feature_count"):
        DriftRuleConfig(
            warning_drifted_feature_count=3,
            critical_drifted_feature_count=2,
        )

    with pytest.raises(ValueError, match="critical_drift_share"):
        DriftRuleConfig(warning_drift_share=0.8, critical_drift_share=0.5)
