import pytest

from audit_eval.drift import (
    ALERT_RULES_VERSION,
    DEFAULT_DRIFT_RULE_CONFIG,
    DriftRuleConfig,
    DriftedFeature,
    EvidentlyRunResult,
    classify_regime_warning,
)


def _result(
    drifted_flags: tuple[bool, ...],
    *,
    feature_count: int | None = None,
) -> EvidentlyRunResult:
    return EvidentlyRunResult(
        report_json={"metrics": []},
        drifted_features=tuple(
            DriftedFeature(
                name=f"feature_{index}",
                score=0.12,
                statistic=None,
                threshold=0.05,
                drifted=drifted,
            )
            for index, drifted in enumerate(drifted_flags)
        ),
        feature_count=feature_count,
    )


def test_classify_regime_warning_none_boundary() -> None:
    decision = classify_regime_warning(_result((False, False, False)))

    assert decision.regime_warning_level == "none"
    assert decision.drifted_feature_count == 0
    assert decision.drift_share == 0.0
    assert decision.alert_rules_version == ALERT_RULES_VERSION


def test_classify_regime_warning_warning_count_boundary() -> None:
    decision = classify_regime_warning(_result((True, False, False)))

    assert decision.regime_warning_level == "warning"
    assert tuple(feature.name for feature in decision.drifted_features) == (
        "feature_0",
    )


def test_classify_regime_warning_critical_count_boundary() -> None:
    decision = classify_regime_warning(_result((True, True, True, False)))

    assert decision.regime_warning_level == "critical"
    assert decision.drifted_feature_count == 3


def test_classify_regime_warning_share_boundaries_are_configurable() -> None:
    rules = DriftRuleConfig(
        warning_drift_share=0.25,
        critical_drift_share=0.5,
        warning_drifted_feature_count=10,
        critical_drifted_feature_count=10,
    )

    warning = classify_regime_warning(
        _result((True, False, False, False), feature_count=4),
        rules=rules,
    )
    critical = classify_regime_warning(
        _result((True, True, False, False), feature_count=4),
        rules=rules,
    )

    assert warning.regime_warning_level == "warning"
    assert warning.drift_share == 0.25
    assert critical.regime_warning_level == "critical"
    assert critical.drift_share == 0.5


def test_default_drift_rule_config_is_versioned() -> None:
    decision = classify_regime_warning(_result((True,)), rules=DEFAULT_DRIFT_RULE_CONFIG)

    assert decision.alert_rules_version == ALERT_RULES_VERSION


@pytest.mark.parametrize(
    "kwargs",
    [
        {"warning_drift_share": -0.01},
        {"critical_drift_share": 1.01},
        {"warning_drift_share": 0.6, "critical_drift_share": 0.5},
        {"warning_drifted_feature_count": 0},
        {"warning_drifted_feature_count": 2, "critical_drifted_feature_count": 1},
    ],
)
def test_drift_rule_config_rejects_invalid_thresholds(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        DriftRuleConfig(**kwargs)
