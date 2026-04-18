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
        score=0.8 if drifted else 0.1,
        threshold=0.05,
        drifted=drifted,
    )


def _result(
    features: tuple[DriftedFeature, ...],
    *,
    total_feature_count: int | None = None,
) -> EvidentlyRunResult:
    return EvidentlyRunResult(
        report_json={"metrics": []},
        drifted_features=features,
        total_feature_count=total_feature_count,
    )


def test_classify_regime_warning_none_when_no_features_drift() -> None:
    decision = classify_regime_warning(
        _result((_feature("beta", drifted=False),), total_feature_count=5)
    )

    assert decision.regime_warning_level == "none"
    assert decision.drifted_feature_count == 0
    assert decision.drifted_features == ()
    assert decision.alert_rules_version == ALERT_RULES_VERSION


def test_classify_regime_warning_warning_at_count_boundary() -> None:
    decision = classify_regime_warning(
        _result((_feature("beta", drifted=True),), total_feature_count=10)
    )

    assert decision.regime_warning_level == "warning"
    assert decision.drifted_feature_count == 1
    assert decision.drifted_share == 0.1


def test_classify_regime_warning_critical_at_count_boundary() -> None:
    decision = classify_regime_warning(
        _result(
            (
                _feature("beta", drifted=True),
                _feature("spread", drifted=True),
                _feature("momentum", drifted=True),
            ),
            total_feature_count=10,
        )
    )

    assert decision.regime_warning_level == "critical"
    assert decision.drifted_feature_count == 3


def test_classify_regime_warning_critical_at_share_boundary() -> None:
    decision = classify_regime_warning(
        _result(
            (
                _feature("beta", drifted=True),
                _feature("spread", drifted=True),
            ),
            total_feature_count=4,
        )
    )

    assert decision.regime_warning_level == "critical"
    assert decision.drifted_share == 0.5


def test_classify_regime_warning_uses_versioned_custom_rules() -> None:
    rules = DriftRuleConfig(
        warning_drifted_feature_count=2,
        critical_drifted_feature_count=4,
        warning_drifted_share=0.4,
        critical_drifted_share=0.8,
        version="drift-rules-test",
    )

    decision = classify_regime_warning(
        _result((_feature("beta", drifted=True),), total_feature_count=10),
        rules=rules,
    )

    assert decision.regime_warning_level == "none"
    assert decision.alert_rules_version == "drift-rules-test"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"warning_drifted_feature_count": 0},
        {
            "warning_drifted_feature_count": 3,
            "critical_drifted_feature_count": 2,
        },
        {"warning_drifted_share": 0.6, "critical_drifted_share": 0.5},
        {"version": ""},
    ],
)
def test_drift_rule_config_rejects_invalid_thresholds(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        DriftRuleConfig(**kwargs)
