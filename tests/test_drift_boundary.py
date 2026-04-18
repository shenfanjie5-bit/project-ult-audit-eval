from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from audit_eval._boundary import BoundaryViolationError
from audit_eval.contracts import DriftReport
from audit_eval.drift import (
    ALERT_RULES_VERSION,
    DriftedFeature,
    EvidentlyRunResult,
    InMemoryDriftInputGateway,
    InMemoryDriftReportJsonWriter,
    InMemoryDriftReportStorage,
    run_drift_report,
)


FORBIDDEN_FIELD = "feature_weight_multiplier"


class FailingIfCalledRunner:
    calls = 0

    def run(self, reference_data: Any, target_data: Any) -> EvidentlyRunResult:
        self.calls += 1
        raise AssertionError("Evidently runner should not be called")


class StaticRunner:
    def __init__(self, result: EvidentlyRunResult) -> None:
        self.result = result
        self.calls = 0

    def run(self, reference_data: Any, target_data: Any) -> EvidentlyRunResult:
        self.calls += 1
        return self.result


def _valid_feature_payload() -> dict[str, Any]:
    return {
        "name": "feature_a",
        "score": 0.42,
        "threshold": 0.30,
        "drifted": True,
        "statistic": 0.42,
        "details": {"stattest_name": "ks"},
    }


def _valid_report_payload() -> dict[str, Any]:
    return {
        "report_id": "drift-cycle_20260418",
        "cycle_id": "cycle_20260418",
        "baseline_ref": "feature-window://reference",
        "target_ref": "feature-window://target",
        "evidently_json_ref": "catalog://provided/drift-report.json",
        "drifted_features": [_valid_feature_payload()],
        "regime_warning_level": "warning",
        "alert_rules_version": ALERT_RULES_VERSION,
        "created_at": datetime(2026, 4, 18, 12, tzinfo=timezone.utc),
    }


def test_drift_report_fields_match_project_contract() -> None:
    assert tuple(DriftReport.model_fields) == (
        "report_id",
        "cycle_id",
        "baseline_ref",
        "target_ref",
        "evidently_json_ref",
        "drifted_features",
        "regime_warning_level",
        "alert_rules_version",
        "created_at",
    )


def test_drift_report_accepts_valid_payload() -> None:
    report = DriftReport.model_validate(_valid_report_payload())

    assert report.regime_warning_level == "warning"
    assert report.drifted_features[0]["name"] == "feature_a"


def test_drift_report_rejects_extra_fields() -> None:
    payload = _valid_report_payload()
    payload["extra"] = "forbidden"

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DriftReport.model_validate(payload)


def test_drift_report_rejects_unknown_regime_warning_level() -> None:
    payload = _valid_report_payload()
    payload["regime_warning_level"] = "emergency"

    with pytest.raises(ValidationError, match="critical"):
        DriftReport.model_validate(payload)


def test_drift_report_rejects_malformed_drifted_feature() -> None:
    payload = _valid_report_payload()
    payload["drifted_features"] = [{"name": "feature_a"}]

    with pytest.raises(ValidationError, match="drifted"):
        DriftReport.model_validate(payload)


def test_drift_report_rejects_nested_forbidden_field() -> None:
    payload = _valid_report_payload()
    payload["drifted_features"][0]["details"] = {FORBIDDEN_FIELD: 1.2}

    with pytest.raises(
        BoundaryViolationError,
        match=r"\$\.drifted_features\[0\]\.details\.feature_weight_multiplier",
    ):
        DriftReport.model_validate(payload)


def test_run_drift_report_rejects_forbidden_input_before_runner_or_writes() -> None:
    gateway = InMemoryDriftInputGateway(
        {
            "reference": {"nested": {FORBIDDEN_FIELD: 1.0}},
            "target": {"feature_a": [1, 2, 3]},
        }
    )
    runner = FailingIfCalledRunner()
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()

    with pytest.raises(BoundaryViolationError, match=FORBIDDEN_FIELD):
        run_drift_report(
            "reference",
            "target",
            input_gateway=gateway,
            evidently_runner=runner,
            json_writer=writer,
            storage=storage,
        )

    assert runner.calls == 0
    assert writer.calls == []
    assert storage.rows == []


def test_run_drift_report_rejects_forbidden_evidently_json_before_writes() -> None:
    gateway = InMemoryDriftInputGateway({"reference": {}, "target": {}})
    runner = StaticRunner(
        EvidentlyRunResult(
            evidently_json={"metrics": [{"result": {FORBIDDEN_FIELD: 1.0}}]},
            drifted_features=(),
            feature_count=0,
        )
    )
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()

    with pytest.raises(BoundaryViolationError, match=FORBIDDEN_FIELD):
        run_drift_report(
            "reference",
            "target",
            input_gateway=gateway,
            evidently_runner=runner,
            json_writer=writer,
            storage=storage,
        )

    assert runner.calls == 1
    assert writer.calls == []
    assert storage.rows == []


def test_run_drift_report_rejects_forbidden_feature_payload_before_writes() -> None:
    gateway = InMemoryDriftInputGateway({"reference": {}, "target": {}})
    feature = DriftedFeature(
        name="feature_a",
        score=0.42,
        threshold=0.30,
        drifted=True,
        details={"nested": {FORBIDDEN_FIELD: 1.0}},
    )
    runner = StaticRunner(
        EvidentlyRunResult(
            evidently_json={"metrics": []},
            drifted_features=(feature,),
            feature_count=1,
        )
    )
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()

    with pytest.raises(BoundaryViolationError, match=FORBIDDEN_FIELD):
        run_drift_report(
            "reference",
            "target",
            input_gateway=gateway,
            evidently_runner=runner,
            json_writer=writer,
            storage=storage,
        )

    assert writer.calls == []
    assert storage.rows == []


def test_alert_payload_is_derived_from_structured_drift_rows_only() -> None:
    payload = _valid_report_payload()
    payload["drifted_features"] = [
        asdict(
            DriftedFeature(
                name="feature_a",
                score=0.42,
                threshold=0.30,
                drifted=True,
            )
        ),
        asdict(
            DriftedFeature(
                name="feature_b",
                score=0.10,
                threshold=0.30,
                drifted=False,
            )
        ),
    ]
    report = DriftReport.model_validate(payload)

    from audit_eval.drift import build_drift_alert_payload

    alert_payload = build_drift_alert_payload(report)

    assert alert_payload.drifted_features == ("feature_a",)
    assert set(asdict(alert_payload)) == {
        "report_id",
        "regime_warning_level",
        "drifted_features",
        "evidently_json_ref",
    }
