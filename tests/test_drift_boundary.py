from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from audit_eval._boundary import BoundaryViolationError
from audit_eval.contracts.drift_report import DriftReport
from audit_eval.drift import (
    DriftedFeature,
    EvidentlyRunResult,
    InMemoryDriftInputGateway,
    InMemoryDriftReportJsonWriter,
    InMemoryDriftReportStorage,
    StaticEvidentlyRunner,
    build_drift_alert_payload,
    run_drift_report,
)


def _valid_report_payload() -> dict[str, Any]:
    return {
        "report_id": "drift-cycle_20260418",
        "cycle_id": "cycle_20260418",
        "baseline_ref": "baseline-ref",
        "target_ref": "target-ref",
        "evidently_json_ref": "catalog://drift/report.json",
        "drifted_features": {
            "features": [
                {
                    "name": "spread",
                    "score": 0.82,
                    "statistic": 0.31,
                    "threshold": 0.05,
                    "drifted": True,
                }
            ],
            "feature_count": 5,
            "drifted_feature_count": 1,
            "drifted_share": 0.2,
        },
        "regime_warning_level": "warning",
        "alert_rules_version": "drift-rules-v1",
        "created_at": datetime(2026, 4, 18, tzinfo=timezone.utc),
    }


def _result_with_payload(
    *,
    report_json: dict[str, Any] | None = None,
    feature_metadata: dict[str, Any] | None = None,
) -> EvidentlyRunResult:
    return EvidentlyRunResult(
        report_json=report_json or {"metrics": []},
        drifted_features=(
            DriftedFeature(
                name="spread",
                score=0.8,
                statistic=0.2,
                threshold=0.05,
                drifted=True,
                metadata=feature_metadata or {},
            ),
        ),
        total_feature_count=5,
    )


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


def test_drift_report_rejects_extra_fields_and_unknown_level() -> None:
    payload = _valid_report_payload()
    payload["extra"] = "forbidden"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DriftReport.model_validate(payload)

    payload = _valid_report_payload()
    payload["regime_warning_level"] = "gate"
    with pytest.raises(ValidationError, match="none"):
        DriftReport.model_validate(payload)


def test_drift_report_rejects_nested_boundary_field() -> None:
    payload = _valid_report_payload()
    payload["drifted_features"]["features"][0]["metadata"] = {
        "feature_weight_multiplier": 1.2
    }

    with pytest.raises(BoundaryViolationError, match="feature_weight_multiplier"):
        DriftReport.model_validate(payload)


def test_runner_rejects_feature_window_boundary_before_runner_or_writes() -> None:
    gateway = InMemoryDriftInputGateway(
        {
            "reference": [{"feature_weight_multiplier": 1.2}],
            "target": [{"spread": 2.0}],
        }
    )
    evidently_runner = StaticEvidentlyRunner(_result_with_payload())
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()

    with pytest.raises(BoundaryViolationError, match="feature_weight_multiplier"):
        run_drift_report(
            "reference",
            "target",
            input_gateway=gateway,
            evidently_runner=evidently_runner,
            json_writer=writer,
            storage=storage,
        )

    assert evidently_runner.calls == []
    assert writer.rows == []
    assert storage.rows == []


def test_runner_rejects_evidently_json_boundary_before_writes() -> None:
    gateway = InMemoryDriftInputGateway({"reference": [], "target": []})
    evidently_runner = StaticEvidentlyRunner(
        _result_with_payload(
            report_json={"metrics": [{"feature_weight_multiplier": 1.2}]}
        )
    )
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()

    with pytest.raises(BoundaryViolationError, match="feature_weight_multiplier"):
        run_drift_report(
            "reference",
            "target",
            input_gateway=gateway,
            evidently_runner=evidently_runner,
            json_writer=writer,
            storage=storage,
        )

    assert writer.rows == []
    assert storage.rows == []


def test_runner_rejects_drifted_feature_boundary_before_writes() -> None:
    gateway = InMemoryDriftInputGateway({"reference": [], "target": []})
    evidently_runner = StaticEvidentlyRunner(
        _result_with_payload(
            feature_metadata={"feature_weight_multiplier": 1.2}
        )
    )
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()

    with pytest.raises(BoundaryViolationError, match="feature_weight_multiplier"):
        run_drift_report(
            "reference",
            "target",
            input_gateway=gateway,
            evidently_runner=evidently_runner,
            json_writer=writer,
            storage=storage,
        )

    assert writer.rows == []
    assert storage.rows == []


def test_alert_payload_only_contains_structural_warning_fields() -> None:
    report = DriftReport.model_validate(_valid_report_payload())

    payload = build_drift_alert_payload(report)

    assert set(asdict(payload)) == {
        "report_id",
        "regime_warning_level",
        "drifted_features",
        "evidently_json_ref",
    }
    assert payload.drifted_features == ("spread",)
    assert "feature_weight_multiplier" not in str(asdict(payload))
    assert "feature_weight" not in str(asdict(payload))
    assert "gate" not in str(asdict(payload))


def test_in_memory_json_writer_rejects_boundary_payload_without_storing() -> None:
    writer = InMemoryDriftReportJsonWriter()

    with pytest.raises(BoundaryViolationError, match="feature_weight_multiplier"):
        writer.write_report_json(
            "drift-report",
            {"nested": {"feature_weight_multiplier": 1.2}},
        )

    assert writer.rows == []


def test_in_memory_storage_stores_copy_and_rechecks_mutated_report() -> None:
    report = DriftReport.model_validate(_valid_report_payload())
    storage = InMemoryDriftReportStorage()

    storage.append_drift_report(report)
    report.drifted_features["features"][0]["name"] = "mutated"

    assert storage.rows[0]["drifted_features"]["features"][0]["name"] == "spread"

    report.drifted_features["features"][0]["feature_weight_multiplier"] = 1.2
    with pytest.raises(BoundaryViolationError, match="feature_weight_multiplier"):
        storage.append_drift_report(report)

    assert len(storage.rows) == 1
