from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from audit_eval._boundary import BoundaryViolationError
from audit_eval.contracts import DriftReport
from audit_eval.drift import (
    DriftRuleConfig,
    DriftedFeature,
    EvidentlyRunResult,
    InMemoryDriftInputGateway,
    InMemoryDriftReportStorage,
    InMemoryEvidentlyRunner,
    run_drift_report,
)
import audit_eval.drift.runner as drift_runner_module
from audit_eval.drift.storage import InMemoryDriftReportJsonWriter


class FrameLike:
    def __init__(self, columns: list[str]) -> None:
        self.columns = columns


def _valid_report_payload() -> dict[str, Any]:
    return {
        "report_id": "drift-baseline-target-20260418T000000Z",
        "cycle_id": "cycle_20260418",
        "baseline_ref": "baseline",
        "target_ref": "target",
        "evidently_json_ref": "platform://drift/report.json",
        "drifted_features": {
            "features": [
                {
                    "name": "alpha",
                    "score": 0.7,
                    "statistic": None,
                    "threshold": 0.2,
                    "drifted": True,
                }
            ]
        },
        "regime_warning_level": "warning",
        "alert_rules_version": "drift-alert-rules-v1",
        "created_at": datetime(2026, 4, 18, tzinfo=timezone.utc),
    }


def _safe_result() -> EvidentlyRunResult:
    return EvidentlyRunResult(
        report_json={
            "metrics": [
                {
                    "column_name": "alpha",
                    "drift_detected": True,
                    "drift_score": 0.7,
                    "threshold": 0.2,
                }
            ]
        },
        drifted_features=(
            DriftedFeature(
                name="alpha",
                score=0.7,
                statistic=None,
                threshold=0.2,
                drifted=True,
            ),
        ),
        feature_count=1,
    )


def _run_with_result(
    result: EvidentlyRunResult,
    *,
    gateway: InMemoryDriftInputGateway | None = None,
    writer: InMemoryDriftReportJsonWriter | None = None,
    storage: InMemoryDriftReportStorage | None = None,
    rules: DriftRuleConfig | None = None,
) -> None:
    run_drift_report(
        "baseline",
        "target",
        input_gateway=gateway
        or InMemoryDriftInputGateway(
            {
                "baseline": {"alpha": [1]},
                "target": {"alpha": [2]},
            }
        ),
        evidently_runner=InMemoryEvidentlyRunner(result),
        json_writer=writer or InMemoryDriftReportJsonWriter(),
        storage=storage or InMemoryDriftReportStorage(),
        rules=rules,
        created_at=datetime(2026, 4, 18, tzinfo=timezone.utc),
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


def test_drift_report_rejects_extra_fields() -> None:
    payload = _valid_report_payload()
    payload["extra"] = "forbidden"

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DriftReport.model_validate(payload)


def test_drift_report_rejects_unknown_warning_level() -> None:
    payload = _valid_report_payload()
    payload["regime_warning_level"] = "gate"

    with pytest.raises(ValidationError, match="regime_warning_level"):
        DriftReport.model_validate(payload)


def test_drift_report_rejects_nested_forbidden_write_fields() -> None:
    payload = _valid_report_payload()
    payload["drifted_features"]["features"][0]["metadata"] = {
        "feature_weight_multiplier": 1.2
    }

    with pytest.raises(BoundaryViolationError, match="feature_weight_multiplier"):
        DriftReport.model_validate(payload)


def test_drift_report_rejects_control_feature_name_string_values() -> None:
    payload = _valid_report_payload()
    payload["drifted_features"] = {"features": ["feature_weight_multiplier"]}

    with pytest.raises(BoundaryViolationError, match="drifted_features"):
        DriftReport.model_validate(payload)


@pytest.mark.parametrize("warning_level", ["warning", "critical"])
def test_drift_report_rejects_alert_without_feature_evidence(
    warning_level: str,
) -> None:
    payload = _valid_report_payload()
    payload["regime_warning_level"] = warning_level
    payload["drifted_features"] = {"features": []}

    with pytest.raises(ValidationError, match="at least one drifted feature"):
        DriftReport.model_validate(payload)


def test_drift_report_rejects_malformed_feature_evidence() -> None:
    payload = _valid_report_payload()
    payload["drifted_features"] = {
        "features": [{"name": "alpha", "score": 0.7, "drifted": True}]
    }

    with pytest.raises(ValidationError, match="threshold"):
        DriftReport.model_validate(payload)


def test_runner_rejects_evidently_json_forbidden_field_before_writes() -> None:
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()
    result = _safe_result()
    result = EvidentlyRunResult(
        report_json={
            "metrics": [
                {
                    "column_name": "alpha",
                    "drift_detected": True,
                    "feature_weight_multiplier": 1.2,
                }
            ]
        },
        drifted_features=result.drifted_features,
        feature_count=result.feature_count,
    )

    with pytest.raises(BoundaryViolationError, match="feature_weight_multiplier"):
        _run_with_result(result, writer=writer, storage=storage)

    assert writer.calls == []
    assert storage.rows == []


def test_runner_rejects_control_feature_name_in_evidently_json_before_writes() -> None:
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()
    result = EvidentlyRunResult(
        report_json={
            "metrics": [
                {
                    "feature_name": "online_control",
                    "drift_detected": True,
                    "drift_score": 0.7,
                }
            ]
        },
        drifted_features=_safe_result().drifted_features,
        feature_count=1,
    )

    with pytest.raises(BoundaryViolationError, match="feature_name"):
        _run_with_result(result, writer=writer, storage=storage)

    assert writer.calls == []
    assert storage.rows == []


@pytest.mark.parametrize(
    ("reference_ref", "target_ref", "cycle_id"),
    [
        ("feature_weight_multiplier", "target", None),
        ("baseline", "feature://feature_weight_multiplier", None),
        ("baseline", "target", "cycle_feature_weight_multiplier"),
        ("baseline", "target", "online_control"),
    ],
)
def test_runner_rejects_forbidden_scalar_inputs_before_writes(
    reference_ref: str,
    target_ref: str,
    cycle_id: str | None,
) -> None:
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()
    runner = InMemoryEvidentlyRunner(_safe_result())
    gateway = InMemoryDriftInputGateway(
        {
            "baseline": {"alpha": [1]},
            "target": {"alpha": [2]},
            "feature_weight_multiplier": {"alpha": [3]},
            "feature://feature_weight_multiplier": {"alpha": [4]},
        }
    )

    with pytest.raises(
        BoundaryViolationError,
        match="feature_weight_multiplier|online_control",
    ):
        run_drift_report(
            reference_ref,
            target_ref,
            cycle_id=cycle_id,
            input_gateway=gateway,
            evidently_runner=runner,
            json_writer=writer,
            storage=storage,
        )

    assert gateway.loaded_refs == []
    assert runner.calls == []
    assert writer.calls == []
    assert storage.rows == []


def test_runner_rejects_forbidden_report_id_before_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()
    monkeypatch.setattr(
        drift_runner_module,
        "_build_report_id",
        lambda *_args: "drift-feature_weight_multiplier-target",
    )

    with pytest.raises(BoundaryViolationError, match="feature_weight_multiplier"):
        run_drift_report(
            "baseline",
            "target",
            input_gateway=InMemoryDriftInputGateway(
                {
                    "baseline": {"alpha": [1]},
                    "target": {"alpha": [2]},
                }
            ),
            evidently_runner=InMemoryEvidentlyRunner(_safe_result()),
            json_writer=writer,
            storage=storage,
        )

    assert writer.calls == []
    assert storage.rows == []


def test_runner_rejects_warning_without_feature_evidence_before_writes() -> None:
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()
    result = EvidentlyRunResult(
        report_json={"metrics": []},
        drifted_features=(),
        feature_count=0,
    )

    with pytest.raises(ValidationError, match="at least one drifted feature"):
        _run_with_result(
            result,
            writer=writer,
            storage=storage,
            gateway=InMemoryDriftInputGateway(
                {
                    "baseline": {"alpha": [1]},
                    "target": {"alpha": [2]},
                }
            ),
            rules=DriftRuleConfig(
                warning_drift_share=0.0,
                critical_drift_share=1.0,
                warning_drifted_feature_count=10,
                critical_drifted_feature_count=10,
            ),
        )

    assert writer.calls == []
    assert storage.rows == []


def test_runner_rejects_control_name_in_drifted_features_before_writes() -> None:
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()
    result = EvidentlyRunResult(
        report_json=_safe_result().report_json,
        drifted_features=(
            DriftedFeature(
                name="feature_weight_multiplier",
                score=0.7,
                statistic=None,
                threshold=0.2,
                drifted=True,
            ),
        ),
        feature_count=1,
    )

    with pytest.raises(BoundaryViolationError, match="drifted_features"):
        _run_with_result(result, writer=writer, storage=storage)

    assert writer.calls == []
    assert storage.rows == []


def test_runner_rejects_frame_like_control_columns_before_evidently() -> None:
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()
    runner = InMemoryEvidentlyRunner(_safe_result())
    gateway = InMemoryDriftInputGateway(
        {
            "baseline": FrameLike(["online_control"]),
            "target": FrameLike(["alpha"]),
        }
    )

    with pytest.raises(BoundaryViolationError, match="columns"):
        run_drift_report(
            "baseline",
            "target",
            input_gateway=gateway,
            evidently_runner=runner,
            json_writer=writer,
            storage=storage,
        )

    assert runner.calls == []
    assert writer.calls == []
    assert storage.rows == []


def test_runner_rejects_nested_input_forbidden_field_before_evidently() -> None:
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()
    runner = InMemoryEvidentlyRunner(_safe_result())
    gateway = InMemoryDriftInputGateway(
        {
            "baseline": {
                "features": [
                    {"metadata": {"feature_weight_multiplier": 1.0}},
                ]
            },
            "target": {"alpha": [2]},
        }
    )

    with pytest.raises(BoundaryViolationError, match="feature_weight_multiplier"):
        run_drift_report(
            "baseline",
            "target",
            input_gateway=gateway,
            evidently_runner=runner,
            json_writer=writer,
            storage=storage,
        )

    assert runner.calls == []
    assert writer.calls == []
    assert storage.rows == []


def test_runner_rejects_pandas_control_columns_before_evidently() -> None:
    pd = pytest.importorskip("pandas")
    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()
    runner = InMemoryEvidentlyRunner(_safe_result())
    gateway = InMemoryDriftInputGateway(
        {
            "baseline": pd.DataFrame({"feature_weight_multiplier": [1.0]}),
            "target": pd.DataFrame({"alpha": [2.0]}),
        }
    )

    with pytest.raises(BoundaryViolationError, match="columns"):
        run_drift_report(
            "baseline",
            "target",
            input_gateway=gateway,
            evidently_runner=runner,
            json_writer=writer,
            storage=storage,
        )

    assert runner.calls == []
    assert writer.calls == []
    assert storage.rows == []


def test_runner_rejects_arrow_like_schema_control_columns_before_evidently() -> None:
    class Schema:
        names = ["feature_weight_multiplier"]

    class TableLike:
        schema = Schema()

    writer = InMemoryDriftReportJsonWriter()
    storage = InMemoryDriftReportStorage()
    runner = InMemoryEvidentlyRunner(_safe_result())
    gateway = InMemoryDriftInputGateway(
        {
            "baseline": TableLike(),
            "target": FrameLike(["alpha"]),
        }
    )

    with pytest.raises(BoundaryViolationError, match="columns"):
        run_drift_report(
            "baseline",
            "target",
            input_gateway=gateway,
            evidently_runner=runner,
            json_writer=writer,
            storage=storage,
        )

    assert runner.calls == []
    assert writer.calls == []
    assert storage.rows == []
