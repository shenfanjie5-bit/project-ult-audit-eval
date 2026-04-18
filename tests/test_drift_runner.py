import socket
import urllib.request
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import pytest

from audit_eval.drift import (
    ALERT_RULES_VERSION,
    DriftInputError,
    DriftRunnerError,
    DriftStorageError,
    DriftedFeature,
    EvidentlyDataDriftRunner,
    EvidentlyRunResult,
    InMemoryDriftInputGateway,
    InMemoryDriftReportStorage,
    InMemoryEvidentlyRunner,
    build_drift_alert_payload,
    run_drift_report,
)
from audit_eval.drift.storage import DriftReportJsonWriter


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_network_call(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Drift report tests must not call network")

    monkeypatch.setattr(urllib.request, "urlopen", fail_network_call)
    monkeypatch.setattr(socket, "create_connection", fail_network_call)
    monkeypatch.setattr(socket.socket, "connect", fail_network_call)


class FixedRefJsonWriter:
    def __init__(self, ref: str = "platform://drift/report.json") -> None:
        self.ref = ref
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def write_report_json(self, report_id: str, payload: dict[str, Any]) -> str:
        self.calls.append((report_id, payload))
        return self.ref


def _result() -> EvidentlyRunResult:
    return EvidentlyRunResult(
        report_json={
            "metrics": [
                {
                    "column_name": "alpha",
                    "drift_detected": True,
                    "drift_score": 0.7,
                    "threshold": 0.2,
                },
                {
                    "column_name": "beta",
                    "drift_detected": False,
                    "drift_score": 0.1,
                    "threshold": 0.2,
                },
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
            DriftedFeature(
                name="beta",
                score=0.1,
                statistic=None,
                threshold=0.2,
                drifted=False,
            ),
        ),
        feature_count=5,
    )


def test_run_drift_report_uses_injected_boundaries_and_persists_report() -> None:
    created_at = datetime(2026, 4, 18, 8, 30, tzinfo=timezone.utc)
    gateway = InMemoryDriftInputGateway(
        {
            "feature://baseline": {"alpha": [1, 2], "beta": [3, 4]},
            "feature://target": {"alpha": [5, 6], "beta": [7, 8]},
        }
    )
    evidently_runner = InMemoryEvidentlyRunner(_result())
    json_writer = FixedRefJsonWriter()
    storage = InMemoryDriftReportStorage()

    report = run_drift_report(
        "feature://baseline",
        "feature://target",
        cycle_id="cycle_20260418",
        input_gateway=gateway,
        evidently_runner=evidently_runner,
        json_writer=json_writer,
        storage=storage,
        created_at=created_at,
    )

    assert gateway.loaded_refs == ["feature://baseline", "feature://target"]
    assert evidently_runner.calls == [
        (
            {"alpha": [1, 2], "beta": [3, 4]},
            {"alpha": [5, 6], "beta": [7, 8]},
        )
    ]
    assert len(json_writer.calls) == 1
    assert json_writer.calls[0][1] == _result().report_json
    assert report.evidently_json_ref == "platform://drift/report.json"
    assert report.report_id == "drift-feature-baseline-feature-target-20260418T083000Z"
    assert report.cycle_id == "cycle_20260418"
    assert report.regime_warning_level == "warning"
    assert report.alert_rules_version == ALERT_RULES_VERSION
    assert report.drifted_features == {
        "features": [
            {
                "name": "alpha",
                "score": 0.7,
                "statistic": None,
                "threshold": 0.2,
                "drifted": True,
            }
        ]
    }
    assert storage.reports == [report]
    assert storage.rows[0]["report_id"] == report.report_id


def test_build_drift_alert_payload_contains_only_structural_warning_fields() -> None:
    report = run_drift_report(
        "baseline",
        "target",
        input_gateway=InMemoryDriftInputGateway(
            {
                "baseline": {"alpha": [1], "beta": [2]},
                "target": {"alpha": [3], "beta": [4]},
            }
        ),
        evidently_runner=InMemoryEvidentlyRunner(_result()),
        json_writer=FixedRefJsonWriter(),
        storage=InMemoryDriftReportStorage(),
        created_at=datetime(2026, 4, 18, tzinfo=timezone.utc),
    )

    payload = build_drift_alert_payload(report)

    assert asdict(payload) == {
        "report_id": report.report_id,
        "regime_warning_level": "warning",
        "drifted_features": ("alpha",),
        "evidently_json_ref": "platform://drift/report.json",
    }
    assert "gate_action" not in asdict(payload)
    assert "feature_weight_multiplier" not in repr(asdict(payload))
    assert "online_control" not in repr(asdict(payload))


def test_default_gateway_fails_closed() -> None:
    with pytest.raises(DriftInputError, match="pass input_gateway"):
        run_drift_report("baseline", "target")


def test_default_json_writer_fails_closed_after_evidently_validation() -> None:
    with pytest.raises(DriftStorageError, match="pass json_writer"):
        run_drift_report(
            "baseline",
            "target",
            input_gateway=InMemoryDriftInputGateway(
                {
                    "baseline": {"alpha": [1]},
                    "target": {"alpha": [2]},
                }
            ),
            evidently_runner=InMemoryEvidentlyRunner(_result()),
            storage=InMemoryDriftReportStorage(),
        )


def test_default_storage_fails_closed_after_json_writer_delegate() -> None:
    json_writer = FixedRefJsonWriter()

    with pytest.raises(DriftStorageError, match="pass storage"):
        run_drift_report(
            "baseline",
            "target",
            input_gateway=InMemoryDriftInputGateway(
                {
                    "baseline": {"alpha": [1]},
                    "target": {"alpha": [2]},
                }
            ),
            evidently_runner=InMemoryEvidentlyRunner(_result()),
            json_writer=json_writer,
        )

    assert len(json_writer.calls) == 1


def test_default_evidently_runner_reports_missing_lazy_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_import_module(name: str) -> object:
        if name.startswith("evidently."):
            raise ImportError("blocked")
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(
        "audit_eval.drift.storage.import_module",
        fake_import_module,
    )

    with pytest.raises(DriftRunnerError, match="Evidently is unavailable"):
        EvidentlyDataDriftRunner().run([], [])


def test_evidently_data_drift_runner_smoke_current_api() -> None:
    pd = pytest.importorskip("pandas")
    reference_data = pd.DataFrame(
        {
            "alpha": [1.0, 1.1, 1.2, 1.3, 1.4, 1.5],
            "beta": [1, 1, 1, 1, 1, 1],
        }
    )
    target_data = pd.DataFrame(
        {
            "alpha": [5.0, 5.1, 5.2, 5.3, 5.4, 5.5],
            "beta": [1, 1, 1, 1, 1, 1],
        }
    )

    result = EvidentlyDataDriftRunner().run(reference_data, target_data)

    assert result.report_json["metrics"]
    features = {feature.name: feature for feature in result.drifted_features}
    assert {"alpha", "beta"}.issubset(features)
    assert features["alpha"].drifted is True
    assert features["alpha"].score is not None
    assert features["alpha"].threshold == pytest.approx(0.05)
    assert features["beta"].drifted is False
    assert result.feature_count >= 2


def test_json_writer_protocol_is_runtime_shape_only() -> None:
    writer: DriftReportJsonWriter = FixedRefJsonWriter()

    assert writer.write_report_json("report-id", {"ok": True}) == (
        "platform://drift/report.json"
    )
