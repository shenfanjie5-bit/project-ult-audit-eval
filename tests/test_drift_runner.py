import builtins
import socket
import urllib.request
from collections.abc import Sequence
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import pytest

from audit_eval.contracts import DriftReport
from audit_eval.drift import (
    ALERT_RULES_VERSION,
    DriftInputError,
    DriftRuleConfig,
    DriftRunnerError,
    DriftStorageError,
    DriftedFeature,
    EvidentlyReportRunner,
    EvidentlyRunResult,
    InMemoryDriftInputGateway,
    InMemoryDriftReportJsonWriter,
    InMemoryDriftReportStorage,
    build_drift_alert_payload,
    get_default_evidently_runner,
    get_default_input_gateway,
    get_default_json_writer,
    get_default_report_storage,
    run_drift_report,
)


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_network_call(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Drift runner tests must not call network")

    monkeypatch.setattr(urllib.request, "urlopen", fail_network_call)
    monkeypatch.setattr(socket, "socket", fail_network_call)


class TrackingEvidentlyRunner:
    def __init__(self, result: EvidentlyRunResult) -> None:
        self.result = result
        self.calls: list[tuple[Any, Any]] = []

    def run(self, reference_data: Any, target_data: Any) -> EvidentlyRunResult:
        self.calls.append((reference_data, target_data))
        return self.result


class TrackingJsonWriter(InMemoryDriftReportJsonWriter):
    def __init__(self, events: list[str], ref: str) -> None:
        super().__init__()
        self.events = events
        self.ref = ref

    def write_report_json(self, report_id: str, payload: dict[str, Any]) -> str:
        self.events.append("json_writer")
        super().write_report_json(report_id, payload)
        return self.ref


class TrackingStorage(InMemoryDriftReportStorage):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events

    def append_drift_report(self, report: DriftReport) -> str:
        self.events.append("storage")
        return super().append_drift_report(report)


def _run_result(features: Sequence[DriftedFeature]) -> EvidentlyRunResult:
    return EvidentlyRunResult(
        evidently_json={
            "metrics": [
                {
                    "metric": "DataDriftPreset",
                    "result": {"dataset_drift": True},
                }
            ]
        },
        drifted_features=tuple(features),
        feature_count=len(features),
    )


def _feature(name: str, *, drifted: bool) -> DriftedFeature:
    return DriftedFeature(
        name=name,
        score=0.42 if drifted else 0.1,
        threshold=0.30,
        drifted=drifted,
        statistic=0.42 if drifted else 0.1,
        details={"stattest_name": "ks"},
    )


def test_run_drift_report_delegates_all_boundaries_and_appends_once() -> None:
    gateway = InMemoryDriftInputGateway(
        {
            "feature-window://reference": {"feature_a": [1, 2, 3]},
            "feature-window://target": {"feature_a": [2, 3, 4]},
        }
    )
    runner = TrackingEvidentlyRunner(
        _run_result(
            [
                _feature("feature_a", drifted=True),
                _feature("feature_b", drifted=False),
            ]
        )
    )
    events: list[str] = []
    writer = TrackingJsonWriter(events, ref="catalog://provided/drift-report.json")
    storage = TrackingStorage(events)
    created_at = datetime(2026, 4, 18, 12, tzinfo=timezone.utc)

    report = run_drift_report(
        "feature-window://reference",
        "feature-window://target",
        cycle_id="cycle_20260418",
        input_gateway=gateway,
        evidently_runner=runner,
        json_writer=writer,
        storage=storage,
        rules=DriftRuleConfig(
            warning_drifted_feature_count=1,
            critical_drifted_feature_count=3,
            warning_drift_share=0.25,
            critical_drift_share=0.75,
        ),
        created_at=created_at,
    )

    assert gateway.loaded_refs == [
        "feature-window://reference",
        "feature-window://target",
    ]
    assert runner.calls == [
        ({"feature_a": [1, 2, 3]}, {"feature_a": [2, 3, 4]})
    ]
    assert events == ["json_writer", "storage"]
    assert len(writer.calls) == 1
    assert writer.calls[0][0] == report.report_id
    assert writer.calls[0][1] == runner.result.evidently_json
    assert report.evidently_json_ref == "catalog://provided/drift-report.json"
    assert report.alert_rules_version == ALERT_RULES_VERSION
    assert report.regime_warning_level == "warning"
    assert report.baseline_ref == "feature-window://reference"
    assert report.target_ref == "feature-window://target"
    assert report.created_at == created_at
    assert len(storage.rows) == 1
    assert storage.rows[0]["report_id"] == report.report_id


def test_build_drift_alert_payload_contains_only_structural_warning_fields() -> None:
    report = DriftReport(
        report_id="drift-1",
        cycle_id=None,
        baseline_ref="feature-window://reference",
        target_ref="feature-window://target",
        evidently_json_ref="catalog://provided/drift-report.json",
        drifted_features=[
            asdict(_feature("feature_a", drifted=True)),
            asdict(_feature("feature_b", drifted=False)),
        ],
        regime_warning_level="warning",
        alert_rules_version=ALERT_RULES_VERSION,
        created_at=datetime(2026, 4, 18, 12, tzinfo=timezone.utc),
    )

    payload = build_drift_alert_payload(report)

    assert asdict(payload) == {
        "report_id": "drift-1",
        "regime_warning_level": "warning",
        "drifted_features": ("feature_a",),
        "evidently_json_ref": "catalog://provided/drift-report.json",
    }
    assert "weight" not in repr(payload)
    assert "multiplier" not in repr(payload)
    assert "gate_action" not in repr(payload)


def test_default_drift_boundaries_fail_closed() -> None:
    with pytest.raises(DriftInputError, match="pass input_gateway"):
        get_default_input_gateway()
    with pytest.raises(DriftRunnerError, match="pass evidently_runner"):
        get_default_evidently_runner()
    with pytest.raises(DriftStorageError, match="pass json_writer"):
        get_default_json_writer()
    with pytest.raises(DriftStorageError, match="pass storage"):
        get_default_report_storage()


def test_run_drift_report_fails_closed_for_missing_default_adapters() -> None:
    with pytest.raises(DriftInputError, match="pass input_gateway"):
        run_drift_report("reference", "target")

    gateway = InMemoryDriftInputGateway({"reference": {}, "target": {}})
    with pytest.raises(DriftRunnerError, match="pass evidently_runner"):
        run_drift_report("reference", "target", input_gateway=gateway)

    runner = TrackingEvidentlyRunner(_run_result([]))
    with pytest.raises(DriftStorageError, match="pass json_writer"):
        run_drift_report(
            "reference",
            "target",
            input_gateway=gateway,
            evidently_runner=runner,
        )

    writer = InMemoryDriftReportJsonWriter()
    with pytest.raises(DriftStorageError, match="pass storage"):
        run_drift_report(
            "reference",
            "target",
            input_gateway=gateway,
            evidently_runner=runner,
            json_writer=writer,
        )


def test_evidently_report_runner_lazy_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import = builtins.__import__

    def blocked_import(name: str, *args: object, **kwargs: object) -> object:
        if name.startswith("evidently"):
            raise ImportError("blocked evidently import")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_import)

    with pytest.raises(DriftRunnerError, match="Evidently is not importable"):
        EvidentlyReportRunner().run([], [])
