import socket
import urllib.request
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

import pytest

from audit_eval.contracts.drift_report import DriftReport
from audit_eval.drift import (
    ALERT_RULES_VERSION,
    DriftInputError,
    DriftedFeature,
    DriftStorageError,
    EvidentlyReportRunner,
    EvidentlyRunResult,
    EvidentlyRunnerError,
    InMemoryDriftInputGateway,
    InMemoryDriftReportJsonWriter,
    InMemoryDriftReportStorage,
    StaticEvidentlyRunner,
    build_drift_alert_payload,
    run_drift_report,
)


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_network_call(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Drift report tests must not call network")

    monkeypatch.setattr(urllib.request, "urlopen", fail_network_call)
    monkeypatch.setattr(socket, "create_connection", fail_network_call)


class ReturningWriter:
    def __init__(self, returned_ref: str) -> None:
        self.returned_ref = returned_ref
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def write_report_json(self, report_id: str, payload: dict[str, Any]) -> str:
        self.calls.append((report_id, payload))
        return self.returned_ref


class CountingStorage(InMemoryDriftReportStorage):
    def __init__(self) -> None:
        super().__init__()
        self.append_calls = 0

    def append_drift_report(self, report: DriftReport) -> str:
        self.append_calls += 1
        return super().append_drift_report(report)


def _run_result() -> EvidentlyRunResult:
    return EvidentlyRunResult(
        report_json={
            "metrics": [
                {
                    "metric": "DataDriftPreset",
                    "result": {"number_of_columns": 5},
                }
            ]
        },
        drifted_features=(
            DriftedFeature(
                name="spread",
                score=0.82,
                statistic=0.31,
                threshold=0.05,
                drifted=True,
            ),
        ),
        total_feature_count=5,
    )


def test_run_drift_report_uses_all_boundaries_and_appends_once() -> None:
    gateway = InMemoryDriftInputGateway(
        {
            "baseline-ref": [{"spread": 1.0}],
            "target-ref": [{"spread": 3.0}],
        }
    )
    evidently_runner = StaticEvidentlyRunner(_run_result())
    writer = InMemoryDriftReportJsonWriter(ref_prefix="platform://reports")
    storage = CountingStorage()
    created_at = datetime(2026, 4, 18, 1, 2, 3, tzinfo=timezone.utc)

    report = run_drift_report(
        "baseline-ref",
        "target-ref",
        cycle_id="cycle_20260418",
        input_gateway=gateway,
        evidently_runner=evidently_runner,
        json_writer=writer,
        storage=storage,
        created_at=created_at,
    )

    assert gateway.loaded_refs == ["baseline-ref", "target-ref"]
    assert evidently_runner.calls == [([{"spread": 1.0}], [{"spread": 3.0}])]
    assert writer.write_calls == 1
    assert storage.append_calls == 1
    assert report.report_id == (
        "drift-baseline-ref-target-ref-20260418T010203000000Z"
    )
    assert report.evidently_json_ref == (
        "platform://reports/"
        "drift-baseline-ref-target-ref-20260418T010203000000Z.json"
    )
    assert report.regime_warning_level == "warning"
    assert report.alert_rules_version == ALERT_RULES_VERSION
    assert report.drifted_features == {
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
    }
    assert storage.rows[0]["report_id"] == report.report_id


def test_run_drift_report_uses_json_ref_returned_by_writer() -> None:
    gateway = InMemoryDriftInputGateway({"reference": [], "target": []})
    evidently_runner = StaticEvidentlyRunner(_run_result())
    writer = ReturningWriter("catalog://owned/by-data-platform")
    storage = CountingStorage()

    report = run_drift_report(
        "reference",
        "target",
        input_gateway=gateway,
        evidently_runner=evidently_runner,
        json_writer=writer,
        storage=storage,
        created_at=datetime(2026, 4, 18, tzinfo=timezone.utc),
    )

    assert report.evidently_json_ref == "catalog://owned/by-data-platform"
    assert writer.calls == [(report.report_id, _run_result().report_json)]
    assert storage.append_calls == 1


def test_run_drift_report_default_boundaries_fail_closed() -> None:
    with pytest.raises(DriftInputError, match="pass input_gateway"):
        run_drift_report("reference", "target")


def test_json_writer_and_storage_defaults_fail_closed() -> None:
    gateway = InMemoryDriftInputGateway({"reference": [], "target": []})
    evidently_runner = StaticEvidentlyRunner(_run_result())

    with pytest.raises(EvidentlyRunnerError, match="pass evidently_runner"):
        run_drift_report(
            "reference",
            "target",
            input_gateway=gateway,
        )

    with pytest.raises(DriftStorageError, match="pass json_writer"):
        run_drift_report(
            "reference",
            "target",
            input_gateway=gateway,
            evidently_runner=evidently_runner,
        )

    with pytest.raises(DriftStorageError, match="pass storage"):
        run_drift_report(
            "reference",
            "target",
            input_gateway=gateway,
            evidently_runner=evidently_runner,
            json_writer=ReturningWriter("catalog://report.json"),
        )


def test_build_drift_alert_payload_derives_feature_names() -> None:
    gateway = InMemoryDriftInputGateway({"reference": [], "target": []})
    report = run_drift_report(
        "reference",
        "target",
        input_gateway=gateway,
        evidently_runner=StaticEvidentlyRunner(_run_result()),
        json_writer=ReturningWriter("catalog://report.json"),
        storage=CountingStorage(),
        created_at=datetime(2026, 4, 18, tzinfo=timezone.utc),
    )

    payload = build_drift_alert_payload(report)

    assert payload.report_id == report.report_id
    assert payload.regime_warning_level == "warning"
    assert payload.drifted_features == ("spread",)
    assert payload.evidently_json_ref == "catalog://report.json"


@pytest.mark.filterwarnings("ignore:numpy.core is deprecated.*:DeprecationWarning")
@pytest.mark.filterwarnings("ignore:.*invalid escape sequence.*:SyntaxWarning")
def test_evidently_report_runner_lazy_import_or_smoke() -> None:
    runner = EvidentlyReportRunner()
    reference_data: Sequence[dict[str, float]] | Any = [
        {"spread": 1.0 + index * 0.01} for index in range(50)
    ]
    target_data: Sequence[dict[str, float]] | Any = [
        {"spread": 3.0 + index * 0.01} for index in range(50)
    ]
    try:
        import pandas as pd
    except Exception:
        pass
    else:
        reference_data = pd.DataFrame(reference_data)
        target_data = pd.DataFrame(target_data)

    try:
        result = runner.run(reference_data, target_data)
    except EvidentlyRunnerError as exc:
        assert "Evidently" in str(exc) or "failed" in str(exc)
    else:
        assert result.report_json
        assert result.drifted_features
        feature = result.drifted_features[0]
        assert feature.name == "spread"
        assert feature.score is not None
        assert feature.threshold is not None
        assert feature.drifted is True
