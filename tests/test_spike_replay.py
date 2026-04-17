import json
import socket
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from audit_eval.audit import ReplayQueryContext
from scripts import spike_replay


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "spike"


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_network_call(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Replay spike CLI must not call network")

    monkeypatch.setattr(urllib.request, "urlopen", fail_network_call)
    monkeypatch.setattr(socket, "socket", fail_network_call)


def test_cli_outputs_package_replay_view_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = spike_replay.main(
        [
            "--cycle-id",
            "cycle_20260410",
            "--object-ref",
            "recommendation",
            "--fixtures",
            str(FIXTURE_ROOT),
        ]
    )

    assert exit_code == 0
    replay_view = json.loads(capsys.readouterr().out)

    assert set(replay_view) >= {
        "audit_records",
        "dagster_run_summary",
        "graph_snapshot",
        "graph_snapshot_ref",
        "graph_snapshot_summary",
        "historical_formal_objects",
        "manifest_snapshot_set",
        "replay_record",
    }
    assert replay_view["object_ref"] == "recommendation"
    assert replay_view["graph_snapshot_ref"] == (
        "graph://cycle_20260410/portfolio_graph"
    )
    assert replay_view["graph_snapshot"]["graph_snapshot_ref"] == (
        replay_view["graph_snapshot_ref"]
    )
    assert replay_view["dagster_run_summary"]["run_id"] == (
        "dagster-fixture-run-20260410"
    )


def test_cli_core_path_calls_package_replay_query(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: dict[str, Any] = {}

    class FakeReplayView:
        def to_dict(self) -> dict[str, Any]:
            return {
                "cycle_id": "cycle_20260410",
                "object_ref": "recommendation",
                "via": "package-api",
            }

    def fake_replay_cycle_object(
        cycle_id: str,
        object_ref: str,
        context: ReplayQueryContext | None = None,
    ) -> FakeReplayView:
        calls["cycle_id"] = cycle_id
        calls["object_ref"] = object_ref
        calls["context"] = context
        return FakeReplayView()

    monkeypatch.setattr(
        spike_replay,
        "replay_cycle_object",
        fake_replay_cycle_object,
    )

    exit_code = spike_replay.main(
        [
            "--cycle-id",
            "cycle_20260410",
            "--object-ref",
            "recommendation",
            "--fixtures",
            str(FIXTURE_ROOT),
        ]
    )
    replay_view = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert replay_view == {
        "cycle_id": "cycle_20260410",
        "object_ref": "recommendation",
        "via": "package-api",
    }
    assert calls["cycle_id"] == "cycle_20260410"
    assert calls["object_ref"] == "recommendation"
    assert isinstance(calls["context"], ReplayQueryContext)
