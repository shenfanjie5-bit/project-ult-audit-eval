import json
import shutil
import socket
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from audit_eval.contracts.replay_draft import AuditRecordDraft, ReplayRecordDraft
from scripts.spike_replay import reconstruct_replay_view


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "spike"


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_network_call(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Replay spike must not call network")

    monkeypatch.setattr(urllib.request, "urlopen", fail_network_call)
    monkeypatch.setattr(socket, "socket", fail_network_call)


def test_reconstruct_returns_manifest_bound_snapshots() -> None:
    replay_view = reconstruct_replay_view(
        cycle_id="cycle_20260410",
        object_ref="recommendation",
        fixture_root=FIXTURE_ROOT,
    )

    assert set(replay_view) >= {
        "audit_records",
        "manifest_snapshot_set",
        "historical_formal_objects",
    }
    manifest_refs = set(replay_view["manifest_snapshot_set"].values())
    historical_objects = replay_view["historical_formal_objects"]

    assert set(historical_objects) == {"world_state", "recommendation"}
    for historical_object in historical_objects.values():
        assert historical_object["source_ref"] in manifest_refs


def test_reconstruct_preserves_replay_lineage_fields() -> None:
    replay_view = reconstruct_replay_view(
        cycle_id="cycle_20260410",
        object_ref="recommendation",
        fixture_root=FIXTURE_ROOT,
    )

    replay_record = replay_view["replay_record"]
    assert replay_record["graph_snapshot_ref"] == (
        "graph://cycle_20260410/portfolio_graph"
    )
    assert replay_record["created_at"] == "2026-04-10T16:09:00Z"


def test_manifest_ref_selects_snapshot_file_not_object_name(tmp_path: Path) -> None:
    fixture_copy = tmp_path / "spike"
    shutil.copytree(FIXTURE_ROOT, fixture_copy)
    cycle_fixture = fixture_copy / "cycle_20260410"

    manifest_snapshot_ref = (
        "snapshot://cycle_20260410/formal_snapshots/"
        "recommendation_manifest_bound.json"
    )
    manifest_path = cycle_fixture / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["snapshot_refs"]["recommendation"] = manifest_snapshot_ref
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    replay_records_path = cycle_fixture / "replay_records.json"
    replay_records = json.loads(replay_records_path.read_text(encoding="utf-8"))
    for replay_record in replay_records:
        if replay_record["object_ref"] == "recommendation":
            replay_record["formal_snapshot_refs"][
                "recommendation"
            ] = manifest_snapshot_ref
    replay_records_path.write_text(json.dumps(replay_records), encoding="utf-8")

    bound_snapshot_path = (
        cycle_fixture / "formal_snapshots" / "recommendation_manifest_bound.json"
    )
    bound_snapshot = {
        "snapshot_ref": manifest_snapshot_ref,
        "object_ref": "recommendation",
        "as_of": "2026-04-10",
        "recommendation": {
            "action": "raise_cash",
            "confidence": 0.91,
            "source_world_state_ref": "snapshot://cycle_20260410/world_state",
        },
    }
    bound_snapshot_path.write_text(json.dumps(bound_snapshot), encoding="utf-8")

    replay_view = reconstruct_replay_view(
        cycle_id="cycle_20260410",
        object_ref="recommendation",
        fixture_root=fixture_copy,
    )

    recommendation = replay_view["historical_formal_objects"]["recommendation"]
    assert recommendation["source_ref"] == manifest_snapshot_ref
    assert recommendation["data"]["snapshot_ref"] == manifest_snapshot_ref
    assert recommendation["data"]["recommendation"]["action"] == "raise_cash"

    bound_snapshot["snapshot_ref"] = "snapshot://cycle_20260410/recommendation"
    bound_snapshot_path.write_text(json.dumps(bound_snapshot), encoding="utf-8")

    with pytest.raises(ValueError, match="not bound to manifest ref"):
        reconstruct_replay_view(
            cycle_id="cycle_20260410",
            object_ref="recommendation",
            fixture_root=fixture_copy,
        )


def test_reconstruct_does_not_call_network() -> None:
    replay_view = reconstruct_replay_view(
        cycle_id="cycle_20260410",
        object_ref="recommendation",
        fixture_root=FIXTURE_ROOT,
    )

    assert replay_view["object_ref"] == "recommendation"


def test_replay_record_rejects_non_read_history_mode() -> None:
    with pytest.raises(ValidationError):
        ReplayRecordDraft(
            replay_id="replay-invalid",
            cycle_id="cycle_20260410",
            object_ref="recommendation",
            audit_record_ids=["audit-cycle_20260410-L7-recommendation"],
            manifest_cycle_id="cycle_20260410",
            formal_snapshot_refs={
                "recommendation": "snapshot://cycle_20260410/recommendation"
            },
            graph_snapshot_ref="graph://cycle_20260410/portfolio_graph",
            dagster_run_id="dagster-fixture-run-20260410",
            replay_mode=cast(Any, "rerun_model"),
            created_at=datetime(2026, 4, 10, 16, 9, tzinfo=timezone.utc),
        )


def test_five_fields_required_when_llm_called() -> None:
    with pytest.raises(ValidationError):
        AuditRecordDraft(
            record_id="audit-missing-field",
            cycle_id="cycle_20260410",
            layer="L7",
            object_ref="recommendation",
            params_snapshot={},
            llm_lineage={"called": True},
            llm_cost={},
            sanitized_input=None,
            input_hash="sha256:input",
            raw_output="{}",
            parsed_result={},
            output_hash="sha256:output",
            degradation_flags={},
            created_at=datetime(2026, 4, 10, 16, 8, tzinfo=timezone.utc),
        )


def test_missing_manifest_ref_raises_key_error(tmp_path: Path) -> None:
    fixture_copy = tmp_path / "spike"
    shutil.copytree(FIXTURE_ROOT, fixture_copy)
    manifest_path = fixture_copy / "cycle_20260410" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    del manifest["snapshot_refs"]["recommendation"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(KeyError):
        reconstruct_replay_view(
            cycle_id="cycle_20260410",
            object_ref="recommendation",
            fixture_root=fixture_copy,
        )
