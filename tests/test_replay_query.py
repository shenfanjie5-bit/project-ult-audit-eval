import json
import socket
import time
import urllib.request
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from audit_eval.audit import (
    AuditRecordMissing,
    DagsterSummaryMissing,
    GraphSnapshotMissing,
    ManifestBindingError,
    ReplayModeError,
    ReplayQueryContext,
    ReplayQueryError,
    ReplayRecordNotFound,
    ReplayView,
    SnapshotLoadError,
    replay_cycle_object,
)
from audit_eval.contracts import (
    AuditRecord,
    CyclePublishManifestDraft,
    ReplayRecord,
)


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "spike" / "cycle_20260410"
_DEFAULT = object()


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_network_call(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Replay query must not call network")

    monkeypatch.setattr(urllib.request, "urlopen", fail_network_call)
    monkeypatch.setattr(socket, "socket", fail_network_call)


class FakeReplayRepository:
    def __init__(
        self,
        replay_records: dict[tuple[str, str], ReplayRecord],
        audit_records: dict[str, AuditRecord],
        calls: list[tuple[str, str]],
    ) -> None:
        self.replay_records = replay_records
        self.audit_records = audit_records
        self.calls = calls

    def get_replay_record(
        self,
        cycle_id: str,
        object_ref: str,
    ) -> ReplayRecord | None:
        self.calls.append(("replay_record", f"{cycle_id}:{object_ref}"))
        return self.replay_records.get((cycle_id, object_ref))

    def get_audit_records(self, record_ids: Sequence[str]) -> list[AuditRecord]:
        self.calls.append(("audit_records", ",".join(record_ids)))
        return [
            self.audit_records[record_id]
            for record_id in record_ids
            if record_id in self.audit_records
        ]


class FakeManifestGateway:
    def __init__(
        self,
        manifest: CyclePublishManifestDraft,
        calls: list[tuple[str, str]],
    ) -> None:
        self.manifest = manifest
        self.calls = calls

    def load(self, cycle_id: str) -> CyclePublishManifestDraft:
        self.calls.append(("manifest", cycle_id))
        return self.manifest


class FakeFormalSnapshotGateway:
    def __init__(
        self,
        snapshots: dict[str, dict[str, Any] | None],
        calls: list[tuple[str, str]],
    ) -> None:
        self.snapshots = snapshots
        self.calls = calls

    def load_snapshot(self, snapshot_ref: str) -> dict[str, Any]:
        self.calls.append(("formal", snapshot_ref))
        try:
            snapshot = self.snapshots[snapshot_ref]
        except KeyError as exc:
            raise FileNotFoundError(snapshot_ref) from exc
        return snapshot  # type: ignore[return-value]


class FakeGraphSnapshotGateway:
    def __init__(
        self,
        graph_snapshot: dict[str, Any] | None,
        calls: list[tuple[str, str]],
    ) -> None:
        self.graph_snapshot = graph_snapshot
        self.calls = calls

    def load(self, graph_snapshot_ref: str) -> dict[str, Any]:
        self.calls.append(("graph", graph_snapshot_ref))
        return self.graph_snapshot  # type: ignore[return-value]


class FakeDagsterRunGateway:
    def __init__(
        self,
        dagster_summary: dict[str, Any] | None,
        calls: list[tuple[str, str]],
    ) -> None:
        self.dagster_summary = dagster_summary
        self.calls = calls

    def load_summary(self, dagster_run_id: str) -> dict[str, Any]:
        self.calls.append(("dagster", dagster_run_id))
        return self.dagster_summary  # type: ignore[return-value]


def _json_fixture(relative_path: str) -> Any:
    return json.loads((FIXTURE_ROOT / relative_path).read_text(encoding="utf-8"))


def _audit_records() -> list[AuditRecord]:
    return [
        AuditRecord.model_validate(payload)
        for payload in _json_fixture("audit_records.json")
    ]


def _replay_record() -> ReplayRecord:
    payloads = _json_fixture("replay_records.json")
    return ReplayRecord.model_validate(payloads[1])


def _manifest() -> CyclePublishManifestDraft:
    return CyclePublishManifestDraft.model_validate(_json_fixture("manifest.json"))


def _snapshots() -> dict[str, dict[str, Any]]:
    snapshots: dict[str, dict[str, Any]] = {}
    for snapshot_path in (FIXTURE_ROOT / "formal_snapshots").glob("*.json"):
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshots[snapshot["snapshot_ref"]] = snapshot
    return snapshots


def _graph_snapshot() -> dict[str, Any]:
    return _json_fixture("graph_snapshots/portfolio_graph.json")


def _dagster_summary() -> dict[str, Any]:
    return _json_fixture("dagster_runs/dagster-fixture-run-20260410.json")


def _make_context(
    *,
    replay_record: ReplayRecord | None | object = _DEFAULT,
    audit_records: list[AuditRecord] | object = _DEFAULT,
    manifest: CyclePublishManifestDraft | object = _DEFAULT,
    snapshots: dict[str, dict[str, Any] | None] | object = _DEFAULT,
    graph_snapshot: dict[str, Any] | None | object = _DEFAULT,
    dagster_summary: dict[str, Any] | None | object = _DEFAULT,
    graph_gateway: FakeGraphSnapshotGateway | None | object = _DEFAULT,
    dagster_gateway: FakeDagsterRunGateway | None | object = _DEFAULT,
) -> tuple[ReplayQueryContext, list[tuple[str, str]]]:
    calls: list[tuple[str, str]] = []

    selected_replay = _replay_record() if replay_record is _DEFAULT else replay_record
    replay_records = {}
    if isinstance(selected_replay, ReplayRecord):
        replay_records[(selected_replay.cycle_id, selected_replay.object_ref)] = (
            selected_replay
        )

    selected_audit_records = (
        _audit_records() if audit_records is _DEFAULT else audit_records
    )
    audit_records_by_id = {
        record.record_id: record for record in selected_audit_records
    }

    selected_manifest = _manifest() if manifest is _DEFAULT else manifest
    selected_snapshots = _snapshots() if snapshots is _DEFAULT else snapshots

    if graph_gateway is _DEFAULT:
        selected_graph = (
            _graph_snapshot() if graph_snapshot is _DEFAULT else graph_snapshot
        )
        graph_gateway = FakeGraphSnapshotGateway(selected_graph, calls)
    if dagster_gateway is _DEFAULT:
        selected_dagster = (
            _dagster_summary()
            if dagster_summary is _DEFAULT
            else dagster_summary
        )
        dagster_gateway = FakeDagsterRunGateway(selected_dagster, calls)

    context = ReplayQueryContext(
        repository=FakeReplayRepository(replay_records, audit_records_by_id, calls),
        manifest_gateway=FakeManifestGateway(selected_manifest, calls),
        formal_gateway=FakeFormalSnapshotGateway(selected_snapshots, calls),
        graph_gateway=graph_gateway,
        dagster_gateway=dagster_gateway,
    )
    return context, calls


def test_replay_cycle_object_returns_manifest_bound_replay_view() -> None:
    context, calls = _make_context()

    started_at = time.perf_counter()
    replay_view = replay_cycle_object(
        "cycle_20260410",
        "recommendation",
        context=context,
    )
    elapsed_seconds = time.perf_counter() - started_at

    assert elapsed_seconds < 5
    assert isinstance(replay_view, ReplayView)
    assert replay_view.cycle_id == "cycle_20260410"
    assert replay_view.object_ref == "recommendation"
    assert len(replay_view.audit_records) == 2
    assert replay_view.graph_snapshot_ref == "graph://cycle_20260410/portfolio_graph"
    assert replay_view.graph_snapshot == _graph_snapshot()
    assert replay_view.dagster_run_summary == _dagster_summary()
    assert replay_view.manifest_snapshot_set == _manifest().snapshot_refs

    historical_objects = replay_view.historical_formal_objects
    assert set(historical_objects) == {"world_state", "recommendation"}
    for formal_object_ref, historical_object in historical_objects.items():
        manifest_ref = replay_view.manifest_snapshot_set[formal_object_ref]
        assert historical_object["source_ref"] == manifest_ref
        assert historical_object["data"]["snapshot_ref"] == manifest_ref
        assert historical_object["data"]["source_ref"] == manifest_ref
        assert historical_object["data"]["object_ref"] == formal_object_ref

    replay_payload = replay_view.to_dict()
    json.dumps(replay_payload)
    assert replay_payload["graph_snapshot"] == _graph_snapshot()
    assert replay_payload["graph_snapshot_summary"] == _graph_snapshot()
    assert replay_payload["replay_record"]["created_at"] == "2026-04-10T16:09:00Z"

    assert [call[0] for call in calls] == [
        "replay_record",
        "manifest",
        "audit_records",
        "formal",
        "formal",
        "graph",
        "dagster",
    ]


def test_replay_cycle_object_requires_explicit_context() -> None:
    with pytest.raises(ReplayQueryError, match="context"):
        replay_cycle_object("cycle_20260410", "recommendation")


def test_missing_replay_record_raises_typed_error() -> None:
    context, _calls = _make_context(replay_record=None)

    with pytest.raises(ReplayRecordNotFound):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_non_read_history_replay_mode_raises_typed_error() -> None:
    replay_record = _replay_record().model_copy(
        update={"replay_mode": "rerun_model"}
    )
    context, _calls = _make_context(replay_record=replay_record)

    with pytest.raises(ReplayModeError):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_missing_referenced_audit_record_raises_typed_error() -> None:
    context, _calls = _make_context(audit_records=_audit_records()[:1])

    with pytest.raises(AuditRecordMissing, match="L7-recommendation"):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_manifest_missing_requested_object_ref_raises_typed_error() -> None:
    snapshot_refs = {"world_state": _manifest().snapshot_refs["world_state"]}
    manifest = _manifest().model_copy(
        update={"snapshot_refs": snapshot_refs}
    )
    context, _calls = _make_context(manifest=manifest)

    with pytest.raises(ManifestBindingError, match="recommendation"):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_manifest_cycle_mismatch_raises_typed_error() -> None:
    replay_record = _replay_record().model_copy(
        update={"manifest_cycle_id": "cycle_other"}
    )
    context, _calls = _make_context(replay_record=replay_record)

    with pytest.raises(ManifestBindingError, match="manifest_cycle_id"):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_replay_snapshot_ref_mismatch_raises_manifest_binding_error() -> None:
    replay_record = _replay_record()
    formal_snapshot_refs = dict(replay_record.formal_snapshot_refs)
    formal_snapshot_refs["recommendation"] = "snapshot://cycle_20260410/other"
    replay_record = replay_record.model_copy(
        update={"formal_snapshot_refs": formal_snapshot_refs}
    )
    context, _calls = _make_context(replay_record=replay_record)

    with pytest.raises(ManifestBindingError, match="recommendation"):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_missing_manifest_bound_snapshot_raises_typed_error() -> None:
    snapshots = _snapshots()
    del snapshots["snapshot://cycle_20260410/recommendation"]
    context, _calls = _make_context(snapshots=snapshots)

    with pytest.raises(SnapshotLoadError, match="recommendation"):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_snapshot_ref_mismatch_raises_typed_error() -> None:
    snapshots = _snapshots()
    bad_snapshot = dict(snapshots["snapshot://cycle_20260410/recommendation"])
    bad_snapshot["snapshot_ref"] = "snapshot://cycle_20260410/head"
    snapshots["snapshot://cycle_20260410/recommendation"] = bad_snapshot
    context, _calls = _make_context(snapshots=snapshots)

    with pytest.raises(SnapshotLoadError, match="manifest ref"):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_unbound_snapshot_missing_metadata_raises_typed_error() -> None:
    snapshots = _snapshots()
    snapshots["snapshot://cycle_20260410/recommendation"] = {
        "recommendation": {"action": "head_object_without_binding"},
    }
    context, _calls = _make_context(snapshots=snapshots)

    with pytest.raises(SnapshotLoadError, match="snapshot_ref"):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_source_ref_mismatch_raises_typed_error() -> None:
    snapshots = _snapshots()
    bad_snapshot = dict(snapshots["snapshot://cycle_20260410/recommendation"])
    bad_snapshot["source_ref"] = "snapshot://cycle_20260410/head"
    snapshots["snapshot://cycle_20260410/recommendation"] = bad_snapshot
    context, _calls = _make_context(snapshots=snapshots)

    with pytest.raises(SnapshotLoadError, match="source_ref"):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_snapshot_object_ref_mismatch_raises_typed_error() -> None:
    snapshots = _snapshots()
    bad_snapshot = dict(snapshots["snapshot://cycle_20260410/recommendation"])
    bad_snapshot["object_ref"] = "formal_head_recommendation"
    snapshots["snapshot://cycle_20260410/recommendation"] = bad_snapshot
    context, _calls = _make_context(snapshots=snapshots)

    with pytest.raises(SnapshotLoadError, match="object_ref"):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_missing_graph_snapshot_raises_typed_error() -> None:
    context, _calls = _make_context(graph_snapshot=None)

    with pytest.raises(GraphSnapshotMissing):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_graph_snapshot_ref_without_gateway_raises_typed_error() -> None:
    context, _calls = _make_context(graph_gateway=None)

    with pytest.raises(GraphSnapshotMissing, match="gateway"):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_missing_dagster_run_summary_raises_typed_error() -> None:
    context, _calls = _make_context(dagster_summary=None)

    with pytest.raises(DagsterSummaryMissing):
        replay_cycle_object("cycle_20260410", "recommendation", context=context)


def test_audit_package_has_no_model_or_http_client_dependencies() -> None:
    audit_package = Path(__file__).resolve().parents[1] / "src" / "audit_eval" / "audit"
    forbidden_imports = (
        "import anthropic",
        "from anthropic",
        "import httpx",
        "from httpx",
        "import openai",
        "from openai",
        "import requests",
        "from requests",
    )

    for module_path in audit_package.glob("*.py"):
        module_text = module_path.read_text(encoding="utf-8")
        for forbidden_import in forbidden_imports:
            assert forbidden_import not in module_text
