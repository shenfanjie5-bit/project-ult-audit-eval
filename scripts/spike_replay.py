"""Offline replay spike bound to fixture manifests."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from audit_eval.contracts.manifest_draft import CyclePublishManifestDraft
from audit_eval.contracts.replay_draft import (
    AuditRecordDraft,
    ReplayRecordDraft,
    ReplayViewDraft,
)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_required_json(path: Path, description: str) -> Any:
    try:
        return _read_json(path)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Missing {description} fixture: {path}") from exc


def load_manifest(path: Path) -> CyclePublishManifestDraft:
    """Load the published manifest snapshot set."""

    return CyclePublishManifestDraft.model_validate(_read_json(path))


def load_audit_records(path: Path) -> list[AuditRecordDraft]:
    """Load draft audit records from a fixture JSON array."""

    records = _read_json(path)
    if not isinstance(records, list):
        raise ValueError(f"Expected audit record array in {path}")
    return [AuditRecordDraft.model_validate(record) for record in records]


def load_replay_record(path: Path, object_ref: str) -> ReplayRecordDraft:
    """Load the replay record matching one object reference."""

    records = _read_json(path)
    if not isinstance(records, list):
        raise ValueError(f"Expected replay record array in {path}")

    matches = [
        ReplayRecordDraft.model_validate(record)
        for record in records
        if record.get("object_ref") == object_ref
    ]
    if len(matches) != 1:
        raise KeyError(f"Expected exactly one replay record for {object_ref}")
    return matches[0]


def _fixture_path(cycle_fixture: Path, relative_ref: str) -> Path:
    relative_path = Path(relative_ref)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError(f"Snapshot ref escapes fixture root: {relative_ref}")
    return cycle_fixture / relative_path


def _snapshot_path(cycle_fixture: Path, snapshot_ref: str) -> Path:
    parsed_ref = urlparse(snapshot_ref)

    if parsed_ref.scheme == "snapshot":
        if parsed_ref.netloc != cycle_fixture.name:
            raise ValueError(
                "Snapshot ref cycle does not match loaded fixture cycle"
            )

        ref_path = parsed_ref.path.lstrip("/")
        if not ref_path:
            raise ValueError(f"Snapshot ref has no fixture path: {snapshot_ref}")

        if ref_path.endswith(".json"):
            relative_ref = ref_path
        elif ref_path.startswith("formal_snapshots/"):
            relative_ref = f"{ref_path}.json"
        else:
            relative_ref = f"formal_snapshots/{ref_path}.json"
        return _fixture_path(cycle_fixture, relative_ref)

    if parsed_ref.scheme:
        raise ValueError(f"Unsupported snapshot ref scheme: {parsed_ref.scheme}")
    if not snapshot_ref.endswith(".json"):
        raise ValueError(
            f"Local snapshot refs must point to JSON fixtures: {snapshot_ref}"
        )
    return _fixture_path(cycle_fixture, snapshot_ref)


def _graph_snapshot_path(cycle_fixture: Path, graph_snapshot_ref: str) -> Path:
    parsed_ref = urlparse(graph_snapshot_ref)

    if parsed_ref.scheme != "graph":
        raise ValueError(
            f"Unsupported graph snapshot ref scheme: {parsed_ref.scheme}"
        )
    if parsed_ref.netloc != cycle_fixture.name:
        raise ValueError(
            "Graph snapshot ref cycle does not match loaded fixture cycle"
        )

    ref_path = parsed_ref.path.lstrip("/")
    if not ref_path:
        raise ValueError(
            f"Graph snapshot ref has no fixture path: {graph_snapshot_ref}"
        )

    if ref_path.endswith(".json"):
        relative_ref = ref_path
    elif ref_path.startswith("graph_snapshots/"):
        relative_ref = f"{ref_path}.json"
    else:
        relative_ref = f"graph_snapshots/{ref_path}.json"
    return _fixture_path(cycle_fixture, relative_ref)


def _dagster_run_summary_path(cycle_fixture: Path, dagster_run_id: str) -> Path:
    run_id_path = Path(dagster_run_id)
    if (
        not dagster_run_id
        or run_id_path.is_absolute()
        or len(run_id_path.parts) != 1
        or ".." in run_id_path.parts
    ):
        raise ValueError(f"Dagster run id escapes fixture root: {dagster_run_id}")
    return _fixture_path(cycle_fixture, f"dagster_runs/{dagster_run_id}.json")


def load_graph_snapshot_summary(
    cycle_fixture: Path,
    graph_snapshot_ref: str | None,
) -> dict[str, Any] | None:
    """Load a graph snapshot summary referenced by the replay record, if present."""

    if graph_snapshot_ref is None:
        return None

    graph_path = _graph_snapshot_path(cycle_fixture, graph_snapshot_ref)
    graph_summary = _read_required_json(
        graph_path,
        f"graph snapshot summary for {graph_snapshot_ref}",
    )
    if not isinstance(graph_summary, dict):
        raise ValueError(f"Expected graph snapshot summary object in {graph_path}")
    if graph_summary.get("graph_snapshot_ref") != graph_snapshot_ref:
        raise ValueError(
            f"Graph snapshot summary {graph_path} is not bound to "
            f"{graph_snapshot_ref}"
        )
    if graph_summary.get("cycle_id") != cycle_fixture.name:
        raise ValueError(
            "Graph snapshot summary cycle_id does not match loaded fixture cycle"
        )
    return graph_summary


def load_dagster_run_summary(
    cycle_fixture: Path,
    dagster_run_id: str,
) -> dict[str, Any]:
    """Load the Dagster run history summary referenced by the replay record."""

    dagster_path = _dagster_run_summary_path(cycle_fixture, dagster_run_id)
    dagster_summary = _read_required_json(
        dagster_path,
        f"Dagster run summary for {dagster_run_id}",
    )
    if not isinstance(dagster_summary, dict):
        raise ValueError(f"Expected Dagster run summary object in {dagster_path}")
    if dagster_summary.get("run_id") != dagster_run_id:
        raise ValueError(
            f"Dagster run summary {dagster_path} is not bound to {dagster_run_id}"
        )
    if dagster_summary.get("cycle_id") != cycle_fixture.name:
        raise ValueError(
            "Dagster run summary cycle_id does not match loaded fixture cycle"
        )
    return dagster_summary


def reconstruct_replay_view(
    cycle_id: str,
    object_ref: str,
    fixture_root: Path,
) -> dict[str, Any]:
    """Rebuild a read-history replay view from offline fixture files."""

    cycle_fixture = fixture_root / cycle_id
    manifest = load_manifest(cycle_fixture / "manifest.json")
    if manifest.published_cycle_id != cycle_id:
        raise ValueError(
            "Manifest published_cycle_id does not match requested cycle_id"
        )

    replay_record = load_replay_record(
        cycle_fixture / "replay_records.json",
        object_ref,
    )
    if replay_record.cycle_id != cycle_id:
        raise ValueError("Replay record cycle_id does not match requested cycle_id")
    if replay_record.manifest_cycle_id != manifest.published_cycle_id:
        raise ValueError("Replay record is not bound to the loaded manifest")

    audit_records = load_audit_records(cycle_fixture / "audit_records.json")
    audit_records_by_id = {record.record_id: record for record in audit_records}
    replay_audit_records = []
    for record_id in replay_record.audit_record_ids:
        try:
            replay_audit_records.append(audit_records_by_id[record_id])
        except KeyError as exc:
            raise KeyError(f"Missing audit_record id {record_id}") from exc

    historical_formal_objects: dict[str, dict[str, Any]] = {}
    for formal_object_ref, replay_snapshot_ref in (
        replay_record.formal_snapshot_refs.items()
    ):
        manifest_snapshot_ref = manifest.snapshot_refs[formal_object_ref]
        if replay_snapshot_ref != manifest_snapshot_ref:
            raise ValueError(
                f"Replay snapshot ref for {formal_object_ref} does not match "
                "manifest"
            )
        snapshot_path = _snapshot_path(cycle_fixture, manifest_snapshot_ref)
        snapshot_data = _read_json(snapshot_path)
        if not isinstance(snapshot_data, dict):
            raise ValueError(f"Expected snapshot object in {snapshot_path}")
        if snapshot_data.get("snapshot_ref") != manifest_snapshot_ref:
            raise ValueError(
                f"Snapshot file {snapshot_path} is not bound to manifest ref "
                f"{manifest_snapshot_ref}"
            )
        historical_formal_objects[formal_object_ref] = {
            "source_ref": manifest_snapshot_ref,
            "data": snapshot_data,
        }

    graph_snapshot_summary = load_graph_snapshot_summary(
        cycle_fixture,
        replay_record.graph_snapshot_ref,
    )
    dagster_run_summary = load_dagster_run_summary(
        cycle_fixture,
        replay_record.dagster_run_id,
    )

    replay_view = ReplayViewDraft(
        cycle_id=cycle_id,
        object_ref=object_ref,
        replay_record=replay_record,
        audit_records=replay_audit_records,
        manifest_snapshot_set=dict(manifest.snapshot_refs),
        historical_formal_objects=historical_formal_objects,
        graph_snapshot_ref=replay_record.graph_snapshot_ref,
        graph_snapshot_summary=graph_snapshot_summary,
        dagster_run_summary=dagster_run_summary,
    )
    return replay_view.model_dump(mode="json")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cycle-id", required=True)
    parser.add_argument("--object-ref", required=True)
    parser.add_argument(
        "--fixtures",
        type=Path,
        required=True,
        help="Root directory containing spike cycle fixtures.",
    )
    args = parser.parse_args(argv)

    replay_view = reconstruct_replay_view(
        cycle_id=args.cycle_id,
        object_ref=args.object_ref,
        fixture_root=args.fixtures,
    )
    json.dump(replay_view, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
