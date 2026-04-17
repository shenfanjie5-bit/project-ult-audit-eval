"""Offline replay reconstruction helpers for fixture-backed spikes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from audit_eval.contracts.manifest_draft import CyclePublishManifestDraft
from audit_eval.contracts.replay_draft import (
    AuditRecordDraft,
    ReplayRecordDraft,
    ReplayViewDraft,
)


class ReplayError(Exception):
    """Base class for replay reconstruction failures."""


class ReplayRecordNotFound(ReplayError):
    """Raised when a replay record cannot be uniquely selected."""


class ManifestBindingError(ReplayError):
    """Raised when replay data is not bound to the loaded manifest."""


class AuditRecordMissing(ReplayError):
    """Raised when a replay record references an absent audit record."""


class SnapshotLoadError(ReplayError):
    """Raised when a referenced historical summary or snapshot cannot load."""


def _read_json(path: Path, description: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SnapshotLoadError(f"Missing {description}: {path}") from exc


def load_manifest(path: Path) -> CyclePublishManifestDraft:
    """Load the published manifest snapshot set."""

    return CyclePublishManifestDraft.model_validate(
        _read_json(path, "cycle publish manifest")
    )


def load_audit_records(path: Path) -> list[AuditRecordDraft]:
    """Load draft audit records from a fixture JSON array."""

    records = _read_json(path, "audit record fixture")
    if not isinstance(records, list):
        raise SnapshotLoadError(f"Expected audit record array in {path}")
    return [AuditRecordDraft.model_validate(record) for record in records]


def load_replay_record(path: Path, object_ref: str) -> ReplayRecordDraft:
    """Load the replay record matching one object reference."""

    records = _read_json(path, "replay record fixture")
    if not isinstance(records, list):
        raise SnapshotLoadError(f"Expected replay record array in {path}")

    matches = [
        ReplayRecordDraft.model_validate(record)
        for record in records
        if record.get("object_ref") == object_ref
    ]
    if len(matches) != 1:
        raise ReplayRecordNotFound(
            f"Expected exactly one replay record for {object_ref}; "
            f"found {len(matches)}"
        )
    return matches[0]


def _fixture_path(cycle_fixture: Path, relative_ref: str) -> Path:
    relative_path = Path(relative_ref)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise SnapshotLoadError(f"Fixture ref escapes fixture root: {relative_ref}")
    return cycle_fixture / relative_path


def _scoped_ref_path(
    cycle_fixture: Path,
    scoped_ref: str,
    scheme: str,
    default_dir: str,
) -> Path:
    parsed_ref = urlparse(scoped_ref)

    if parsed_ref.scheme == scheme:
        if parsed_ref.netloc != cycle_fixture.name:
            raise ManifestBindingError(
                f"{scheme} ref cycle does not match loaded fixture cycle"
            )

        ref_path = parsed_ref.path.lstrip("/")
        if not ref_path:
            raise SnapshotLoadError(f"{scheme} ref has no fixture path: {scoped_ref}")

        if ref_path.endswith(".json"):
            relative_ref = ref_path
        elif ref_path.startswith(f"{default_dir}/"):
            relative_ref = f"{ref_path}.json"
        else:
            relative_ref = f"{default_dir}/{ref_path}.json"
        return _fixture_path(cycle_fixture, relative_ref)

    if parsed_ref.scheme:
        raise SnapshotLoadError(f"Unsupported {scheme} ref scheme: {parsed_ref.scheme}")
    if not scoped_ref.endswith(".json"):
        raise SnapshotLoadError(
            f"Local {scheme} refs must point to JSON fixtures: {scoped_ref}"
        )
    return _fixture_path(cycle_fixture, scoped_ref)


def _snapshot_path(cycle_fixture: Path, snapshot_ref: str) -> Path:
    return _scoped_ref_path(
        cycle_fixture=cycle_fixture,
        scoped_ref=snapshot_ref,
        scheme="snapshot",
        default_dir="formal_snapshots",
    )


def _graph_snapshot_path(cycle_fixture: Path, graph_snapshot_ref: str) -> Path:
    return _scoped_ref_path(
        cycle_fixture=cycle_fixture,
        scoped_ref=graph_snapshot_ref,
        scheme="graph",
        default_dir="graph_snapshots",
    )


def load_graph_snapshot_summary(
    cycle_fixture: Path,
    graph_snapshot_ref: str | None,
) -> dict[str, Any] | None:
    """Load graph snapshot summary data when a replay record references it."""

    if graph_snapshot_ref is None:
        return None

    graph_path = _graph_snapshot_path(cycle_fixture, graph_snapshot_ref)
    graph_summary = _read_json(
        graph_path,
        f"graph snapshot summary {graph_snapshot_ref}",
    )
    if not isinstance(graph_summary, dict):
        raise SnapshotLoadError(f"Expected graph snapshot object in {graph_path}")
    if graph_summary.get("graph_snapshot_ref") != graph_snapshot_ref:
        raise SnapshotLoadError(
            f"Graph snapshot file {graph_path} is not bound to ref "
            f"{graph_snapshot_ref}"
        )
    return graph_summary


def load_dagster_run_summary(
    cycle_fixture: Path,
    dagster_run_id: str,
) -> dict[str, Any]:
    """Load the Dagster run history summary referenced by a replay record."""

    run_path = _fixture_path(cycle_fixture, f"dagster_runs/{dagster_run_id}.json")
    run_summary = _read_json(run_path, f"Dagster run summary {dagster_run_id}")
    if not isinstance(run_summary, dict):
        raise SnapshotLoadError(f"Expected Dagster run summary object in {run_path}")
    if run_summary.get("dagster_run_id") != dagster_run_id:
        raise SnapshotLoadError(
            f"Dagster run summary {run_path} is not bound to run "
            f"{dagster_run_id}"
        )
    return run_summary


def reconstruct_replay_view(
    cycle_id: str,
    object_ref: str,
    fixture_root: Path,
) -> dict[str, Any]:
    """Rebuild a read-history replay view from offline fixture files."""

    cycle_fixture = fixture_root / cycle_id
    manifest = load_manifest(cycle_fixture / "manifest.json")
    if manifest.published_cycle_id != cycle_id:
        raise ManifestBindingError(
            "Manifest published_cycle_id does not match requested cycle_id"
        )

    replay_record = load_replay_record(
        cycle_fixture / "replay_records.json",
        object_ref,
    )
    if replay_record.cycle_id != cycle_id:
        raise ManifestBindingError(
            "Replay record cycle_id does not match requested cycle_id"
        )
    if replay_record.manifest_cycle_id != manifest.published_cycle_id:
        raise ManifestBindingError("Replay record is not bound to the loaded manifest")

    audit_records = load_audit_records(cycle_fixture / "audit_records.json")
    audit_records_by_id = {record.record_id: record for record in audit_records}
    replay_audit_records = []
    for record_id in replay_record.audit_record_ids:
        try:
            replay_audit_records.append(audit_records_by_id[record_id])
        except KeyError as exc:
            raise AuditRecordMissing(f"Missing audit_record id {record_id}") from exc

    historical_formal_objects: dict[str, dict[str, Any]] = {}
    for formal_object_ref, replay_snapshot_ref in (
        replay_record.formal_snapshot_refs.items()
    ):
        try:
            manifest_snapshot_ref = manifest.snapshot_refs[formal_object_ref]
        except KeyError as exc:
            raise ManifestBindingError(
                f"Manifest missing snapshot ref for {formal_object_ref}"
            ) from exc
        if replay_snapshot_ref != manifest_snapshot_ref:
            raise ManifestBindingError(
                f"Replay snapshot ref for {formal_object_ref} does not match "
                "manifest"
            )
        snapshot_path = _snapshot_path(cycle_fixture, manifest_snapshot_ref)
        snapshot_data = _read_json(
            snapshot_path,
            f"formal snapshot {manifest_snapshot_ref}",
        )
        if not isinstance(snapshot_data, dict):
            raise SnapshotLoadError(f"Expected snapshot object in {snapshot_path}")
        if snapshot_data.get("snapshot_ref") != manifest_snapshot_ref:
            raise ManifestBindingError(
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

    replay_view = ReplayViewDraft.model_validate(
        {
            "cycle_id": cycle_id,
            "object_ref": object_ref,
            "replay_record": replay_record,
            "audit_records": replay_audit_records,
            "manifest_snapshot_set": dict(manifest.snapshot_refs),
            "historical_formal_objects": historical_formal_objects,
            "graph_snapshot_ref": replay_record.graph_snapshot_ref,
            "graph_snapshot_summary": graph_snapshot_summary,
            "dagster_run_summary": dagster_run_summary,
        }
    )
    return replay_view.model_dump(mode="json")


__all__ = [
    "AuditRecordMissing",
    "ManifestBindingError",
    "ReplayError",
    "ReplayRecordNotFound",
    "SnapshotLoadError",
    "load_audit_records",
    "load_dagster_run_summary",
    "load_graph_snapshot_summary",
    "load_manifest",
    "load_replay_record",
    "reconstruct_replay_view",
]
