"""Read-history replay query API."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from audit_eval.audit.errors import (
    AuditRecordMissing,
    DagsterSummaryMissing,
    GraphSnapshotMissing,
    ManifestBindingError,
    ReplayModeError,
    ReplayQueryError,
    ReplayRecordNotFound,
    SnapshotLoadError,
)
from audit_eval.audit.manifest_gateway import (
    FormalSnapshotGateway,
    ManifestGateway,
)
from audit_eval.audit.replay_view import ReplayView
from audit_eval.contracts.audit_record import AuditRecord
from audit_eval.contracts.manifest_draft import CyclePublishManifestDraft
from audit_eval.contracts.replay_record import ReplayRecord


class ReplayRepository(Protocol):
    """Repository boundary for formal replay/audit rows."""

    def get_replay_record(
        self,
        cycle_id: str,
        object_ref: str,
    ) -> ReplayRecord | None:
        """Return one replay_record row for the requested cycle/object."""

    def get_audit_records(self, record_ids: Sequence[str]) -> list[AuditRecord]:
        """Return audit_record rows for ``record_ids``."""


class DagsterRunGateway(Protocol):
    """Gateway for Dagster run history summaries."""

    def load_summary(self, dagster_run_id: str) -> dict[str, Any]:
        """Return the Dagster run summary for ``dagster_run_id``."""


class GraphSnapshotGateway(Protocol):
    """Gateway for graph-engine snapshot summaries."""

    def load(self, graph_snapshot_ref: str) -> dict[str, Any]:
        """Return the graph snapshot summary for ``graph_snapshot_ref``."""


@dataclass(frozen=True)
class ReplayQueryContext:
    """Injected dependencies used by ``replay_cycle_object``."""

    repository: ReplayRepository
    manifest_gateway: ManifestGateway
    formal_gateway: FormalSnapshotGateway
    dagster_gateway: DagsterRunGateway | None
    graph_gateway: GraphSnapshotGateway | None


def replay_cycle_object(
    cycle_id: str,
    object_ref: str,
    context: ReplayQueryContext | None = None,
) -> ReplayView:
    """Rebuild a ReplayView from historical persisted records only."""

    if context is None:
        raise ReplayQueryError(
            "No default replay query context is configured; pass context=..."
        )

    replay_record = _load_replay_record(context.repository, cycle_id, object_ref)
    if replay_record.replay_mode != "read_history":
        raise ReplayModeError(
            "ReplayRecord.replay_mode must be read_history for replay queries"
        )
    if replay_record.cycle_id != cycle_id or replay_record.object_ref != object_ref:
        raise ReplayRecordNotFound(
            "ReplayRepository returned a replay_record for a different "
            "cycle_id/object_ref"
        )

    manifest = _load_manifest(context.manifest_gateway, cycle_id)
    _validate_manifest_binding(replay_record, manifest, object_ref)

    audit_records = _load_audit_records(context.repository, replay_record)
    historical_formal_objects = _load_historical_formal_objects(
        context.formal_gateway,
        replay_record,
        manifest,
    )
    graph_snapshot = _load_graph_snapshot(context.graph_gateway, replay_record)
    dagster_run_summary = _load_dagster_summary(
        context.dagster_gateway,
        replay_record,
    )

    return ReplayView(
        cycle_id=cycle_id,
        object_ref=object_ref,
        replay_record=replay_record,
        audit_records=tuple(audit_records),
        manifest_snapshot_set=dict(manifest.snapshot_refs),
        historical_formal_objects=historical_formal_objects,
        graph_snapshot_ref=replay_record.graph_snapshot_ref,
        graph_snapshot=graph_snapshot,
        dagster_run_summary=dagster_run_summary,
    )


def _load_replay_record(
    repository: ReplayRepository,
    cycle_id: str,
    object_ref: str,
) -> ReplayRecord:
    try:
        replay_record = repository.get_replay_record(cycle_id, object_ref)
    except Exception as exc:
        raise ReplayQueryError(
            "Failed to load replay_record for "
            f"cycle_id={cycle_id!r}, object_ref={object_ref!r}"
        ) from exc

    if replay_record is None:
        raise ReplayRecordNotFound(
            "No replay_record found for "
            f"cycle_id={cycle_id!r}, object_ref={object_ref!r}"
        )
    return replay_record


def _load_manifest(
    manifest_gateway: ManifestGateway,
    cycle_id: str,
) -> CyclePublishManifestDraft:
    try:
        manifest = manifest_gateway.load(cycle_id)
    except Exception as exc:
        raise ManifestBindingError(
            f"Failed to load cycle_publish_manifest for cycle_id={cycle_id!r}"
        ) from exc

    if manifest.published_cycle_id != cycle_id:
        raise ManifestBindingError(
            "cycle_publish_manifest.published_cycle_id does not match "
            f"requested cycle_id={cycle_id!r}"
        )
    return manifest


def _validate_manifest_binding(
    replay_record: ReplayRecord,
    manifest: CyclePublishManifestDraft,
    object_ref: str,
) -> None:
    if replay_record.manifest_cycle_id != manifest.published_cycle_id:
        raise ManifestBindingError(
            "ReplayRecord.manifest_cycle_id does not match "
            "cycle_publish_manifest.published_cycle_id"
        )

    if object_ref not in replay_record.formal_snapshot_refs:
        raise ManifestBindingError(
            f"ReplayRecord.formal_snapshot_refs is missing {object_ref!r}"
        )
    if object_ref not in manifest.snapshot_refs:
        raise ManifestBindingError(
            f"cycle_publish_manifest.snapshot_refs is missing {object_ref!r}"
        )

    replay_object_ref = replay_record.formal_snapshot_refs[object_ref]
    manifest_object_ref = manifest.snapshot_refs[object_ref]
    if replay_object_ref != manifest_object_ref:
        raise ManifestBindingError(
            f"ReplayRecord.formal_snapshot_refs[{object_ref!r}] does not match "
            "cycle_publish_manifest.snapshot_refs"
        )

    for formal_object_ref, replay_snapshot_ref in (
        replay_record.formal_snapshot_refs.items()
    ):
        manifest_snapshot_ref = manifest.snapshot_refs.get(formal_object_ref)
        if manifest_snapshot_ref is None:
            raise ManifestBindingError(
                "ReplayRecord.formal_snapshot_refs contains object_ref missing "
                f"from manifest: {formal_object_ref!r}"
            )
        if not isinstance(replay_snapshot_ref, str) or not isinstance(
            manifest_snapshot_ref,
            str,
        ):
            raise ManifestBindingError(
                f"Snapshot refs must be strings for object_ref={formal_object_ref!r}"
            )
        if replay_snapshot_ref != manifest_snapshot_ref:
            raise ManifestBindingError(
                f"Replay snapshot ref for {formal_object_ref!r} does not match "
                "manifest snapshot ref"
            )


def _load_audit_records(
    repository: ReplayRepository,
    replay_record: ReplayRecord,
) -> list[AuditRecord]:
    try:
        audit_records = repository.get_audit_records(replay_record.audit_record_ids)
    except Exception as exc:
        raise ReplayQueryError("Failed to load audit_record rows") from exc

    records_by_id = {record.record_id: record for record in audit_records}
    missing_record_ids = [
        record_id
        for record_id in replay_record.audit_record_ids
        if record_id not in records_by_id
    ]
    if missing_record_ids:
        raise AuditRecordMissing(
            "ReplayRecord.audit_record_ids reference missing audit_record ids: "
            + ", ".join(missing_record_ids)
        )

    ordered_records = [
        records_by_id[record_id]
        for record_id in replay_record.audit_record_ids
    ]
    wrong_cycle_ids = [
        record.record_id
        for record in ordered_records
        if record.cycle_id != replay_record.cycle_id
    ]
    if wrong_cycle_ids:
        raise AuditRecordMissing(
            "ReplayRecord.audit_record_ids reference audit_records from a "
            "different cycle_id: "
            + ", ".join(wrong_cycle_ids)
        )
    return ordered_records


def _load_historical_formal_objects(
    formal_gateway: FormalSnapshotGateway,
    replay_record: ReplayRecord,
    manifest: CyclePublishManifestDraft,
) -> dict[str, Any]:
    historical_formal_objects: dict[str, Any] = {}
    for formal_object_ref in replay_record.formal_snapshot_refs:
        snapshot_ref = manifest.snapshot_refs[formal_object_ref]
        snapshot_data = _load_snapshot(
            formal_gateway,
            formal_object_ref,
            snapshot_ref,
        )
        historical_formal_objects[formal_object_ref] = {
            "source_ref": snapshot_ref,
            "data": snapshot_data,
        }
    return historical_formal_objects


def _load_snapshot(
    formal_gateway: FormalSnapshotGateway,
    formal_object_ref: str,
    snapshot_ref: str,
) -> dict[str, Any]:
    try:
        snapshot_data = formal_gateway.load_snapshot(snapshot_ref)
    except SnapshotLoadError:
        raise
    except Exception as exc:
        raise SnapshotLoadError(
            "Failed to load manifest-bound formal snapshot "
            f"{snapshot_ref!r} for object_ref={formal_object_ref!r}"
        ) from exc

    if not isinstance(snapshot_data, dict) or not snapshot_data:
        raise SnapshotLoadError(
            "Manifest-bound formal snapshot is missing or not an object: "
            f"{snapshot_ref!r}"
        )
    if "snapshot_ref" not in snapshot_data:
        raise SnapshotLoadError(
            f"Formal snapshot {snapshot_ref!r} is missing snapshot_ref "
            "binding metadata"
        )
    if snapshot_data["snapshot_ref"] != snapshot_ref:
        raise SnapshotLoadError(
            f"Formal snapshot {snapshot_ref!r} is not bound to manifest ref"
        )
    if "source_ref" not in snapshot_data:
        raise SnapshotLoadError(
            f"Formal snapshot {snapshot_ref!r} is missing source_ref "
            "binding metadata"
        )
    if snapshot_data["source_ref"] != snapshot_ref:
        raise SnapshotLoadError(
            f"Formal snapshot {snapshot_ref!r} source_ref is not manifest-bound"
        )
    if "object_ref" not in snapshot_data:
        raise SnapshotLoadError(
            f"Formal snapshot {snapshot_ref!r} is missing object_ref "
            "binding metadata"
        )
    if snapshot_data["object_ref"] != formal_object_ref:
        raise SnapshotLoadError(
            f"Formal snapshot {snapshot_ref!r} object_ref does not match "
            f"{formal_object_ref!r}"
        )
    return snapshot_data


def _load_graph_snapshot(
    graph_gateway: GraphSnapshotGateway | None,
    replay_record: ReplayRecord,
) -> dict[str, Any] | None:
    graph_snapshot_ref = replay_record.graph_snapshot_ref
    if graph_snapshot_ref is None:
        return None
    if graph_gateway is None:
        raise GraphSnapshotMissing(
            f"Graph snapshot gateway is not configured for {graph_snapshot_ref!r}"
        )

    try:
        graph_snapshot = graph_gateway.load(graph_snapshot_ref)
    except GraphSnapshotMissing:
        raise
    except Exception as exc:
        raise GraphSnapshotMissing(
            f"Failed to load graph snapshot {graph_snapshot_ref!r}"
        ) from exc

    if not isinstance(graph_snapshot, dict) or not graph_snapshot:
        raise GraphSnapshotMissing(
            f"Graph snapshot {graph_snapshot_ref!r} is missing"
        )
    if "graph_snapshot_ref" not in graph_snapshot:
        raise GraphSnapshotMissing(
            f"Graph snapshot {graph_snapshot_ref!r} is missing "
            "graph_snapshot_ref binding metadata"
        )
    if graph_snapshot["graph_snapshot_ref"] != graph_snapshot_ref:
        raise GraphSnapshotMissing(
            f"Graph snapshot is not bound to {graph_snapshot_ref!r}"
        )
    return graph_snapshot


def _load_dagster_summary(
    dagster_gateway: DagsterRunGateway | None,
    replay_record: ReplayRecord,
) -> dict[str, Any]:
    dagster_run_id = replay_record.dagster_run_id
    if dagster_gateway is None:
        raise DagsterSummaryMissing(
            f"Dagster run gateway is not configured for {dagster_run_id!r}"
        )

    try:
        dagster_summary = dagster_gateway.load_summary(dagster_run_id)
    except DagsterSummaryMissing:
        raise
    except Exception as exc:
        raise DagsterSummaryMissing(
            f"Failed to load Dagster run summary {dagster_run_id!r}"
        ) from exc

    if not isinstance(dagster_summary, dict) or not dagster_summary:
        raise DagsterSummaryMissing(
            f"Dagster run summary {dagster_run_id!r} is missing"
        )
    if "run_id" not in dagster_summary:
        raise DagsterSummaryMissing(
            f"Dagster run summary {dagster_run_id!r} is missing run_id "
            "binding metadata"
        )
    if dagster_summary["run_id"] != dagster_run_id:
        raise DagsterSummaryMissing(
            f"Dagster run summary is not bound to {dagster_run_id!r}"
        )
    return dagster_summary


__all__ = [
    "DagsterRunGateway",
    "GraphSnapshotGateway",
    "ReplayQueryContext",
    "ReplayRepository",
    "replay_cycle_object",
]
