"""ReplayView returned by read-history replay queries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel

from audit_eval.contracts.audit_record import AuditRecord
from audit_eval.contracts.replay_record import ReplayRecord


@dataclass(frozen=True)
class ReplayView:
    """Reconstructed historical view for one cycle/object pair."""

    cycle_id: str
    object_ref: str
    replay_record: ReplayRecord
    audit_records: tuple[AuditRecord, ...]
    manifest_snapshot_set: dict[str, str]
    historical_formal_objects: dict[str, Any]
    graph_snapshot_ref: str | None
    graph_snapshot: dict[str, Any] | None
    dagster_run_summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable replay view payload."""

        graph_snapshot = _json_ready(self.graph_snapshot)
        return {
            "cycle_id": self.cycle_id,
            "object_ref": self.object_ref,
            "replay_record": _json_ready(self.replay_record),
            "audit_records": _json_ready(self.audit_records),
            "manifest_snapshot_set": _json_ready(self.manifest_snapshot_set),
            "historical_formal_objects": _json_ready(
                self.historical_formal_objects
            ),
            "graph_snapshot_ref": self.graph_snapshot_ref,
            "graph_snapshot": graph_snapshot,
            "graph_snapshot_summary": graph_snapshot,
            "dagster_run_summary": _json_ready(self.dagster_run_summary),
        }


def _json_ready(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return [_json_ready(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return value


__all__ = ["ReplayView"]
