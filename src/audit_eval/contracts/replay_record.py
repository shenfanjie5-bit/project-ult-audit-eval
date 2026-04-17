"""Formal replay record runtime contract."""

from datetime import datetime
from typing import Self

from pydantic import BaseModel, ConfigDict, model_validator

from audit_eval.contracts.common import JsonObject, ReplayMode


class ReplayRecord(BaseModel):
    """Formal Zone replay record shape for read-history reconstruction."""

    model_config = ConfigDict(extra="forbid")

    replay_id: str
    cycle_id: str
    object_ref: str
    audit_record_ids: list[str]
    manifest_cycle_id: str
    formal_snapshot_refs: JsonObject
    graph_snapshot_ref: str | None
    dagster_run_id: str
    replay_mode: ReplayMode = "read_history"
    created_at: datetime

    @model_validator(mode="after")
    def require_replay_bound_fields(self) -> Self:
        """Replay records must be bound to manifest and run-history inputs."""

        if not self.audit_record_ids:
            raise ValueError("ReplayRecord.audit_record_ids must not be empty")
        if not self.manifest_cycle_id:
            raise ValueError("ReplayRecord.manifest_cycle_id must not be empty")
        if not self.formal_snapshot_refs:
            raise ValueError("ReplayRecord.formal_snapshot_refs must not be empty")
        if not self.dagster_run_id:
            raise ValueError("ReplayRecord.dagster_run_id must not be empty")
        return self


__all__ = ["ReplayRecord"]
