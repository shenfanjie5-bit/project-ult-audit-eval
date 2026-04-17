"""Draft replay contracts for offline replay spikes."""

from datetime import datetime
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, model_validator


class ReplayBundleFields(BaseModel):
    """Historical LLM replay fields captured at original execution time."""

    model_config = ConfigDict(extra="forbid")

    sanitized_input: str | None
    input_hash: str | None
    raw_output: str | None
    parsed_result: dict[str, Any] | None
    output_hash: str | None


class AuditRecordDraft(ReplayBundleFields):
    """Draft formal audit record shape used by the replay spike."""

    replay_field_names: ClassVar[tuple[str, ...]] = (
        "sanitized_input",
        "input_hash",
        "raw_output",
        "parsed_result",
        "output_hash",
    )

    record_id: str
    cycle_id: str
    layer: Literal["L3", "L4", "L5", "L6", "L7", "L8"]
    object_ref: str
    params_snapshot: dict[str, Any]
    llm_lineage: dict[str, Any]
    llm_cost: dict[str, Any]
    degradation_flags: dict[str, Any]
    created_at: datetime

    @model_validator(mode="after")
    def require_replay_fields_when_llm_called(self) -> "AuditRecordDraft":
        """C5: formal LLM calls must carry a complete replay bundle."""

        if self.llm_lineage.get("called") is True:
            missing_fields = [
                field_name
                for field_name in self.replay_field_names
                if getattr(self, field_name) is None
            ]
            if missing_fields:
                fields = ", ".join(missing_fields)
                raise ValueError(
                    "LLM-called audit records require replay fields: "
                    f"{fields}"
                )
        return self


class ReplayRecordDraft(BaseModel):
    """Draft replay record shape for read-history reconstruction."""

    model_config = ConfigDict(extra="forbid")

    replay_id: str
    cycle_id: str
    object_ref: str
    audit_record_ids: list[str]
    manifest_cycle_id: str
    formal_snapshot_refs: dict[str, str]
    graph_snapshot_ref: str | None = None
    dagster_run_id: str
    replay_mode: Literal["read_history"]


class ReplayViewDraft(BaseModel):
    """Draft replay view returned by read-history reconstruction."""

    model_config = ConfigDict(extra="forbid")

    cycle_id: str
    object_ref: str
    replay_record: ReplayRecordDraft
    audit_records: list[AuditRecordDraft]
    manifest_snapshot_set: dict[str, str]
    historical_formal_objects: dict[str, dict[str, Any]]
    graph_snapshot_ref: str | None
    graph_snapshot_summary: dict[str, Any] | None
    dagster_run_summary: dict[str, Any]

    @model_validator(mode="after")
    def require_replay_record_metadata_consistency(self) -> "ReplayViewDraft":
        """Keep top-level replay metadata aligned with the source record."""

        if self.cycle_id != self.replay_record.cycle_id:
            raise ValueError("ReplayView cycle_id must match replay_record")
        if self.object_ref != self.replay_record.object_ref:
            raise ValueError("ReplayView object_ref must match replay_record")
        if self.graph_snapshot_ref != self.replay_record.graph_snapshot_ref:
            raise ValueError("ReplayView graph_snapshot_ref must match replay_record")
        if self.graph_snapshot_ref is None and self.graph_snapshot_summary is not None:
            raise ValueError("Graph summary requires graph_snapshot_ref")
        if self.graph_snapshot_ref is not None and self.graph_snapshot_summary is None:
            raise ValueError("Graph snapshot refs require a loaded summary")
        return self


__all__ = [
    "AuditRecordDraft",
    "ReplayBundleFields",
    "ReplayRecordDraft",
    "ReplayViewDraft",
]
