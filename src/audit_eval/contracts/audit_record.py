"""Formal audit record runtime contract."""

from datetime import datetime
from typing import ClassVar, Self

from pydantic import BaseModel, ConfigDict, model_validator

from audit_eval.contracts.common import JsonObject, LayerName


class AuditRecord(BaseModel):
    """Formal Zone audit record shape."""

    replay_field_names: ClassVar[tuple[str, ...]] = (
        "sanitized_input",
        "input_hash",
        "raw_output",
        "parsed_result",
        "output_hash",
    )

    model_config = ConfigDict(extra="forbid")

    record_id: str
    cycle_id: str
    layer: LayerName
    object_ref: str
    params_snapshot: JsonObject
    llm_lineage: JsonObject
    llm_cost: JsonObject
    sanitized_input: str | None
    input_hash: str | None
    raw_output: str | None
    parsed_result: JsonObject | None
    output_hash: str | None
    degradation_flags: JsonObject
    created_at: datetime

    @model_validator(mode="after")
    def require_replay_fields_when_llm_called(self) -> Self:
        """Formal LLM calls must carry a complete replay bundle."""

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


__all__ = ["AuditRecord"]
