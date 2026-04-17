"""Runtime input contract for formal audit/replay writes."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from audit_eval.contracts.audit_record import AuditRecord
from audit_eval.contracts.common import JsonObject
from audit_eval.contracts.replay_record import ReplayRecord


class AuditWriteBundle(BaseModel):
    """Validated audit/replay write payload shared by writers and queries."""

    model_config = ConfigDict(extra="forbid")

    bundle_id: str
    manifest_cycle_id: str
    audit_records: list[AuditRecord]
    replay_records: list[ReplayRecord]
    formal_partition_tag: str = "formal"
    analytical_partition_tag: str | None = None
    submitted_at: datetime
    metadata: JsonObject = Field(default_factory=dict)

    def audit_records_by_id(self) -> dict[str, AuditRecord]:
        """Return audit records keyed by record_id."""

        return {record.record_id: record for record in self.audit_records}

    def replay_records_by_object_ref(self) -> dict[str, ReplayRecord]:
        """Return replay records keyed by object_ref."""

        return {record.object_ref: record for record in self.replay_records}


__all__ = ["AuditWriteBundle"]
