"""Runtime input contract for formal audit/replay writes."""

from datetime import datetime
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from audit_eval._boundary import assert_no_forbidden_write
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

    @model_validator(mode="after")
    def validate_bundle_boundaries(self) -> Self:
        """Validate replay references and package-level forbidden write fields."""

        assert_no_forbidden_write(self.model_dump(mode="python"))

        audit_records_by_id = self.audit_records_by_id()
        for audit_record in self.audit_records:
            if audit_record.cycle_id != self.manifest_cycle_id:
                raise ValueError(
                    "AuditRecord.cycle_id must match "
                    "AuditWriteBundle.manifest_cycle_id"
                )

        for replay_record in self.replay_records:
            if replay_record.cycle_id != self.manifest_cycle_id:
                raise ValueError(
                    "ReplayRecord.cycle_id must match "
                    "AuditWriteBundle.manifest_cycle_id"
                )
            if replay_record.manifest_cycle_id != self.manifest_cycle_id:
                raise ValueError(
                    "ReplayRecord.manifest_cycle_id must match "
                    "AuditWriteBundle.manifest_cycle_id"
                )

            missing_record_ids = [
                record_id
                for record_id in replay_record.audit_record_ids
                if record_id not in audit_records_by_id
            ]
            if missing_record_ids:
                missing = ", ".join(missing_record_ids)
                raise ValueError(
                    "ReplayRecord.audit_record_ids reference missing "
                    f"AuditRecord.record_id values: {missing}"
                )
            mismatched_record_ids = [
                record_id
                for record_id in replay_record.audit_record_ids
                if audit_records_by_id[record_id].cycle_id != replay_record.cycle_id
            ]
            if mismatched_record_ids:
                mismatched = ", ".join(mismatched_record_ids)
                raise ValueError(
                    "ReplayRecord.audit_record_ids reference AuditRecord rows "
                    "from a different cycle_id: "
                    f"{mismatched}"
                )
        return self

    def audit_records_by_id(self) -> dict[str, AuditRecord]:
        """Return audit records keyed by record_id."""

        return {record.record_id: record for record in self.audit_records}

    def replay_records_by_object_ref(self) -> dict[str, ReplayRecord]:
        """Return replay records keyed by object_ref."""

        return {record.object_ref: record for record in self.replay_records}


__all__ = ["AuditWriteBundle"]
