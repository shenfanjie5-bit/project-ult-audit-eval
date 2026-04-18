"""Analytical drift report runtime contract."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, model_validator

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.contracts.common import JsonObject

DriftRegimeWarningLevel = Literal["none", "warning", "critical"]


class DriftReport(BaseModel):
    """Analytical Zone drift report shape."""

    model_config = ConfigDict(extra="forbid")

    report_id: str
    cycle_id: str | None
    baseline_ref: str
    target_ref: str
    evidently_json_ref: str
    drifted_features: JsonObject
    regime_warning_level: DriftRegimeWarningLevel
    alert_rules_version: str
    created_at: datetime

    @model_validator(mode="after")
    def validate_write_boundary(self) -> Self:
        """Reject payloads that attempt to cross analytical write boundaries."""

        assert_no_forbidden_write(
            self.drifted_features,
            path="$.drifted_features",
        )
        assert_no_forbidden_write(self.model_dump(mode="python"), path="$")
        return self


__all__ = ["DriftReport"]
