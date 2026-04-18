"""Analytical drift report runtime contract."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, model_validator

from audit_eval._boundary import assert_no_forbidden_write


class DriftedFeatureEvidence(BaseModel):
    """Structured evidence for one drifted feature."""

    model_config = ConfigDict(extra="forbid")

    name: str
    score: float | None = None
    statistic: float | None = None
    threshold: float
    drifted: bool

    @model_validator(mode="after")
    def validate_feature_evidence(self) -> Self:
        """Require complete, finite feature drift evidence."""

        if not self.name.strip():
            raise ValueError("drifted feature name must not be empty")
        if self.score is None and self.statistic is None:
            raise ValueError("drifted feature requires score or statistic")
        for field_name in ("score", "statistic", "threshold"):
            value = getattr(self, field_name)
            if value is not None and not math.isfinite(value):
                raise ValueError(f"{field_name} must be finite")
        return self


class DriftedFeaturesPayload(BaseModel):
    """Validated drifted feature evidence payload."""

    model_config = ConfigDict(extra="forbid")

    features: list[DriftedFeatureEvidence]


class DriftReport(BaseModel):
    """Analytical Zone drift report shape."""

    model_config = ConfigDict(extra="forbid")

    report_id: str
    cycle_id: str | None
    baseline_ref: str
    target_ref: str
    evidently_json_ref: str
    drifted_features: DriftedFeaturesPayload
    regime_warning_level: Literal["none", "warning", "critical"]
    alert_rules_version: str
    created_at: datetime

    @model_validator(mode="before")
    @classmethod
    def reject_forbidden_raw_payload(cls, payload: object) -> object:
        """Reject forbidden write fields before nested validation normalizes input."""

        assert_no_forbidden_write(payload)
        return payload

    @model_validator(mode="after")
    def validate_drift_report(self) -> Self:
        """Validate boundary safety and structural warning evidence."""

        drifted_features = self.drifted_features.features
        if self.regime_warning_level in {"warning", "critical"}:
            if not drifted_features:
                raise ValueError(
                    "warning and critical drift reports require feature evidence"
                )
            if not any(feature.drifted for feature in drifted_features):
                raise ValueError(
                    "warning and critical drift reports require drifted evidence"
                )

        assert_no_forbidden_write(
            self.drifted_features.model_dump(mode="python"),
            path="$.drifted_features",
        )
        assert_no_forbidden_write(self.model_dump(mode="python"))
        return self


__all__ = [
    "DriftReport",
    "DriftedFeatureEvidence",
    "DriftedFeaturesPayload",
]
