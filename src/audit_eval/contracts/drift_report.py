"""Analytical drift report runtime contract."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime
from numbers import Real
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.contracts.common import JsonObject


class DriftReport(BaseModel):
    """Analytical Zone drift report shape."""

    model_config = ConfigDict(extra="forbid")

    report_id: str
    cycle_id: str | None
    baseline_ref: str
    target_ref: str
    evidently_json_ref: str
    drifted_features: list[JsonObject]
    regime_warning_level: Literal["none", "warning", "critical"]
    alert_rules_version: str
    created_at: datetime

    @field_validator("drifted_features")
    @classmethod
    def validate_drifted_features(cls, value: list[JsonObject]) -> list[JsonObject]:
        """Validate structured feature drift rows before analytical writes."""

        assert_no_forbidden_write(value, path="$.drifted_features")
        for index, feature in enumerate(value):
            if not isinstance(feature, Mapping):
                raise ValueError(
                    f"drifted_features[{index}] must be an object"
                )
            name = feature.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError(
                    f"drifted_features[{index}].name must be a non-empty string"
                )
            if not isinstance(feature.get("drifted"), bool):
                raise ValueError(
                    f"drifted_features[{index}].drifted must be a boolean"
                )
            if "score" not in feature and "statistic" not in feature:
                raise ValueError(
                    f"drifted_features[{index}] must include score or statistic"
                )
            metric_value = feature.get("score")
            if metric_value is None and "statistic" in feature:
                metric_value = feature["statistic"]
            _require_finite_number(
                metric_value,
                field_path=f"drifted_features[{index}].score",
            )
            _require_finite_number(
                feature.get("threshold"),
                field_path=f"drifted_features[{index}].threshold",
            )
        return value

    @model_validator(mode="after")
    def validate_boundary(self) -> Self:
        """Reject package-boundary fields anywhere in the analytical payload."""

        assert_no_forbidden_write(
            self.drifted_features,
            path="$.drifted_features",
        )
        assert_no_forbidden_write(self.model_dump(mode="python"))
        return self


def _require_finite_number(value: object, *, field_path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{field_path} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field_path} must be finite")
    return number


__all__ = ["DriftReport"]
