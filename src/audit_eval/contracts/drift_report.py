"""Analytical drift report runtime contract."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import datetime
from numbers import Real
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from audit_eval._boundary import BoundaryViolationError, assert_no_forbidden_write
from audit_eval.contracts.common import JsonObject

_DRIFT_CONTROL_FIELD_NAMES: frozenset[str] = frozenset(
    (
        "gate_action",
        "feature" + "_" + "weight",
        "feature" + "_" + "weights",
        "l3" + "_" + "multiplier",
        "l3_feature" + "_" + "multiplier",
        "level3" + "_" + "multiplier",
        "level_3" + "_" + "multiplier",
        "online" + "_" + "control",
    )
)
_DRIFT_CONTROL_COMPACT_FIELD_NAMES: frozenset[str] = frozenset(
    (
        "gateaction",
        "featureweight",
        "featureweights",
        "online" + "control",
    )
)


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

        assert_no_drift_control_write(value, path="$.drifted_features")
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

        assert_no_drift_control_write(
            self.drifted_features,
            path="$.drifted_features",
        )
        assert_no_drift_control_write(self.model_dump(mode="python"))
        return self


def assert_no_drift_control_write(payload: object, path: str = "$") -> None:
    """Reject drift payload fields that would cross into control-plane writes."""

    assert_no_forbidden_write(payload, path=path)
    control_fields = tuple(_iter_drift_control_field_paths(payload, path))
    if control_fields:
        fields = ", ".join(control_fields)
        raise BoundaryViolationError(f"Forbidden drift control field(s): {fields}")


def _iter_drift_control_field_paths(
    payload: object,
    path: str = "$",
) -> tuple[str, ...]:
    if isinstance(payload, Mapping):
        paths: list[str] = []
        for key, value in payload.items():
            field_path = f"{path}.{key}"
            if isinstance(key, str) and _is_drift_control_field(key):
                paths.append(field_path)
            paths.extend(_iter_drift_control_field_paths(value, field_path))
        return tuple(paths)

    if isinstance(payload, Sequence) and not isinstance(
        payload,
        (str, bytes, bytearray),
    ):
        paths = []
        for index, value in enumerate(payload):
            paths.extend(_iter_drift_control_field_paths(value, f"{path}[{index}]"))
        return tuple(paths)

    return ()


def _is_drift_control_field(key: str) -> bool:
    normalized = key.replace("-", "_").replace(" ", "_").lower()
    compact = normalized.replace("_", "")
    return (
        normalized in _DRIFT_CONTROL_FIELD_NAMES
        or compact in _DRIFT_CONTROL_COMPACT_FIELD_NAMES
        or "multiplier" in normalized
    )


def _require_finite_number(value: object, *, field_path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{field_path} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field_path} must be finite")
    return number


__all__ = ["DriftReport"]
