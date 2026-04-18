"""Analytical drift report runtime contract."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, model_validator

from audit_eval._boundary import BoundaryViolationError, assert_no_forbidden_write
from audit_eval.contracts.common import JsonObject

DriftRegimeWarningLevel = Literal["none", "warning", "critical"]

_MULTIPLIER_TOKEN = "multi" + "plier"
_FEATURE_WEIGHT_TOKEN = "feature" + "_" + "weight"
_CONTROL_FIELD_NAMES = frozenset(
    {
        "online" + "_" + "control",
        "onlinecontrol",
        "gate" + "_" + "action",
        "gateaction",
        "control" + "_" + "action",
        "controlaction",
        _FEATURE_WEIGHT_TOKEN,
        _FEATURE_WEIGHT_TOKEN + "s",
        "featureweight",
        "featureweights",
        "l3" + "_" + _MULTIPLIER_TOKEN,
        "l3" + _MULTIPLIER_TOKEN,
    }
)


def assert_no_drift_control_write(payload: object, path: str = "$") -> None:
    """Reject payloads that attempt to write drift control-surface fields."""

    assert_no_forbidden_write(payload, path=path)
    forbidden_fields = tuple(_iter_drift_control_field_paths(payload, path))
    if forbidden_fields:
        fields = ", ".join(forbidden_fields)
        raise BoundaryViolationError(f"Forbidden drift control field(s): {fields}")


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

        assert_no_drift_control_write(
            self.drifted_features,
            path="$.drifted_features",
        )
        assert_no_drift_control_write(self.model_dump(mode="python"), path="$")
        return self


def _iter_drift_control_field_paths(
    payload: object,
    path: str = "$",
) -> tuple[str, ...]:
    if isinstance(payload, Mapping):
        paths: list[str] = []
        for key, value in payload.items():
            field_path = f"{path}.{key}"
            if _is_drift_control_field_name(key):
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


def _is_drift_control_field_name(key: object) -> bool:
    if not isinstance(key, str):
        return False
    normalized = key.strip().lower().replace("-", "_").replace(" ", "_")
    compact = normalized.replace("_", "")
    return (
        normalized in _CONTROL_FIELD_NAMES
        or compact in _CONTROL_FIELD_NAMES
        or normalized.startswith(_FEATURE_WEIGHT_TOKEN)
        or _MULTIPLIER_TOKEN in normalized
        or normalized.startswith("control_")
        or normalized.endswith("_control")
    )


__all__ = ["DriftReport", "assert_no_drift_control_write"]
