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
    paths = list(_iter_tabular_schema_field_paths(payload, path))

    if isinstance(payload, Mapping):
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
        for index, value in enumerate(payload):
            paths.extend(_iter_drift_control_field_paths(value, f"{path}[{index}]"))
        return tuple(paths)

    return tuple(paths)


def _iter_tabular_schema_field_paths(payload: object, path: str) -> tuple[str, ...]:
    column_names = _extract_tabular_column_names(payload)
    paths: list[str] = []
    for index, column_name in enumerate(column_names):
        if _is_drift_control_column_name(column_name):
            paths.append(
                f"{path}.columns[{index}:{_column_name_display(column_name)}]"
            )
    return tuple(paths)


def _extract_tabular_column_names(payload: object) -> tuple[object, ...]:
    for attribute_path in (
        ("column_names",),
        ("schema", "names"),
        ("columns",),
    ):
        value = _read_attribute_path(payload, attribute_path)
        if value is None:
            continue
        names = _coerce_column_names(value)
        if names or _is_empty_column_collection(value):
            return names
    return ()


def _read_attribute_path(payload: object, attribute_path: tuple[str, ...]) -> object:
    value = payload
    for attribute_name in attribute_path:
        try:
            value = getattr(value, attribute_name)
        except Exception:
            return None
        if callable(value):
            return None
    return value


def _coerce_column_names(value: object) -> tuple[object, ...]:
    if isinstance(value, (str, bytes, bytearray)):
        return (value,)
    try:
        return tuple(value)  # type: ignore[arg-type]
    except TypeError:
        return ()


def _is_empty_column_collection(value: object) -> bool:
    try:
        return len(value) == 0  # type: ignore[arg-type]
    except Exception:
        return False


def _is_drift_control_column_name(column_name: object) -> bool:
    if _is_drift_control_field_name(column_name):
        return True
    if isinstance(column_name, Sequence) and not isinstance(
        column_name,
        (str, bytes, bytearray),
    ):
        return any(_is_drift_control_column_name(part) for part in column_name)
    return False


def _column_name_display(column_name: object) -> str:
    if isinstance(column_name, Sequence) and not isinstance(
        column_name,
        (str, bytes, bytearray),
    ):
        return ".".join(str(part) for part in column_name)
    return str(column_name)


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
