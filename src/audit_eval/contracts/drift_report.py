"""Analytical drift report runtime contract."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, model_validator

from audit_eval._boundary import (
    BoundaryViolationError,
    FORBIDDEN_WRITE_FIELDS,
    assert_no_forbidden_write,
)
from audit_eval.contracts.common import JsonObject

RegimeWarningLevelValue = Literal["none", "warning", "critical"]

_CONTROL_FIELD_NAMES = frozenset(
    (*FORBIDDEN_WRITE_FIELDS, "_".join(("online", "control")))
)
_IDENTIFIER_FIELD_NAMES = frozenset(("column_name", "feature_name", "name"))


class DriftReport(BaseModel):
    """Analytical Zone drift report shape."""

    model_config = ConfigDict(extra="forbid")

    report_id: str
    cycle_id: str | None
    baseline_ref: str
    target_ref: str
    evidently_json_ref: str
    drifted_features: JsonObject
    regime_warning_level: RegimeWarningLevelValue
    alert_rules_version: str
    created_at: datetime

    @model_validator(mode="after")
    def validate_boundary_fields(self) -> Self:
        """Reject forbidden write fields and control-like drift identifiers."""

        if self.regime_warning_level not in {"none", "warning", "critical"}:
            raise ValueError(
                "regime_warning_level must be one of: none, warning, critical"
            )

        assert_no_forbidden_write(
            self.drifted_features,
            path="$.drifted_features",
        )
        assert_no_forbidden_write(self.model_dump(mode="python"), path="$")
        assert_no_drift_control_write(
            self.drifted_features,
            path="$.drifted_features",
        )
        assert_no_drift_control_write(self.model_dump(mode="python"), path="$")
        return self


def assert_no_drift_control_write(payload: object, path: str = "$") -> None:
    """Reject drift payload keys or feature identifiers that target controls."""

    forbidden_paths = tuple(_iter_control_paths(payload, path))
    if forbidden_paths:
        fields = ", ".join(forbidden_paths)
        raise BoundaryViolationError(f"Forbidden drift control field(s): {fields}")


def assert_no_drift_control_columns(payload: object, path: str = "$") -> None:
    """Reject supported tabular feature-window schemas with control columns."""

    forbidden_paths: list[str] = []
    for index, column_name in enumerate(_iter_tabular_column_names(payload)):
        if _is_control_name(column_name):
            forbidden_paths.append(f"{path}.columns[{index}] ({column_name})")

    if forbidden_paths:
        fields = ", ".join(forbidden_paths)
        raise BoundaryViolationError(f"Forbidden drift control field(s): {fields}")


def _iter_control_paths(payload: object, path: str) -> tuple[str, ...]:
    if isinstance(payload, str) and _is_control_name(payload):
        return (f"{path} ({payload})",)

    if isinstance(payload, Mapping):
        paths: list[str] = []
        for key, value in payload.items():
            field_path = f"{path}.{key}"
            if isinstance(key, str) and _is_control_name(key):
                paths.append(field_path)
            if (
                isinstance(key, str)
                and key in _IDENTIFIER_FIELD_NAMES
                and isinstance(value, str)
                and _is_control_name(value)
            ):
                paths.append(f"{field_path} ({value})")
            paths.extend(_iter_control_paths(value, field_path))
        return tuple(paths)

    if isinstance(payload, Sequence) and not isinstance(
        payload,
        (str, bytes, bytearray),
    ):
        paths = []
        for index, value in enumerate(payload):
            paths.extend(_iter_control_paths(value, f"{path}[{index}]"))
        return tuple(paths)

    return ()


def _iter_tabular_column_names(payload: object) -> tuple[str, ...]:
    names: list[str] = []

    if isinstance(payload, Mapping):
        names.extend(str(key) for key in payload if isinstance(key, str))

    columns = getattr(payload, "columns", None)
    if columns is not None:
        names.extend(str(column) for column in columns)

    column_names = getattr(payload, "column_names", None)
    if column_names is not None:
        names.extend(str(column) for column in column_names)

    schema = getattr(payload, "schema", None)
    schema_names = getattr(schema, "names", None)
    if schema_names is not None:
        names.extend(str(column) for column in schema_names)

    return tuple(dict.fromkeys(names))


def _is_control_name(name: str) -> bool:
    return name.strip().lower() in _CONTROL_FIELD_NAMES


__all__ = [
    "DriftReport",
    "assert_no_drift_control_columns",
    "assert_no_drift_control_write",
]
