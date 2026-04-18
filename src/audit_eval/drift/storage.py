"""Storage and adapter boundaries for drift reporting."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from importlib import import_module
from numbers import Real
from typing import Any, Protocol

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.contracts.common import JsonObject
from audit_eval.contracts.drift_report import (
    DriftReport,
    assert_no_drift_control_write,
)
from audit_eval.drift.schema import DriftedFeature, EvidentlyRunResult


class DriftInputError(RuntimeError):
    """Raised when drift input data is unavailable or invalid."""


class DriftRunnerError(RuntimeError):
    """Raised when Evidently execution is unavailable or fails."""


class DriftStorageError(RuntimeError):
    """Raised when drift analytical storage is unavailable or fails."""


class DriftInputGateway(Protocol):
    """Input boundary for reference and target feature windows."""

    def load_feature_window(self, ref: str) -> Any:
        """Return the feature window identified by ref."""


class EvidentlyRunner(Protocol):
    """Adapter boundary for Evidently report execution."""

    def run(self, reference_data: Any, target_data: Any) -> EvidentlyRunResult:
        """Run Evidently and return normalized drift output."""


class DriftReportJsonWriter(Protocol):
    """Data-platform boundary for report JSON content writes."""

    def write_report_json(self, report_id: str, payload: JsonObject) -> str:
        """Write report JSON content and return the data-platform reference."""


class DriftReportStorage(Protocol):
    """Analytical storage boundary for drift reports."""

    def append_drift_report(self, report: DriftReport) -> str:
        """Append a validated drift report and return its report_id."""


class InMemoryDriftInputGateway:
    """In-memory drift input gateway for tests and Lite workflows."""

    def __init__(self, windows_by_ref: Mapping[str, Any]) -> None:
        self.windows_by_ref = dict(windows_by_ref)
        self.loaded_refs: list[str] = []

    def load_feature_window(self, ref: str) -> Any:
        self.loaded_refs.append(ref)
        try:
            return deepcopy(self.windows_by_ref[ref])
        except KeyError as exc:
            raise DriftInputError(f"Unknown drift feature window ref: {ref}") from exc


class InMemoryEvidentlyRunner:
    """In-memory Evidently runner for deterministic tests."""

    def __init__(self, result: EvidentlyRunResult) -> None:
        self.result = result
        self.calls: list[tuple[Any, Any]] = []

    def run(self, reference_data: Any, target_data: Any) -> EvidentlyRunResult:
        self.calls.append((reference_data, target_data))
        return self.result


class InMemoryDriftReportJsonWriter:
    """In-memory report JSON writer for tests and Lite workflows."""

    def __init__(self, ref_prefix: str = "memory://drift-reports") -> None:
        self.ref_prefix = ref_prefix.rstrip("/")
        self.payloads_by_report_id: dict[str, JsonObject] = {}
        self.calls: list[tuple[str, JsonObject]] = []

    def write_report_json(self, report_id: str, payload: JsonObject) -> str:
        assert_no_forbidden_write(payload, path="$.evidently_json")
        assert_no_drift_control_write(payload, path="$.evidently_json")
        stored_payload = deepcopy(payload)
        self.payloads_by_report_id[report_id] = stored_payload
        self.calls.append((report_id, stored_payload))
        return f"{self.ref_prefix}/{report_id}.json"


class InMemoryDriftReportStorage:
    """In-memory drift analytical storage for tests and Lite workflows."""

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.reports: list[DriftReport] = []

    def append_drift_report(self, report: DriftReport) -> str:
        row = report.model_dump(mode="json")
        assert_no_forbidden_write(row, path="$.drift_report")
        assert_no_drift_control_write(row, path="$.drift_report")
        self.rows.append(deepcopy(row))
        self.reports.append(report)
        return report.report_id


class EvidentlyDataDriftRunner:
    """Default Evidently adapter with lazy imports."""

    def run(self, reference_data: Any, target_data: Any) -> EvidentlyRunResult:
        try:
            report_module = import_module("evidently.report")
            preset_module = import_module("evidently.metric_preset")
            report_cls = getattr(report_module, "Report")
            preset_cls = getattr(preset_module, "DataDriftPreset")
        except Exception as exc:  # pragma: no cover - environment dependent
            raise DriftRunnerError(
                "Evidently is unavailable; install runtime dependencies or pass "
                "evidently_runner=..."
            ) from exc

        report = report_cls(metrics=[preset_cls()])
        try:
            report.run(reference_data=reference_data, current_data=target_data)
            payload = _report_to_payload(report)
        except Exception as exc:  # pragma: no cover - depends on Evidently internals
            raise DriftRunnerError(f"Evidently drift report failed: {exc}") from exc

        features = _extract_drifted_features(payload)
        return EvidentlyRunResult(
            report_json=payload,
            drifted_features=features,
            dataset_drift=_extract_dataset_drift(payload),
            feature_count=len(features),
        )


def get_default_input_gateway() -> DriftInputGateway:
    """Return configured drift input gateway, or fail closed."""

    raise DriftInputError(
        "No default drift input gateway is configured; pass input_gateway=..."
    )


def get_default_evidently_runner() -> EvidentlyRunner:
    """Return the lazy Evidently runner adapter."""

    return EvidentlyDataDriftRunner()


def get_default_json_writer() -> DriftReportJsonWriter:
    """Return configured report JSON writer, or fail closed."""

    raise DriftStorageError(
        "No default drift report JSON writer is configured; pass json_writer=..."
    )


def get_default_drift_report_storage() -> DriftReportStorage:
    """Return configured drift analytical storage, or fail closed."""

    raise DriftStorageError(
        "No default drift report storage is configured; pass storage=..."
    )


def _report_to_payload(report: Any) -> JsonObject:
    if hasattr(report, "as_dict"):
        payload = report.as_dict()
    elif hasattr(report, "json"):
        payload = json.loads(report.json())
    else:
        raise DriftRunnerError("Evidently report does not expose JSON payload output")

    if not isinstance(payload, dict):
        raise DriftRunnerError("Evidently report payload must be a JSON object")
    return payload


def _extract_drifted_features(payload: object) -> tuple[DriftedFeature, ...]:
    features: list[DriftedFeature] = []
    seen_names: set[str] = set()
    for item in _iter_mappings(payload):
        name = _first_string(item, ("column_name", "feature_name", "name"))
        if name is None or name in seen_names:
            continue
        drifted_value = _first_existing(
            item,
            ("drift_detected", "drifted", "is_drifted"),
        )
        if drifted_value is None:
            continue
        seen_names.add(name)
        features.append(
            DriftedFeature(
                name=name,
                score=_first_number(
                    item,
                    ("drift_score", "score", "stattest_value", "p_value"),
                ),
                statistic=_first_number(item, ("statistic", "stattest_value")),
                threshold=_first_number(item, ("threshold", "stattest_threshold")),
                drifted=bool(drifted_value),
            )
        )
    return tuple(features)


def _extract_dataset_drift(payload: object) -> bool:
    for item in _iter_mappings(payload):
        value = item.get("dataset_drift")
        if isinstance(value, bool):
            return value
    return False


def _iter_mappings(payload: object) -> tuple[Mapping[str, Any], ...]:
    if isinstance(payload, Mapping):
        children: list[Mapping[str, Any]] = [payload]
        for value in payload.values():
            children.extend(_iter_mappings(value))
        return tuple(children)

    if isinstance(payload, Sequence) and not isinstance(
        payload,
        (str, bytes, bytearray),
    ):
        children = []
        for value in payload:
            children.extend(_iter_mappings(value))
        return tuple(children)

    return ()


def _first_existing(item: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in item:
            return item[key]
    return None


def _first_string(item: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    value = _first_existing(item, keys)
    return value if isinstance(value, str) else None


def _first_number(item: Mapping[str, Any], keys: Sequence[str]) -> float | None:
    value = _first_existing(item, keys)
    if isinstance(value, bool) or not isinstance(value, Real):
        return None
    number = float(value)
    return number if number == number else None


__all__ = [
    "DriftInputError",
    "DriftInputGateway",
    "DriftReportJsonWriter",
    "DriftReportStorage",
    "DriftRunnerError",
    "DriftStorageError",
    "EvidentlyDataDriftRunner",
    "EvidentlyRunner",
    "InMemoryDriftInputGateway",
    "InMemoryDriftReportJsonWriter",
    "InMemoryDriftReportStorage",
    "InMemoryEvidentlyRunner",
    "get_default_drift_report_storage",
    "get_default_evidently_runner",
    "get_default_input_gateway",
    "get_default_json_writer",
]
