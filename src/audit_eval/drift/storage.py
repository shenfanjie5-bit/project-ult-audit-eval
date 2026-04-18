"""Storage and adapter boundaries for drift reporting."""

from __future__ import annotations

import json
import re
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
            report_cls, preset_cls = _load_evidently_report_components()
        except Exception as exc:  # pragma: no cover - environment dependent
            raise DriftRunnerError(
                "Evidently is unavailable; install runtime dependencies or pass "
                "evidently_runner=..."
            ) from exc

        report = report_cls(metrics=[preset_cls()])
        try:
            run_output = report.run(
                reference_data=reference_data,
                current_data=target_data,
            )
            payload = _report_to_payload(run_output or report)
        except Exception as exc:  # pragma: no cover - depends on Evidently internals
            raise DriftRunnerError(f"Evidently drift report failed: {exc}") from exc

        features = _extract_drifted_features(payload)
        return EvidentlyRunResult(
            report_json=payload,
            drifted_features=features,
            dataset_drift=_extract_dataset_drift(payload),
            feature_count=_extract_feature_count(payload, features),
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
    elif hasattr(report, "dict"):
        payload = report.dict()
    else:
        raise DriftRunnerError("Evidently report does not expose JSON payload output")

    if not isinstance(payload, dict):
        raise DriftRunnerError("Evidently report payload must be a JSON object")
    return payload


def _load_evidently_report_components() -> tuple[type[Any], type[Any]]:
    last_error: Exception | None = None
    for report_module_name, preset_module_name in (
        ("evidently.report", "evidently.metric_preset"),
        ("evidently.core.report", "evidently.presets"),
    ):
        try:
            report_module = import_module(report_module_name)
            preset_module = import_module(preset_module_name)
            return (
                getattr(report_module, "Report"),
                getattr(preset_module, "DataDriftPreset"),
            )
        except Exception as exc:  # pragma: no cover - environment dependent
            last_error = exc

    raise DriftRunnerError(
        "No supported Evidently data drift API is available"
    ) from last_error


def _extract_drifted_features(payload: object) -> tuple[DriftedFeature, ...]:
    features: list[DriftedFeature] = []
    seen_names: set[str] = set()
    for item in _iter_mappings(payload):
        feature = _feature_from_mapping(item)
        if feature is None or feature.name in seen_names:
            continue
        seen_names.add(feature.name)
        features.append(feature)
    return tuple(features)


def _feature_from_mapping(item: Mapping[str, Any]) -> DriftedFeature | None:
    name = _first_string(item, ("column_name", "feature_name", "name"))
    if name is not None:
        drifted_value = _first_existing(
            item,
            ("drift_detected", "drifted", "is_drifted"),
        )
        if drifted_value is None:
            return None
        return DriftedFeature(
            name=name,
            score=_first_number(
                item,
                ("drift_score", "score", "stattest_value", "p_value"),
            ),
            statistic=_first_number(item, ("statistic", "stattest_value")),
            threshold=_first_number(item, ("threshold", "stattest_threshold")),
            drifted=bool(drifted_value),
        )

    config = item.get("config")
    if not isinstance(config, Mapping):
        return None
    metric_type = str(config.get("type", ""))
    metric_name = str(item.get("metric_name", ""))
    if "ValueDrift" not in metric_type and "ValueDrift" not in metric_name:
        return None

    feature_name = _first_string(config, ("column", "column_name", "feature_name"))
    if feature_name is None:
        feature_name = _parse_metric_name_part(metric_name, "column")
    if feature_name is None:
        return None

    score = _metric_value_number(item)
    threshold = _first_number(config, ("threshold", "stattest_threshold"))
    if threshold is None:
        threshold = _parse_metric_name_number(metric_name, "threshold")

    drifted_value = _first_existing(item, ("drift_detected", "drifted", "is_drifted"))
    if drifted_value is None:
        method = config.get("method") or _parse_metric_name_part(
            metric_name,
            "method",
        )
        drifted_value = _infer_value_drifted(
            score=score,
            threshold=threshold,
            method=str(method or ""),
        )
    if drifted_value is None:
        return None

    return DriftedFeature(
        name=feature_name,
        score=score,
        statistic=None,
        threshold=threshold,
        drifted=bool(drifted_value),
    )


def _extract_dataset_drift(payload: object) -> bool:
    for item in _iter_mappings(payload):
        value = item.get("dataset_drift")
        if isinstance(value, bool):
            return value
        if _is_drifted_columns_count_metric(item):
            count = _metric_count(item)
            if count is not None:
                return count > 0
    return False


def _extract_feature_count(
    payload: object,
    features: Sequence[DriftedFeature],
) -> int:
    observed_count = len(features)
    for item in _iter_mappings(payload):
        if not _is_drifted_columns_count_metric(item):
            continue
        count = _metric_count(item)
        share = _metric_share(item)
        if count is not None and share is not None and share > 0:
            return max(observed_count, round(count / share))
    return observed_count


def _is_drifted_columns_count_metric(item: Mapping[str, Any]) -> bool:
    config = item.get("config")
    metric_type = str(config.get("type", "")) if isinstance(config, Mapping) else ""
    metric_name = str(item.get("metric_name", ""))
    return (
        "DriftedColumnsCount" in metric_type
        or "DriftedColumnsCount" in metric_name
    )


def _metric_count(item: Mapping[str, Any]) -> float | None:
    value = item.get("value")
    if isinstance(value, Mapping):
        return _first_number(value, ("count", "drifted_count"))
    return None


def _metric_share(item: Mapping[str, Any]) -> float | None:
    value = item.get("value")
    if isinstance(value, Mapping):
        return _first_number(value, ("share", "drift_share"))
    return None


def _metric_value_number(item: Mapping[str, Any]) -> float | None:
    value = item.get("value")
    if _is_number(value):
        return float(value)
    if isinstance(value, Mapping):
        return _first_number(
            value,
            ("drift_score", "score", "stattest_value", "p_value", "value"),
        )
    return None


def _infer_value_drifted(
    *,
    score: float | None,
    threshold: float | None,
    method: str,
) -> bool | None:
    if score is None or threshold is None:
        return None
    normalized_method = method.lower().replace("-", "_")
    if "p_value" in normalized_method or "pvalue" in normalized_method:
        return score < threshold
    return score > threshold


def _parse_metric_name_part(metric_name: str, key: str) -> str | None:
    match = re.search(rf"(?:^|,){re.escape(key)}=([^,)]+)", metric_name)
    if match is None:
        return None
    value = match.group(1).strip()
    return value or None


def _parse_metric_name_number(metric_name: str, key: str) -> float | None:
    value = _parse_metric_name_part(metric_name, key)
    if value is None:
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    return number if number == number else None


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
    if not _is_number(value):
        return None
    number = float(value)
    return number if number == number else None


def _is_number(value: object) -> bool:
    return isinstance(value, Real) and not isinstance(value, bool)


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
