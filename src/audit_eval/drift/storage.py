"""Storage, input, and Evidently boundaries for drift reporting."""

from __future__ import annotations

import math
from collections.abc import Mapping
from copy import deepcopy
from numbers import Real
from threading import Lock
from typing import Any, Protocol

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.contracts.common import JsonObject
from audit_eval.contracts.drift_report import DriftReport
from audit_eval.drift.schema import DriftedFeature, EvidentlyRunResult


class DriftInputError(RuntimeError):
    """Raised when drift input data is unavailable or invalid."""


class DriftStorageError(RuntimeError):
    """Raised when drift analytical storage is unavailable or fails."""


class EvidentlyRunnerError(RuntimeError):
    """Raised when the Evidently adapter is unavailable or fails."""


class DriftInputGateway(Protocol):
    """Input boundary for reference and target feature windows."""

    def load_feature_window(self, ref: str) -> Any:
        """Return a feature window by data-platform ref."""


class EvidentlyRunner(Protocol):
    """Boundary for running Evidently without exposing its runtime API."""

    def run(self, reference_data: Any, target_data: Any) -> EvidentlyRunResult:
        """Run drift detection and return normalized drift evidence."""


class DriftReportJsonWriter(Protocol):
    """Data-platform boundary for persisting the raw Evidently JSON payload."""

    def write_report_json(self, report_id: str, payload: JsonObject) -> str:
        """Write JSON content and return the externally owned ref."""


class DriftReportStorage(Protocol):
    """Analytical storage boundary for drift reports."""

    def append_drift_report(self, report: DriftReport) -> str:
        """Append a validated analytical drift report and return its id."""


class InMemoryDriftInputGateway:
    """In-memory input gateway for tests and Lite workflows."""

    def __init__(self, windows: Mapping[str, Any]) -> None:
        self.windows = deepcopy(dict(windows))
        self.loaded_refs: list[str] = []

    def load_feature_window(self, ref: str) -> Any:
        self.loaded_refs.append(ref)
        try:
            return deepcopy(self.windows[ref])
        except KeyError as exc:
            raise DriftInputError(f"No feature window configured for ref={ref!r}") from exc


class StaticEvidentlyRunner:
    """Static Evidently runner for deterministic tests."""

    def __init__(self, result: EvidentlyRunResult) -> None:
        self.result = result
        self.calls: list[tuple[Any, Any]] = []

    def run(self, reference_data: Any, target_data: Any) -> EvidentlyRunResult:
        self.calls.append((deepcopy(reference_data), deepcopy(target_data)))
        return self.result


class EvidentlyReportRunner:
    """Evidently adapter, imported lazily to keep unit tests cheap."""

    def run(self, reference_data: Any, target_data: Any) -> EvidentlyRunResult:
        try:
            DataDriftPreset, Report = _load_evidently_report_classes()
        except Exception as exc:  # pragma: no cover - depends on local env
            raise EvidentlyRunnerError(
                "Evidently is not importable; install the project dependency or "
                "pass evidently_runner=..."
            ) from exc

        report = Report(metrics=[DataDriftPreset()])
        try:
            try:
                report.run(reference_data=reference_data, current_data=target_data)
            except TypeError:
                report.run(reference_data=reference_data, target_data=target_data)
            payload = report.as_dict()
        except Exception as exc:  # pragma: no cover - depends on Evidently internals
            raise EvidentlyRunnerError(f"Evidently drift run failed: {exc}") from exc

        if not isinstance(payload, dict):
            raise EvidentlyRunnerError("Evidently report payload must be a JSON object")
        features = _extract_evidently_features(payload)
        return EvidentlyRunResult(
            report_json=payload,
            drifted_features=features,
            total_feature_count=_extract_total_feature_count(payload, features),
        )


def _load_evidently_report_classes() -> tuple[type[Any], type[Any]]:
    try:
        from evidently.metric_preset import DataDriftPreset
        from evidently.report import Report
    except ModuleNotFoundError:
        from evidently.legacy.metric_preset import DataDriftPreset
        from evidently.legacy.report import Report
    return DataDriftPreset, Report


class InMemoryDriftReportJsonWriter:
    """In-memory JSON writer that records externally returned report refs."""

    def __init__(self, ref_prefix: str = "memory://drift-report-json") -> None:
        self.ref_prefix = ref_prefix.rstrip("/")
        self.rows: list[JsonObject] = []
        self.write_calls = 0

    def write_report_json(self, report_id: str, payload: JsonObject) -> str:
        assert_no_forbidden_write(payload, path="$.evidently_json")
        row = {
            "report_id": report_id,
            "payload": deepcopy(payload),
        }
        assert_no_forbidden_write(row, path="$.json_writer")
        self.write_calls += 1
        self.rows.append(deepcopy(row))
        return f"{self.ref_prefix}/{report_id}.json"


class InMemoryDriftReportStorage:
    """In-memory analytical drift report storage for tests and Lite workflows."""

    def __init__(self) -> None:
        self.rows: list[JsonObject] = []
        self._lock = Lock()

    def append_drift_report(self, report: DriftReport) -> str:
        row = report.model_dump(mode="json")
        assert_no_forbidden_write(row, path="$.drift_report")
        validated = DriftReport.model_validate(deepcopy(row))
        stored_row = validated.model_dump(mode="json")
        with self._lock:
            self.rows.append(deepcopy(stored_row))
        return validated.report_id

    @property
    def reports(self) -> list[DriftReport]:
        """Return validated copies of stored reports."""

        with self._lock:
            rows = deepcopy(self.rows)
        return [DriftReport.model_validate(row) for row in rows]


def get_default_input_gateway() -> DriftInputGateway:
    """Return configured drift input gateway, or fail closed."""

    raise DriftInputError(
        "No default drift input gateway is configured; pass input_gateway=..."
    )


def get_default_evidently_runner() -> EvidentlyRunner:
    """Return configured Evidently runner, or fail closed."""

    raise EvidentlyRunnerError(
        "No default Evidently runner is configured; pass evidently_runner=..."
    )


def get_default_json_writer() -> DriftReportJsonWriter:
    """Return configured drift report JSON writer, or fail closed."""

    raise DriftStorageError(
        "No default drift report JSON writer is configured; pass json_writer=..."
    )


def get_default_storage() -> DriftReportStorage:
    """Return configured drift analytical storage, or fail closed."""

    raise DriftStorageError(
        "No default drift report storage is configured; pass storage=..."
    )


def _extract_evidently_features(payload: JsonObject) -> tuple[DriftedFeature, ...]:
    features: list[DriftedFeature] = []
    for metric in _iter_metric_results(payload):
        column_payloads = _extract_column_payloads(metric)
        for name, column_payload in column_payloads:
            feature = _feature_from_column_payload(name, column_payload)
            if feature is not None:
                features.append(feature)
    return tuple(features)


def _iter_metric_results(payload: JsonObject) -> tuple[Mapping[str, Any], ...]:
    metrics = payload.get("metrics", ())
    if not isinstance(metrics, list):
        return ()
    results: list[Mapping[str, Any]] = []
    for metric in metrics:
        if not isinstance(metric, Mapping):
            continue
        result = metric.get("result")
        if isinstance(result, Mapping):
            results.append(result)
    return tuple(results)


def _extract_column_payloads(
    result: Mapping[str, Any],
) -> tuple[tuple[str, Mapping[str, Any]], ...]:
    for key in ("drift_by_columns", "drift_by_features"):
        columns = result.get(key)
        if isinstance(columns, Mapping):
            return tuple(
                (str(name), payload)
                for name, payload in columns.items()
                if isinstance(payload, Mapping)
            )

    columns = result.get("columns")
    if isinstance(columns, list):
        extracted: list[tuple[str, Mapping[str, Any]]] = []
        for item in columns:
            if not isinstance(item, Mapping):
                continue
            name = (
                item.get("column_name")
                or item.get("feature_name")
                or item.get("name")
            )
            if isinstance(name, str) and name:
                extracted.append((name, item))
        return tuple(extracted)
    return ()


def _feature_from_column_payload(
    name: str,
    payload: Mapping[str, Any],
) -> DriftedFeature | None:
    feature_name = payload.get("column_name") or payload.get("feature_name") or name
    if not isinstance(feature_name, str) or not feature_name:
        return None
    return DriftedFeature(
        name=feature_name,
        score=_optional_number(
            payload.get("drift_score", payload.get("score", payload.get("p_value")))
        ),
        threshold=_optional_number(
            payload.get("threshold", payload.get("stattest_threshold"))
        ),
        drifted=bool(payload.get("drift_detected", payload.get("drifted", False))),
        statistic=_optional_number(payload.get("statistic")),
    )


def _optional_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool) or not isinstance(value, Real):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _extract_total_feature_count(
    payload: JsonObject,
    features: tuple[DriftedFeature, ...],
) -> int:
    for result in _iter_metric_results(payload):
        for key in (
            "number_of_columns",
            "number_of_features",
            "n_features",
            "columns_count",
        ):
            value = result.get(key)
            if isinstance(value, int) and value >= len(features):
                return value
        drift_by_columns = result.get("drift_by_columns")
        if isinstance(drift_by_columns, Mapping):
            return max(len(features), len(drift_by_columns))
    return len(features)


__all__ = [
    "DriftInputError",
    "DriftInputGateway",
    "DriftReportJsonWriter",
    "DriftReportStorage",
    "DriftStorageError",
    "EvidentlyReportRunner",
    "EvidentlyRunner",
    "EvidentlyRunnerError",
    "InMemoryDriftInputGateway",
    "InMemoryDriftReportJsonWriter",
    "InMemoryDriftReportStorage",
    "StaticEvidentlyRunner",
    "get_default_evidently_runner",
    "get_default_input_gateway",
    "get_default_json_writer",
    "get_default_storage",
]
