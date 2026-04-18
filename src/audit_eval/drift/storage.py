"""Storage and adapter boundaries for drift reporting."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from numbers import Real
from typing import Any, Protocol

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.contracts.common import JsonObject
from audit_eval.contracts.drift_report import DriftReport
from audit_eval.drift.schema import DriftedFeature, EvidentlyRunResult


class DriftInputError(RuntimeError):
    """Raised when drift input windows are unavailable or invalid."""


class DriftRunnerError(RuntimeError):
    """Raised when Evidently report generation is unavailable or fails."""


class DriftStorageError(RuntimeError):
    """Raised when drift analytical storage is unavailable or fails."""


class DriftInputGateway(Protocol):
    """Input boundary for historical feature windows."""

    def load_feature_window(self, ref: str) -> Any:
        """Return a point-in-time feature window by external reference."""


class EvidentlyRunner(Protocol):
    """Adapter boundary for Evidently report execution."""

    def run(self, reference_data: Any, target_data: Any) -> EvidentlyRunResult:
        """Run Evidently and return report JSON plus feature summaries."""


class DriftReportJsonWriter(Protocol):
    """Data-platform boundary for persisting full Evidently JSON content."""

    def write_report_json(self, report_id: str, payload: JsonObject) -> str:
        """Write report JSON and return the external analytical reference."""


class DriftReportStorage(Protocol):
    """Analytical storage boundary for drift report rows."""

    def append_drift_report(self, report: DriftReport) -> str:
        """Append a validated drift report and return its id."""


class InMemoryDriftInputGateway:
    """In-memory drift input gateway for tests and Lite workflows."""

    def __init__(self, windows: Mapping[str, Any]) -> None:
        self.windows = dict(windows)
        self.loaded_refs: list[str] = []

    def load_feature_window(self, ref: str) -> Any:
        self.loaded_refs.append(ref)
        try:
            return deepcopy(self.windows[ref])
        except KeyError as exc:
            raise DriftInputError(f"Missing drift feature window: {ref}") from exc


class InMemoryEvidentlyRunner:
    """In-memory Evidently runner for deterministic tests."""

    def __init__(self, result: EvidentlyRunResult) -> None:
        self.result = result
        self.calls: list[tuple[Any, Any]] = []

    def run(self, reference_data: Any, target_data: Any) -> EvidentlyRunResult:
        self.calls.append((deepcopy(reference_data), deepcopy(target_data)))
        return self.result


class InMemoryDriftReportJsonWriter:
    """In-memory report JSON writer for tests and Lite workflows."""

    def __init__(self, ref_prefix: str = "memory://drift-reports") -> None:
        self.ref_prefix = ref_prefix.rstrip("/")
        self.payloads: dict[str, JsonObject] = {}
        self.calls: list[tuple[str, JsonObject]] = []

    def write_report_json(self, report_id: str, payload: JsonObject) -> str:
        assert_no_forbidden_write(payload, path="$.evidently_json")
        payload_copy = deepcopy(payload)
        self.calls.append((report_id, payload_copy))
        self.payloads[report_id] = payload_copy
        return f"{self.ref_prefix}/{report_id}.json"


class InMemoryDriftReportStorage:
    """In-memory drift report analytical storage for tests and Lite workflows."""

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.reports: list[DriftReport] = []

    def append_drift_report(self, report: DriftReport) -> str:
        row = report.model_dump(mode="json")
        assert_no_forbidden_write(row, path="$.drift_report")
        self.rows.append(deepcopy(row))
        self.reports.append(report)
        return report.report_id


class EvidentlyReportRunner:
    """Lazy Evidently adapter for data drift reports."""

    def run(self, reference_data: Any, target_data: Any) -> EvidentlyRunResult:
        try:
            from evidently.metric_preset import DataDriftPreset
            from evidently.report import Report
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise DriftRunnerError(
                "Evidently is not importable; install project dependencies or "
                "pass evidently_runner=..."
            ) from exc

        report = Report(metrics=[DataDriftPreset()])
        try:
            report.run(reference_data=reference_data, current_data=target_data)
            payload = report.as_dict()
        except Exception as exc:  # pragma: no cover - Evidently integration
            raise DriftRunnerError(f"Evidently report execution failed: {exc}") from exc

        if not isinstance(payload, dict):
            raise DriftRunnerError("Evidently report payload must be an object")

        features, feature_count = _extract_evidently_feature_results(payload)
        return EvidentlyRunResult(
            evidently_json=payload,
            drifted_features=tuple(features),
            feature_count=feature_count,
        )


def get_default_input_gateway() -> DriftInputGateway:
    """Return configured drift input gateway, or fail closed."""

    raise DriftInputError(
        "No default drift input gateway is configured; pass input_gateway=..."
    )


def get_default_evidently_runner() -> EvidentlyRunner:
    """Return configured Evidently runner, or fail closed."""

    raise DriftRunnerError(
        "No default Evidently runner is configured; pass evidently_runner=..."
    )


def get_default_json_writer() -> DriftReportJsonWriter:
    """Return configured drift report JSON writer, or fail closed."""

    raise DriftStorageError(
        "No default drift report JSON writer is configured; pass json_writer=..."
    )


def get_default_report_storage() -> DriftReportStorage:
    """Return configured drift report storage, or fail closed."""

    raise DriftStorageError(
        "No default drift report storage is configured; pass storage=..."
    )


def _extract_evidently_feature_results(
    payload: JsonObject,
) -> tuple[list[DriftedFeature], int | None]:
    features: list[DriftedFeature] = []
    feature_count: int | None = None
    for mapping in _walk_mappings(payload):
        if feature_count is None:
            feature_count = _extract_feature_count(mapping)
        drift_by_columns = mapping.get("drift_by_columns")
        if isinstance(drift_by_columns, Mapping):
            for fallback_name, feature_payload in drift_by_columns.items():
                if not isinstance(feature_payload, Mapping):
                    continue
                features.append(
                    _feature_from_evidently_payload(
                        fallback_name=str(fallback_name),
                        payload=feature_payload,
                    )
                )
    if feature_count is None and features:
        feature_count = len(features)
    return features, feature_count


def _walk_mappings(payload: Any) -> Sequence[Mapping[str, Any]]:
    mappings: list[Mapping[str, Any]] = []
    if isinstance(payload, Mapping):
        mappings.append(payload)
        for value in payload.values():
            mappings.extend(_walk_mappings(value))
    elif isinstance(payload, list | tuple):
        for value in payload:
            mappings.extend(_walk_mappings(value))
    return mappings


def _extract_feature_count(payload: Mapping[str, Any]) -> int | None:
    for key in (
        "number_of_columns",
        "number_of_features",
        "total_feature_count",
        "features_count",
    ):
        value = payload.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value
    return None


def _feature_from_evidently_payload(
    *,
    fallback_name: str,
    payload: Mapping[str, Any],
) -> DriftedFeature:
    name = payload.get("column_name")
    if not isinstance(name, str) or not name:
        name = fallback_name
    score = _optional_float(
        payload.get("drift_score", payload.get("score", payload.get("statistic"))),
        default=0.0,
    )
    threshold = _optional_float(
        payload.get("stattest_threshold", payload.get("threshold")),
        default=0.0,
    )
    statistic = payload.get("statistic")
    return DriftedFeature(
        name=name,
        score=score,
        threshold=threshold,
        drifted=bool(payload.get("drift_detected", payload.get("drifted", False))),
        statistic=_optional_float(statistic) if statistic is not None else None,
        details=dict(payload),
    )


def _optional_float(value: Any, default: float | None = None) -> float:
    if value is None:
        if default is None:
            raise DriftRunnerError("Expected numeric Evidently metric value")
        return default
    if isinstance(value, bool) or not isinstance(value, Real):
        if default is None:
            raise DriftRunnerError("Expected numeric Evidently metric value")
        return default
    return float(value)


__all__ = [
    "DriftInputError",
    "DriftInputGateway",
    "DriftReportJsonWriter",
    "DriftReportStorage",
    "DriftRunnerError",
    "DriftStorageError",
    "EvidentlyReportRunner",
    "EvidentlyRunner",
    "InMemoryDriftInputGateway",
    "InMemoryDriftReportJsonWriter",
    "InMemoryDriftReportStorage",
    "InMemoryEvidentlyRunner",
    "get_default_evidently_runner",
    "get_default_input_gateway",
    "get_default_json_writer",
    "get_default_report_storage",
]
