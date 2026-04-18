"""Drift report orchestration."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime, timezone

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.contracts.common import JsonObject
from audit_eval.contracts.drift_report import (
    DriftReport,
    assert_no_drift_control_columns,
    assert_no_drift_control_write,
)
from audit_eval.drift.rules import (
    ALERT_RULES_VERSION,
    DEFAULT_DRIFT_RULE_CONFIG,
    DriftRuleConfig,
    classify_regime_warning,
)
from audit_eval.drift.schema import DriftAlertPayload, DriftedFeature
from audit_eval.drift.storage import (
    DriftInputGateway,
    DriftReportJsonWriter,
    DriftReportStorage,
    EvidentlyRunner,
    get_default_drift_report_storage,
    get_default_evidently_runner,
    get_default_input_gateway,
    get_default_json_writer,
)

_REPORT_ID_UNSAFE_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def run_drift_report(
    reference_ref: str,
    target_ref: str,
    *,
    cycle_id: str | None = None,
    input_gateway: DriftInputGateway | None = None,
    evidently_runner: EvidentlyRunner | None = None,
    json_writer: DriftReportJsonWriter | None = None,
    storage: DriftReportStorage | None = None,
    rules: DriftRuleConfig | None = None,
    created_at: datetime | None = None,
) -> DriftReport:
    """Generate, persist, and return one analytical drift report."""

    gateway = input_gateway or get_default_input_gateway()
    reference_data = gateway.load_feature_window(reference_ref)
    assert_no_forbidden_write(reference_data, path="$.reference_data")
    assert_no_drift_control_write(reference_data, path="$.reference_data")
    assert_no_drift_control_columns(reference_data, path="$.reference_data")

    target_data = gateway.load_feature_window(target_ref)
    assert_no_forbidden_write(target_data, path="$.target_data")
    assert_no_drift_control_write(target_data, path="$.target_data")
    assert_no_drift_control_columns(target_data, path="$.target_data")

    runner = evidently_runner or get_default_evidently_runner()
    result = runner.run(reference_data, target_data)

    assert_no_forbidden_write(result.report_json, path="$.evidently_json")
    assert_no_drift_control_write(result.report_json, path="$.evidently_json")
    result_features_payload = _drifted_features_payload(result.drifted_features)
    assert_no_forbidden_write(result_features_payload, path="$.drifted_features")
    assert_no_drift_control_write(result_features_payload, path="$.drifted_features")

    decision = classify_regime_warning(
        result,
        rules=rules or DEFAULT_DRIFT_RULE_CONFIG,
    )
    drifted_features = _drifted_features_payload(decision.drifted_features)
    assert_no_forbidden_write(drifted_features, path="$.drifted_features")
    assert_no_drift_control_write(drifted_features, path="$.drifted_features")

    effective_created_at = _normalize_created_at(created_at)
    report_id = _build_report_id(reference_ref, target_ref, effective_created_at)

    writer = json_writer or get_default_json_writer()
    evidently_json_ref = writer.write_report_json(report_id, result.report_json)

    report = DriftReport(
        report_id=report_id,
        cycle_id=cycle_id,
        baseline_ref=reference_ref,
        target_ref=target_ref,
        evidently_json_ref=evidently_json_ref,
        drifted_features=drifted_features,
        regime_warning_level=decision.regime_warning_level,
        alert_rules_version=ALERT_RULES_VERSION,
        created_at=effective_created_at,
    )

    report_storage = storage or get_default_drift_report_storage()
    report_storage.append_drift_report(report)
    return report


def build_drift_alert_payload(report: DriftReport) -> DriftAlertPayload:
    """Build a third-layer structural alert payload from a drift report."""

    feature_names = _extract_report_feature_names(report.drifted_features)
    assert_no_drift_control_write(
        {"features": [{"name": feature_name} for feature_name in feature_names]},
        path="$.drift_alert_payload.drifted_features",
    )
    payload = DriftAlertPayload(
        report_id=report.report_id,
        regime_warning_level=report.regime_warning_level,
        drifted_features=feature_names,
        evidently_json_ref=report.evidently_json_ref,
    )
    payload_dict = asdict(payload)
    assert_no_forbidden_write(payload_dict, path="$.drift_alert_payload")
    assert_no_drift_control_write(payload_dict, path="$.drift_alert_payload")
    return payload


def _drifted_features_payload(
    features: Sequence[DriftedFeature],
) -> JsonObject:
    return {"features": [asdict(feature) for feature in features]}


def _extract_report_feature_names(payload: JsonObject) -> tuple[str, ...]:
    names: list[str] = []
    features = payload.get("features")
    if isinstance(features, Sequence) and not isinstance(
        features,
        (str, bytes, bytearray),
    ):
        for feature in features:
            if isinstance(feature, Mapping):
                name = feature.get("name")
                if isinstance(name, str):
                    names.append(name)
    return tuple(dict.fromkeys(names))


def _build_report_id(
    reference_ref: str,
    target_ref: str,
    created_at: datetime,
) -> str:
    timestamp = created_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (
        "drift-"
        f"{_slug_ref(reference_ref)}-"
        f"{_slug_ref(target_ref)}-"
        f"{timestamp}"
    )


def _slug_ref(value: str) -> str:
    slug = _REPORT_ID_UNSAFE_RE.sub("-", value).strip("-")
    return slug or "ref"


def _normalize_created_at(created_at: datetime | None) -> datetime:
    value = created_at or datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


__all__ = [
    "build_drift_alert_payload",
    "run_drift_report",
]
