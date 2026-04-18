"""Drift report orchestration entrypoints."""

from __future__ import annotations

import re
from dataclasses import asdict
from datetime import datetime, timezone

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.contracts.common import JsonObject
from audit_eval.contracts.drift_report import DriftReport
from audit_eval.drift.rules import (
    DEFAULT_DRIFT_RULE_CONFIG,
    DriftRuleConfig,
    DriftRuleDecision,
    classify_regime_warning,
)
from audit_eval.drift.schema import DriftAlertPayload, DriftedFeature
from audit_eval.drift.storage import (
    DriftInputGateway,
    DriftReportJsonWriter,
    DriftReportStorage,
    EvidentlyRunner,
    get_default_evidently_runner,
    get_default_input_gateway,
    get_default_json_writer,
    get_default_storage,
)

_REPORT_ID_TOKEN_RE = re.compile(r"[^A-Za-z0-9_.-]+")


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

    assert_no_forbidden_write(
        {
            "reference_ref": reference_ref,
            "target_ref": target_ref,
            "cycle_id": cycle_id,
        },
        path="$.drift_request",
    )

    gateway = input_gateway or get_default_input_gateway()
    runner = evidently_runner or get_default_evidently_runner()
    writer = json_writer or get_default_json_writer()
    report_storage = storage or get_default_storage()
    effective_rules = rules or DEFAULT_DRIFT_RULE_CONFIG
    effective_created_at = _normalize_created_at(created_at)

    reference_data = gateway.load_feature_window(reference_ref)
    assert_no_forbidden_write(reference_data, path="$.reference_data")
    target_data = gateway.load_feature_window(target_ref)
    assert_no_forbidden_write(target_data, path="$.target_data")

    result = runner.run(reference_data, target_data)
    assert_no_forbidden_write(result.report_json, path="$.evidently_json")

    decision = classify_regime_warning(result, rules=effective_rules)
    drifted_features_payload = _build_drifted_features_payload(decision)
    assert_no_forbidden_write(
        drifted_features_payload,
        path="$.drifted_features",
    )

    report_id = _build_report_id(reference_ref, target_ref, effective_created_at)
    evidently_json_ref = writer.write_report_json(report_id, result.report_json)

    report = DriftReport(
        report_id=report_id,
        cycle_id=cycle_id,
        baseline_ref=reference_ref,
        target_ref=target_ref,
        evidently_json_ref=evidently_json_ref,
        drifted_features=drifted_features_payload,
        regime_warning_level=decision.regime_warning_level,
        alert_rules_version=decision.alert_rules_version,
        created_at=effective_created_at,
    )
    report_storage.append_drift_report(report)
    return report


def build_drift_alert_payload(report: DriftReport) -> DriftAlertPayload:
    """Build the third-layer structural warning payload from a report."""

    report = DriftReport.model_validate(report.model_dump(mode="python"))
    payload = DriftAlertPayload(
        report_id=report.report_id,
        regime_warning_level=report.regime_warning_level,
        drifted_features=_extract_drifted_feature_names(report.drifted_features),
        evidently_json_ref=report.evidently_json_ref,
    )
    assert_no_forbidden_write(asdict(payload), path="$.drift_alert_payload")
    return payload


def _build_drifted_features_payload(decision: DriftRuleDecision) -> JsonObject:
    return {
        "features": [
            _drifted_feature_to_json(feature)
            for feature in decision.drifted_features
        ],
        "feature_count": decision.total_feature_count,
        "drifted_feature_count": decision.drifted_feature_count,
        "drifted_share": decision.drifted_share,
    }


def _drifted_feature_to_json(feature: DriftedFeature) -> JsonObject:
    payload: JsonObject = {
        "name": feature.name,
        "score": feature.score,
        "statistic": feature.statistic,
        "threshold": feature.threshold,
        "drifted": feature.drifted,
    }
    if feature.metadata:
        payload["metadata"] = dict(feature.metadata)
    return payload


def _extract_drifted_feature_names(payload: JsonObject) -> tuple[str, ...]:
    features = payload.get("features")
    if not isinstance(features, list):
        return ()
    names: list[str] = []
    for feature in features:
        if not isinstance(feature, dict) or feature.get("drifted") is not True:
            continue
        name = feature.get("name")
        if isinstance(name, str) and name:
            names.append(name)
    return tuple(names)


def _build_report_id(
    reference_ref: str,
    target_ref: str,
    created_at: datetime,
) -> str:
    timestamp = created_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return (
        "drift-"
        f"{_report_id_token(reference_ref)}-"
        f"{_report_id_token(target_ref)}-"
        f"{timestamp}"
    )


def _report_id_token(value: str) -> str:
    token = _REPORT_ID_TOKEN_RE.sub("-", value).strip("-")
    return token or "ref"


def _normalize_created_at(created_at: datetime | None) -> datetime:
    value = created_at or datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


__all__ = [
    "build_drift_alert_payload",
    "run_drift_report",
]
