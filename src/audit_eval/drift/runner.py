"""Drift report orchestration entrypoints."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.contracts.common import JsonObject
from audit_eval.contracts.drift_report import DriftReport
from audit_eval.drift.rules import (
    DEFAULT_DRIFT_RULE_CONFIG,
    ALERT_RULES_VERSION,
    DriftRuleConfig,
    classify_regime_warning,
)
from audit_eval.drift.schema import DriftAlertPayload, DriftedFeature
from audit_eval.drift.storage import (
    DriftInputGateway,
    DriftReportJsonWriter,
    DriftReportStorage,
    DriftRunnerError,
    EvidentlyRunner,
    get_default_evidently_runner,
    get_default_input_gateway,
    get_default_json_writer,
    get_default_report_storage,
)


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
    """Generate and persist one analytical drift report."""

    gateway = input_gateway or get_default_input_gateway()
    runner = evidently_runner or get_default_evidently_runner()
    writer = json_writer or get_default_json_writer()
    report_storage = storage or get_default_report_storage()
    effective_created_at = created_at or datetime.now(timezone.utc)

    reference_data = gateway.load_feature_window(reference_ref)
    target_data = gateway.load_feature_window(target_ref)
    assert_no_forbidden_write(reference_data, path="$.reference_data")
    assert_no_forbidden_write(target_data, path="$.target_data")

    result = runner.run(reference_data, target_data)
    evidently_json = _require_json_object(result.evidently_json)
    feature_payloads = [
        _drifted_feature_payload(feature)
        for feature in result.drifted_features
    ]
    assert_no_forbidden_write(evidently_json, path="$.evidently_json")
    assert_no_forbidden_write(feature_payloads, path="$.drifted_features")

    rule_decision = classify_regime_warning(
        result,
        rules=rules or DEFAULT_DRIFT_RULE_CONFIG,
    )
    report_id = _drift_report_id(
        reference_ref=reference_ref,
        target_ref=target_ref,
        cycle_id=cycle_id,
        created_at=effective_created_at,
    )
    evidently_json_ref = writer.write_report_json(report_id, evidently_json)

    report = DriftReport(
        report_id=report_id,
        cycle_id=cycle_id,
        baseline_ref=reference_ref,
        target_ref=target_ref,
        evidently_json_ref=evidently_json_ref,
        drifted_features=feature_payloads,
        regime_warning_level=rule_decision.regime_warning_level,
        alert_rules_version=ALERT_RULES_VERSION,
        created_at=effective_created_at,
    )
    report_storage.append_drift_report(report)
    return report


def build_drift_alert_payload(report: DriftReport) -> DriftAlertPayload:
    """Build the structural warning payload consumed outside this package."""

    feature_names = tuple(
        feature["name"]
        for feature in report.drifted_features
        if feature.get("drifted") is True and isinstance(feature.get("name"), str)
    )
    return DriftAlertPayload(
        report_id=report.report_id,
        regime_warning_level=report.regime_warning_level,
        drifted_features=feature_names,
        evidently_json_ref=report.evidently_json_ref,
    )


def _require_json_object(payload: Any) -> JsonObject:
    if not isinstance(payload, dict):
        raise DriftRunnerError("Evidently result payload must be an object")
    return deepcopy(payload)


def _drifted_feature_payload(feature: DriftedFeature) -> JsonObject:
    payload = asdict(feature)
    if not isinstance(payload.get("details"), Mapping):
        payload["details"] = {}
    return payload


def _drift_report_id(
    *,
    reference_ref: str,
    target_ref: str,
    cycle_id: str | None,
    created_at: datetime,
) -> str:
    seed = "|".join(
        (
            cycle_id or "",
            reference_ref,
            target_ref,
            created_at.isoformat(),
        )
    )
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    return f"drift-{digest}"


__all__ = [
    "build_drift_alert_payload",
    "run_drift_report",
]
