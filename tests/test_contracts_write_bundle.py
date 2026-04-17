import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from audit_eval._boundary import BoundaryViolationError
from audit_eval.contracts import AuditRecord, AuditWriteBundle, ReplayRecord


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "spike" / "cycle_20260410"


def _fixture_payloads() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    audit_payloads = json.loads((FIXTURE_ROOT / "audit_records.json").read_text())
    replay_payloads = json.loads((FIXTURE_ROOT / "replay_records.json").read_text())
    return audit_payloads, replay_payloads


def _bundle_payload() -> dict[str, Any]:
    audit_payloads, replay_payloads = _fixture_payloads()
    return {
        "bundle_id": "bundle-cycle_20260410",
        "manifest_cycle_id": "cycle_20260410",
        "audit_records": audit_payloads,
        "replay_records": replay_payloads,
        "submitted_at": datetime(2026, 4, 10, 16, 11, tzinfo=timezone.utc),
    }


def test_write_bundle_accepts_fixture_payloads_and_builds_indexes() -> None:
    bundle = AuditWriteBundle.model_validate(_bundle_payload())

    assert bundle.formal_partition_tag == "formal"
    assert bundle.analytical_partition_tag is None
    assert bundle.metadata == {}
    assert set(bundle.audit_records_by_id()) == {
        "audit-cycle_20260410-L4-world_state",
        "audit-cycle_20260410-L7-recommendation",
    }
    assert set(bundle.replay_records_by_object_ref()) == {
        "world_state",
        "recommendation",
    }
    assert isinstance(
        bundle.audit_records_by_id()["audit-cycle_20260410-L4-world_state"],
        AuditRecord,
    )
    assert isinstance(
        bundle.replay_records_by_object_ref()["recommendation"],
        ReplayRecord,
    )


def test_write_bundle_rejects_missing_referenced_audit_record_id() -> None:
    payload = _bundle_payload()
    payload["replay_records"][1]["audit_record_ids"].append("audit-missing")

    with pytest.raises(ValidationError, match="audit-missing"):
        AuditWriteBundle.model_validate(payload)


def test_write_bundle_rejects_replay_manifest_mismatch() -> None:
    payload = _bundle_payload()
    payload["replay_records"][0]["manifest_cycle_id"] = "cycle_other"

    with pytest.raises(ValidationError, match="manifest_cycle_id"):
        AuditWriteBundle.model_validate(payload)


def test_write_bundle_rejects_forbidden_write_fields_recursively() -> None:
    payload = _bundle_payload()
    payload["metadata"] = {
        "review": {
            "feature_weight_multiplier": 1.2,
        },
    }

    with pytest.raises(
        BoundaryViolationError,
        match=r"\$\.metadata\.review\.feature_weight_multiplier",
    ):
        AuditWriteBundle.model_validate(payload)
