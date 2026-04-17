import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from audit_eval._boundary import BoundaryViolationError
from audit_eval.audit import (
    AuditPersistenceError,
    AuditStorageError,
    DuckDBFormalAuditStorageAdapter,
    InMemoryFormalAuditStorageAdapter,
    get_default_storage_adapter,
    persist_audit_records,
    persist_replay_records,
)
from audit_eval.contracts import AuditRecord, AuditWriteBundle, ReplayRecord


FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "audit_writer" / "sample_bundle.json"
)


def _bundle_payload() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _bundle() -> AuditWriteBundle:
    return AuditWriteBundle.model_validate(_bundle_payload())


def test_persist_audit_records_writes_serialized_rows() -> None:
    bundle = _bundle()
    storage = InMemoryFormalAuditStorageAdapter()

    record_ids = persist_audit_records(bundle, storage)

    assert record_ids == [
        "audit-cycle_20260410-L4-world_state",
        "audit-cycle_20260410-L7-recommendation",
    ]
    assert [row["record_id"] for row in storage.audit_rows] == record_ids
    row = storage.audit_rows[0]
    for field_name in (
        "params_snapshot",
        "llm_lineage",
        "llm_cost",
        "sanitized_input",
        "input_hash",
        "raw_output",
        "parsed_result",
        "output_hash",
        "degradation_flags",
    ):
        assert field_name in row
    assert row["params_snapshot"] == {"market": "US", "as_of": "2026-04-10"}
    assert row["llm_lineage"]["called"] is True
    assert row["llm_cost"]["input_tokens"] == 120
    assert row["degradation_flags"] == {"degraded": False}


def test_persist_replay_records_writes_serialized_rows() -> None:
    bundle = _bundle()
    storage = InMemoryFormalAuditStorageAdapter()

    replay_ids = persist_replay_records(bundle, storage)

    assert replay_ids == [
        "replay-cycle_20260410-world_state",
        "replay-cycle_20260410-recommendation",
    ]
    assert [row["replay_id"] for row in storage.replay_rows] == replay_ids
    row = storage.replay_rows[1]
    assert row["manifest_cycle_id"] == "cycle_20260410"
    assert row["formal_snapshot_refs"] == {
        "world_state": "snapshot://cycle_20260410/world_state",
        "recommendation": "snapshot://cycle_20260410/recommendation",
    }
    assert row["graph_snapshot_ref"] == "graph://cycle_20260410/portfolio_graph"
    assert row["dagster_run_id"] == "dagster-fixture-run-20260410"
    assert row["created_at"] == "2026-04-10T16:09:00Z"


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda bundle: setattr(
                bundle.replay_records[0],
                "replay_mode",
                cast(Any, "rerun_model"),
            ),
            "replay_mode",
        ),
        (
            lambda bundle: setattr(
                bundle.replay_records[0],
                "manifest_cycle_id",
                "",
            ),
            "manifest_cycle_id",
        ),
    ],
)
def test_persist_replay_records_rejects_invalid_replay_before_adapter_call(
    mutate: Any,
    match: str,
) -> None:
    bundle = _bundle()
    storage = InMemoryFormalAuditStorageAdapter()
    mutate(bundle)

    with pytest.raises((ValidationError, ValueError), match=match):
        persist_replay_records(bundle, storage)

    assert storage.replay_rows == []


def test_persist_replay_records_rejects_manifest_mismatch_before_adapter_call() -> None:
    bundle = _bundle()
    storage = InMemoryFormalAuditStorageAdapter()
    bundle.replay_records[0].manifest_cycle_id = "cycle_other"

    with pytest.raises(ValidationError, match="manifest_cycle_id"):
        persist_replay_records(bundle, storage)

    assert storage.replay_rows == []


def test_persist_replay_records_rejects_missing_audit_id_before_adapter_call() -> None:
    bundle = _bundle()
    storage = InMemoryFormalAuditStorageAdapter()
    bundle.replay_records[0].audit_record_ids.append("audit-missing")

    with pytest.raises(ValidationError, match="audit-missing"):
        persist_replay_records(bundle, storage)

    assert storage.replay_rows == []


def test_persist_audit_records_rejects_missing_llm_replay_field_before_adapter() -> None:
    bundle = _bundle()
    storage = InMemoryFormalAuditStorageAdapter()
    bundle.audit_records[0].sanitized_input = None

    with pytest.raises(ValidationError, match="sanitized_input"):
        persist_audit_records(bundle, storage)

    assert storage.audit_rows == []


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda bundle: bundle.audit_records[0].params_snapshot.update(
                {"nested": {"feature_weight_multiplier": 1}}
            ),
            r"\$\.audit_records\[0\]\.params_snapshot\.nested\.feature_weight_multiplier",
        ),
        (
            lambda bundle: setattr(
                bundle.audit_records[0],
                "parsed_result",
                {"nested": {"feature_weight_multiplier": 1}},
            ),
            r"\$\.audit_records\[0\]\.parsed_result\.nested\.feature_weight_multiplier",
        ),
        (
            lambda bundle: bundle.audit_records[0].degradation_flags.update(
                {"nested": {"feature_weight_multiplier": 1}}
            ),
            r"\$\.audit_records\[0\]\.degradation_flags\.nested\.feature_weight_multiplier",
        ),
        (
            lambda bundle: bundle.metadata.update(
                {"nested": {"feature_weight_multiplier": 1}}
            ),
            r"\$\.metadata\.nested\.feature_weight_multiplier",
        ),
    ],
)
def test_persist_audit_records_rejects_recursive_forbidden_fields_before_adapter(
    mutate: Any,
    match: str,
) -> None:
    bundle = _bundle()
    storage = InMemoryFormalAuditStorageAdapter()
    mutate(bundle)

    with pytest.raises(BoundaryViolationError, match=match):
        persist_audit_records(bundle, storage)

    assert storage.audit_rows == []


def test_adapter_failure_raises_audit_persistence_error() -> None:
    class FailingStorage:
        def append_audit_records(self, records: Sequence[AuditRecord]) -> list[str]:
            raise RuntimeError("adapter down")

        def append_replay_records(self, records: Sequence[ReplayRecord]) -> list[str]:
            return [record.replay_id for record in records]

    with pytest.raises(AuditPersistenceError, match="adapter down") as exc_info:
        persist_audit_records(_bundle(), FailingStorage())

    assert exc_info.value.operation == "append_audit_records"
    assert exc_info.value.partial_ids == []
    assert isinstance(exc_info.value.__cause__, RuntimeError)


def test_default_storage_adapter_requires_explicit_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AUDIT_EVAL_DUCKDB_DATABASE", raising=False)
    monkeypatch.delenv("AUDIT_EVAL_AUDIT_TABLE", raising=False)
    monkeypatch.delenv("AUDIT_EVAL_REPLAY_TABLE", raising=False)

    with pytest.raises(AuditStorageError, match="No default audit storage adapter"):
        get_default_storage_adapter()


def test_duckdb_adapter_only_executes_insert_append() -> None:
    class FakeDuckDBConnection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[tuple[Any, ...]]]] = []

        def executemany(self, sql: str, values: list[tuple[Any, ...]]) -> None:
            self.calls.append((sql, values))

    bundle = _bundle()
    connection = FakeDuckDBConnection()
    adapter = DuckDBFormalAuditStorageAdapter(
        connection=connection,
        audit_table="formal.audit_record",
        replay_table="formal.replay_record",
    )

    record_ids = adapter.append_audit_records([bundle.audit_records[0]])

    assert record_ids == ["audit-cycle_20260410-L4-world_state"]
    assert len(connection.calls) == 1
    sql, values = connection.calls[0]
    assert sql.startswith('INSERT INTO "formal"."audit_record"')
    assert "CREATE TABLE" not in sql.upper()
    assert values[0][0] == "audit-cycle_20260410-L4-world_state"
