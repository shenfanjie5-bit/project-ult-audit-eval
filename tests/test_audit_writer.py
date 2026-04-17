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
from audit_eval.audit.storage import FormalAuditStorageAdapter
from audit_eval.contracts import AuditRecord, AuditWriteBundle, ReplayRecord


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "audit_writer" / "sample_bundle.json"


def _sample_bundle_payload() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _sample_bundle() -> AuditWriteBundle:
    return AuditWriteBundle.model_validate(_sample_bundle_payload())


class CountingStorage(InMemoryFormalAuditStorageAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.audit_append_calls = 0
        self.replay_append_calls = 0

    def append_audit_records(self, records: Sequence[AuditRecord]) -> list[str]:
        self.audit_append_calls += 1
        return super().append_audit_records(records)

    def append_replay_records(self, records: Sequence[ReplayRecord]) -> list[str]:
        self.replay_append_calls += 1
        return super().append_replay_records(records)


class PartialFailure(RuntimeError):
    def __init__(self) -> None:
        self.partial_ids = ["audit-cycle_20260410-L4-world_state"]
        super().__init__("storage append failed")


class FailingAuditStorage:
    def append_audit_records(self, records: Sequence[AuditRecord]) -> list[str]:
        raise PartialFailure()

    def append_replay_records(self, records: Sequence[ReplayRecord]) -> list[str]:
        return [record.replay_id for record in records]


class FakeDuckDBConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    def executemany(self, sql: str, parameters: list[tuple[Any, ...]]) -> None:
        self.calls.append((sql, parameters))


def test_sample_bundle_fixture_does_not_include_forbidden_control_field() -> None:
    assert "feature_weight_multiplier" not in FIXTURE_PATH.read_text(encoding="utf-8")


def test_persist_audit_records_writes_serialized_rows() -> None:
    bundle = _sample_bundle()
    storage = InMemoryFormalAuditStorageAdapter()

    record_ids = persist_audit_records(bundle, storage)

    assert record_ids == [record.record_id for record in bundle.audit_records]
    assert len(storage.audit_rows) == 2
    first_row = storage.audit_rows[0]
    assert first_row["params_snapshot"] == {"market": "US", "as_of": "2026-04-10"}
    assert first_row["llm_lineage"]["called"] is True
    assert first_row["llm_cost"]["input_tokens"] == 120
    assert first_row["sanitized_input"]
    assert first_row["input_hash"]
    assert first_row["raw_output"]
    assert first_row["parsed_result"] == {
        "trend": "risk_off",
        "risk_level": "medium",
    }
    assert first_row["output_hash"]
    assert first_row["degradation_flags"] == {"degraded": False}


def test_persist_replay_records_writes_serialized_rows() -> None:
    bundle = _sample_bundle()
    storage = InMemoryFormalAuditStorageAdapter()

    replay_ids = persist_replay_records(bundle, storage)

    assert replay_ids == [record.replay_id for record in bundle.replay_records]
    assert len(storage.replay_rows) == 2
    second_row = storage.replay_rows[1]
    assert second_row["manifest_cycle_id"] == "cycle_20260410"
    assert second_row["formal_snapshot_refs"] == {
        "world_state": "snapshot://cycle_20260410/world_state",
        "recommendation": "snapshot://cycle_20260410/recommendation",
    }
    assert second_row["graph_snapshot_ref"] == "graph://cycle_20260410/portfolio_graph"
    assert second_row["dagster_run_id"] == "dagster-fixture-run-20260410"
    assert second_row["created_at"] == "2026-04-10T16:09:00Z"


def test_persist_audit_records_rejects_llm_record_missing_replay_field() -> None:
    bundle = _sample_bundle()
    bad_record = bundle.audit_records[0].model_copy(update={"sanitized_input": None})
    bad_bundle = bundle.model_copy(
        update={"audit_records": [bad_record, *bundle.audit_records[1:]]}
    )
    storage = CountingStorage()

    with pytest.raises(ValidationError, match="sanitized_input"):
        persist_audit_records(bad_bundle, storage)

    assert storage.audit_append_calls == 0
    assert storage.audit_rows == []


@pytest.mark.parametrize(
    ("record_field", "expected_path"),
    [
        (
            "params_snapshot",
            r"\$\.audit_records\[0\]\.params_snapshot\.feature_weight_multiplier",
        ),
        (
            "parsed_result",
            r"\$\.audit_records\[0\]\.parsed_result\.feature_weight_multiplier",
        ),
        (
            "degradation_flags",
            r"\$\.audit_records\[0\]\.degradation_flags\.feature_weight_multiplier",
        ),
    ],
)
def test_persist_audit_records_rejects_nested_forbidden_fields(
    record_field: str,
    expected_path: str,
) -> None:
    bundle = _sample_bundle()
    getattr(bundle.audit_records[0], record_field)["feature_weight_multiplier"] = 1
    storage = CountingStorage()

    with pytest.raises(BoundaryViolationError, match=expected_path):
        persist_audit_records(bundle, storage)

    assert storage.audit_append_calls == 0
    assert storage.audit_rows == []


def test_persist_audit_records_rejects_metadata_forbidden_field() -> None:
    bundle = _sample_bundle()
    bundle.metadata["nested"] = {"feature_weight_multiplier": 1}
    storage = CountingStorage()

    with pytest.raises(
        BoundaryViolationError,
        match=r"\$\.metadata\.nested\.feature_weight_multiplier",
    ):
        persist_audit_records(bundle, storage)

    assert storage.audit_append_calls == 0
    assert storage.audit_rows == []


@pytest.mark.parametrize(
    ("field_name", "value", "match"),
    [
        ("replay_mode", cast(Any, "rerun_model"), "replay_mode"),
        ("cycle_id", "cycle_other", "cycle_id"),
        ("manifest_cycle_id", "", "manifest_cycle_id"),
    ],
)
def test_persist_replay_records_rejects_invalid_bindings_without_append(
    field_name: str,
    value: object,
    match: str,
) -> None:
    bundle = _sample_bundle()
    bad_replay = bundle.replay_records[0].model_copy(update={field_name: value})
    bad_bundle = bundle.model_copy(
        update={"replay_records": [bad_replay, *bundle.replay_records[1:]]}
    )
    storage = CountingStorage()

    with pytest.raises(ValidationError, match=match):
        persist_replay_records(bad_bundle, storage)

    assert storage.replay_append_calls == 0
    assert storage.replay_rows == []


def test_persist_replay_records_rejects_manifest_mismatch_without_append() -> None:
    bundle = _sample_bundle()
    bad_replay = bundle.replay_records[0].model_copy(
        update={"manifest_cycle_id": "cycle_other"}
    )
    bad_bundle = bundle.model_copy(
        update={"replay_records": [bad_replay, *bundle.replay_records[1:]]}
    )
    storage = CountingStorage()

    with pytest.raises(ValidationError, match="manifest_cycle_id"):
        persist_replay_records(bad_bundle, storage)

    assert storage.replay_append_calls == 0
    assert storage.replay_rows == []


def test_persist_replay_records_rejects_missing_audit_record_ids_without_append() -> None:
    bundle = _sample_bundle()
    bad_replay = bundle.replay_records[0].model_copy(
        update={"audit_record_ids": ["audit-missing"]}
    )
    bad_bundle = bundle.model_copy(
        update={"replay_records": [bad_replay, *bundle.replay_records[1:]]}
    )
    storage = CountingStorage()

    with pytest.raises(ValidationError, match="audit-missing"):
        persist_replay_records(bad_bundle, storage)

    assert storage.replay_append_calls == 0
    assert storage.replay_rows == []


def test_adapter_failure_raises_audit_persistence_error() -> None:
    bundle = _sample_bundle()

    with pytest.raises(AuditPersistenceError) as exc_info:
        persist_audit_records(
            bundle,
            cast(FormalAuditStorageAdapter, FailingAuditStorage()),
        )

    assert exc_info.value.operation == "append_audit_records"
    assert exc_info.value.partial_ids == ["audit-cycle_20260410-L4-world_state"]
    assert "storage append failed" in str(exc_info.value)


def test_default_storage_fails_closed_without_explicit_configuration() -> None:
    with pytest.raises(AuditStorageError, match="No default formal audit storage"):
        get_default_storage_adapter()


def test_duckdb_adapter_only_appends_to_configured_tables() -> None:
    bundle = _sample_bundle()
    connection = FakeDuckDBConnection()
    adapter = DuckDBFormalAuditStorageAdapter(
        connection=connection,
        audit_table="formal.audit_record",
        replay_table="formal.replay_record",
    )

    audit_ids = adapter.append_audit_records(bundle.audit_records)
    replay_ids = adapter.append_replay_records(bundle.replay_records)

    assert audit_ids == [record.record_id for record in bundle.audit_records]
    assert replay_ids == [record.replay_id for record in bundle.replay_records]
    statements = [call[0] for call in connection.calls]
    assert statements[0].startswith('INSERT INTO "formal"."audit_record"')
    assert statements[1].startswith('INSERT INTO "formal"."replay_record"')
    statement_verbs = [
        statement.lstrip().split(maxsplit=1)[0].upper()
        for statement in statements
    ]
    assert statement_verbs == ["INSERT", "INSERT"]


@pytest.mark.parametrize(
    "table_name",
    [
        "formal.audit_record; DROP TABLE formal.replay_record",
        "formal.audit_record -- comment",
        "formal.audit_record/*comment*/",
        "formal audit_record",
        "formal..audit_record",
        ".formal",
        "formal.",
        "formal.audit-record",
        '"formal"."audit_record"',
        "formal.audit_record.extra.part",
    ],
)
def test_duckdb_adapter_rejects_suspicious_table_names(table_name: str) -> None:
    connection = FakeDuckDBConnection()

    with pytest.raises(AuditStorageError, match="SQL identifiers"):
        DuckDBFormalAuditStorageAdapter(
            connection=connection,
            audit_table=table_name,
            replay_table="formal.replay_record",
        )

    assert connection.calls == []
