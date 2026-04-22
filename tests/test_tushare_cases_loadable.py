"""Tushare Phase B — loadability + invariant gate for the 2 tushare-derived shared cases.

Plan: ~/.claude/plans/wise-cooking-wolf.md §6.2.3.

Guards the 2 shared cases that Phase B cuts from the /Volumes/dockcase2tb
Tushare corpus:

    - minimal_cycle/case_tushare_one_stock_one_cycle
    - event_cases/case_tushare_namechange_alias

The tests assert each case:

1. Loads through ``audit_eval_fixtures.load_case(...)`` without raising.
2. Exposes all five payload sections as non-empty dicts (except
   ``manifest_refs`` for the namechange case, which uses the empty
   shape per plan §6.2.2 — that case does NOT drive a publish
   manifest, so its ``manifest_refs`` has an explicitly empty
   ``snapshot_ids`` list; we assert the ``manifest_refs`` dict itself
   is present and non-empty as a top-level contract).
3. Carries ``metadata.replay_mode == "read_history"`` (audit-eval
   CLAUDE.md C1 mandatory).
4. Carries a complete ``metadata.tushare_source`` block with the 8
   keys the Phase B traceability contract requires (plan §7), and
   ``completeness_status == "未见明显遗漏"``.
"""

from __future__ import annotations

import pytest

from audit_eval_fixtures import Case, load_case


_REQUIRED_TRACEABILITY_KEYS = frozenset(
    {
        "corpus_root",
        "dataset_path",
        "datasets",
        "selected_ts_codes",
        "date_window",
        "audit_timestamp",
        "completeness_status",
        "coverage_note",
    }
)


@pytest.fixture(scope="module")
def minimal_cycle_case() -> Case:
    return load_case("minimal_cycle", "case_tushare_one_stock_one_cycle")


@pytest.fixture(scope="module")
def namechange_case() -> Case:
    return load_case("event_cases", "case_tushare_namechange_alias")


class TestMinimalCycleTushareCaseLoadable:
    """minimal_cycle/case_tushare_one_stock_one_cycle."""

    def test_all_five_payloads_are_non_empty_dicts(
        self, minimal_cycle_case: Case
    ) -> None:
        for section in ("input", "context", "expected", "manifest_refs", "metadata"):
            payload = getattr(minimal_cycle_case, section)
            assert isinstance(payload, dict), (
                f"{section!r} must be a dict, got {type(payload).__name__}"
            )
            assert payload, f"{section!r} must be non-empty for this case"

    def test_metadata_replay_mode_is_read_history(
        self, minimal_cycle_case: Case
    ) -> None:
        """audit-eval CLAUDE.md C1: replay_mode must be read_history."""

        assert minimal_cycle_case.metadata["replay_mode"] == "read_history"

    def test_metadata_fixture_kind_is_minimal_cycle_baseline(
        self, minimal_cycle_case: Case
    ) -> None:
        assert (
            minimal_cycle_case.metadata["fixture_kind"]
            == "minimal_cycle_baseline"
        )

    def test_metadata_tushare_source_has_all_eight_traceability_keys(
        self, minimal_cycle_case: Case
    ) -> None:
        """Plan §7 traceability contract: every committed shared-case fixture
        must carry the 8-key tushare_source block so the source-to-case
        link is preserved through the repo."""

        source = minimal_cycle_case.metadata["tushare_source"]
        assert isinstance(source, dict)
        assert _REQUIRED_TRACEABILITY_KEYS.issubset(source.keys()), (
            f"missing traceability keys: "
            f"{_REQUIRED_TRACEABILITY_KEYS - source.keys()}"
        )
        assert source["completeness_status"] == "未见明显遗漏"

    def test_input_candidate_universe_matches_tushare_source_ts_code(
        self, minimal_cycle_case: Case
    ) -> None:
        """The case_tushare_one_stock_one_cycle candidate IS the tushare-cut
        stock — the input ts_code must match the metadata selected ts_code."""

        universe = minimal_cycle_case.input["candidate_universe"]
        assert len(universe) == 1
        candidate_ts = universe[0]["ts_code"]
        selected = minimal_cycle_case.metadata["tushare_source"][
            "selected_ts_codes"
        ]
        assert candidate_ts in selected, (
            f"candidate_universe ts_code {candidate_ts!r} not in "
            f"selected_ts_codes {selected!r}"
        )

    def test_manifest_cycle_id_matches_expected_publish_manifest(
        self, minimal_cycle_case: Case
    ) -> None:
        """Cross-file consistency: the manifest_cycle_id in metadata must
        equal the manifest_refs.cycle_publish_manifest_id, and the
        expected publish manifest's cycle_id must be derivable (no _v0
        suffix at shared-case level)."""

        metadata_manifest = minimal_cycle_case.metadata["manifest_cycle_id"]
        refs_manifest = minimal_cycle_case.manifest_refs[
            "cycle_publish_manifest_id"
        ]
        assert metadata_manifest == refs_manifest


class TestNamechangeAliasTushareCaseLoadable:
    """event_cases/case_tushare_namechange_alias."""

    def test_all_five_payloads_are_non_empty_dicts(
        self, namechange_case: Case
    ) -> None:
        for section in ("input", "context", "expected", "manifest_refs", "metadata"):
            payload = getattr(namechange_case, section)
            assert isinstance(payload, dict), (
                f"{section!r} must be a dict, got {type(payload).__name__}"
            )
            assert payload, f"{section!r} must be non-empty for this case"

    def test_metadata_replay_mode_is_read_history(
        self, namechange_case: Case
    ) -> None:
        """audit-eval CLAUDE.md C1: replay_mode must be read_history even
        for non-cycle event cases."""

        assert namechange_case.metadata["replay_mode"] == "read_history"

    def test_metadata_fixture_kind_is_namechange_alias_event(
        self, namechange_case: Case
    ) -> None:
        assert (
            namechange_case.metadata["fixture_kind"]
            == "namechange_alias_event"
        )

    def test_metadata_tushare_source_has_all_eight_traceability_keys(
        self, namechange_case: Case
    ) -> None:
        source = namechange_case.metadata["tushare_source"]
        assert isinstance(source, dict)
        assert _REQUIRED_TRACEABILITY_KEYS.issubset(source.keys()), (
            f"missing traceability keys: "
            f"{_REQUIRED_TRACEABILITY_KEYS - source.keys()}"
        )
        assert source["completeness_status"] == "未见明显遗漏"

    def test_manifest_refs_uses_minimal_non_publish_shape(
        self, namechange_case: Case
    ) -> None:
        """Plan §6.2.2: namechange case does NOT drive a publish manifest,
        so manifest_refs carries the explicitly empty shape."""

        refs = namechange_case.manifest_refs
        assert refs["cycle_publish_manifest_id"] is None
        assert refs["snapshot_ids"] == []
        assert refs["iceberg_namespace"] == "n/a"
        assert "entity-registry" in refs["consumer_modules"]

    def test_expected_proves_same_canonical_entity_id_across_three_mentions(
        self, namechange_case: Case
    ) -> None:
        """The core invariant of the namechange case: old-name + new-name
        + ts_code mentions all resolve to the same canonical entity id.
        Zero-tolerance per entity-registry CLAUDE.md."""

        expected_id = namechange_case.expected["expected_canonical_entity_id"]
        resolutions = namechange_case.expected["expected_resolutions"]
        assert len(resolutions) == 3
        for resolution in resolutions:
            assert resolution["canonical_entity_id"] == expected_id, (
                f"mention {resolution['mention_id']!r} resolves to "
                f"{resolution['canonical_entity_id']!r}, not the expected "
                f"{expected_id!r} — this would split the entity into two "
                f"canonical ids (entity-registry CLAUDE.md zero tolerance)"
            )

    def test_input_mention_samples_cover_old_new_and_ts_code(
        self, namechange_case: Case
    ) -> None:
        """Plan §6.2.2: at least 3 mentions — one with old name, one with
        new name, one with ts_code."""

        mentions = namechange_case.input["mention_samples"]
        kinds = {m["mention_kind"] for m in mentions}
        assert {"name_before", "name_after", "ts_code"}.issubset(kinds)

    def test_input_namechange_event_matches_metadata_traceability(
        self, namechange_case: Case
    ) -> None:
        """Cross-file consistency: the event's ts_code must be the one
        declared in metadata.tushare_source.selected_ts_codes."""

        event_ts = namechange_case.input["namechange_event"]["ts_code"]
        selected = namechange_case.metadata["tushare_source"][
            "selected_ts_codes"
        ]
        assert event_ts in selected
