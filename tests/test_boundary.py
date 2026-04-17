import pytest

from audit_eval._boundary import BoundaryViolationError, assert_no_forbidden_write


def test_boundary_forbids_top_level_field() -> None:
    with pytest.raises(BoundaryViolationError, match=r"\$\.feature_weight_multiplier"):
        assert_no_forbidden_write({"feature_weight_multiplier": 1})


def test_boundary_forbids_nested_mapping_field_with_path() -> None:
    with pytest.raises(
        BoundaryViolationError,
        match=r"\$\.nested\.feature_weight_multiplier",
    ):
        assert_no_forbidden_write({"nested": {"feature_weight_multiplier": 1}})


def test_boundary_forbids_sequence_field_with_index_path() -> None:
    with pytest.raises(
        BoundaryViolationError,
        match=r"\$\[0\]\.safe\.feature_weight_multiplier",
    ):
        assert_no_forbidden_write([{"safe": {"feature_weight_multiplier": 1}}])


def test_boundary_allows_safe_nested_payloads() -> None:
    assert_no_forbidden_write(
        {
            "nested": [{"alert_score": 2}],
            "metadata": {"review": "safe"},
        }
    )
