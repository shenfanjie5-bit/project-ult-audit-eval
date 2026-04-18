from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import pytest

from audit_eval._boundary import BoundaryViolationError
from audit_eval.backtest import (
    BacktestInputError,
    FeatureAvailability,
    InMemoryPointInTimeFeatureGateway,
    PITCheckResult,
    PITViolationError,
    PointInTimeChecker,
    PointInTimeFeatureGateway,
    get_default_pit_feature_gateway,
)


def _snapshot_range() -> dict[str, object]:
    return {
        "manifest_cycle_id": "cycle_20260418",
        "manifest_snapshot_refs": [
            "snapshot://features/20260417",
            "snapshot://features/20260418",
        ],
    }


def _availability(
    *,
    feature_ref: str = "feature://momentum/v1",
    as_of: datetime = datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
    available_at: datetime = datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
    snapshot_ref: str = "snapshot://features/20260418",
) -> FeatureAvailability:
    return FeatureAvailability(
        feature_ref=feature_ref,
        as_of=as_of,
        available_at=available_at,
        snapshot_ref=snapshot_ref,
    )


def test_backtest_package_exports_pit_checker() -> None:
    from audit_eval.backtest import PointInTimeChecker as ExportedChecker

    assert ExportedChecker is PointInTimeChecker


def test_protocol_export_is_importable() -> None:
    assert PointInTimeFeatureGateway is not None


def test_validate_passes_for_point_in_time_manifest_bound_features() -> None:
    gateway = InMemoryPointInTimeFeatureGateway(
        {
            "feature://momentum/v1": [
                _availability(snapshot_ref="snapshot://features/20260417"),
                _availability(snapshot_ref="snapshot://features/20260418"),
            ]
        }
    )
    checker = PointInTimeChecker(gateway)

    result = checker.validate(" feature://momentum/v1 ", _snapshot_range())

    assert result == PITCheckResult(passed=True)
    assert gateway.load_calls == [("feature://momentum/v1", _snapshot_range())]


def test_validate_fails_closed_without_gateway() -> None:
    result = PointInTimeChecker().validate(
        "feature://momentum/v1",
        _snapshot_range(),
    )

    assert result.passed is False
    assert result.violations == ("No point-in-time feature gateway configured",)


def test_default_gateway_fails_closed_with_backtest_input_error() -> None:
    with pytest.raises(BacktestInputError, match="No default point-in-time"):
        get_default_pit_feature_gateway()


def test_validate_fails_closed_without_availability_rows() -> None:
    checker = PointInTimeChecker(
        InMemoryPointInTimeFeatureGateway({"feature://momentum/v1": []})
    )

    result = checker.validate("feature://momentum/v1", _snapshot_range())

    assert result.passed is False
    assert "No feature availability rows" in result.violations[0]


def test_validate_fails_closed_for_gateway_input_error() -> None:
    checker = PointInTimeChecker(InMemoryPointInTimeFeatureGateway({}))

    result = checker.validate("feature://missing/v1", _snapshot_range())

    assert result.passed is False
    assert "feature availability unavailable" in result.violations[0]
    assert "Feature availability not found" in result.violations[0]


def test_validate_blocks_look_ahead_bias() -> None:
    checker = PointInTimeChecker(
        InMemoryPointInTimeFeatureGateway(
            {
                "feature://momentum/v1": [
                    _availability(
                        as_of=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
                        available_at=datetime(
                            2026,
                            4,
                            18,
                            10,
                            0,
                            tzinfo=timezone.utc,
                        ),
                    )
                ]
            }
        )
    )

    result = checker.validate("feature://momentum/v1", _snapshot_range())

    assert result.passed is False
    assert any("look-ahead bias" in violation for violation in result.violations)
    assert any("available_at" in violation for violation in result.violations)


def test_validate_blocks_snapshot_ref_outside_manifest_bound_range() -> None:
    checker = PointInTimeChecker(
        InMemoryPointInTimeFeatureGateway(
            {
                "feature://momentum/v1": [
                    _availability(snapshot_ref="snapshot://head/latest")
                ]
            }
        )
    )

    result = checker.validate("feature://momentum/v1", _snapshot_range())

    assert result.passed is False
    assert any(
        "not declared in manifest-bound formal_snapshot_range" in violation
        for violation in result.violations
    )


def test_validate_fails_closed_when_snapshot_range_has_no_manifest_set() -> None:
    checker = PointInTimeChecker(
        InMemoryPointInTimeFeatureGateway(
            {"feature://momentum/v1": [_availability()]}
        )
    )

    result = checker.validate(
        "feature://momentum/v1",
        {"start": "2026-04-01", "end": "2026-04-18"},
    )

    assert result.passed is False
    assert result.violations[0] == (
        "formal_snapshot_range must declare manifest-bound snapshot refs"
    )


def test_validate_fails_closed_for_missing_availability_bindings() -> None:
    class _MalformedGateway:
        def load_feature_availability(
            self,
            feature_ref: str,
            snapshot_range: dict[str, object],
        ) -> list[dict[str, object]]:
            return [
                {
                    "feature_ref": feature_ref,
                    "as_of": None,
                    "available_at": None,
                    "snapshot_ref": "   ",
                }
            ]

    result = PointInTimeChecker(cast(Any, _MalformedGateway())).validate(
        "feature://momentum/v1",
        _snapshot_range(),
    )

    assert result.passed is False
    assert any("as_of binding is missing" in item for item in result.violations)
    assert any(
        "available_at binding is missing" in item for item in result.violations
    )
    assert any("snapshot_ref binding is missing" in item for item in result.violations)


def test_validate_rejects_forbidden_field_in_snapshot_range() -> None:
    checker = PointInTimeChecker(
        InMemoryPointInTimeFeatureGateway(
            {"feature://momentum/v1": [_availability()]}
        )
    )

    with pytest.raises(
        BoundaryViolationError,
        match=r"\$\.formal_snapshot_range\.nested\.feature_weight_multiplier",
    ):
        checker.validate(
            "feature://momentum/v1",
            {"nested": {"feature_weight_multiplier": 1.2}},
        )


def test_validate_rejects_forbidden_field_in_availability_row() -> None:
    class _ForbiddenGateway:
        def load_feature_availability(
            self,
            feature_ref: str,
            snapshot_range: dict[str, object],
        ) -> list[dict[str, object]]:
            return [
                {
                    "feature_ref": feature_ref,
                    "as_of": datetime(2026, 4, 18, tzinfo=timezone.utc),
                    "available_at": datetime(2026, 4, 17, tzinfo=timezone.utc),
                    "snapshot_ref": "snapshot://features/20260418",
                    "metadata": {"feature_weight_multiplier": 1.2},
                }
            ]

    with pytest.raises(
        BoundaryViolationError,
        match=r"\$\.availability\[0\]\.metadata\.feature_weight_multiplier",
    ):
        PointInTimeChecker(cast(Any, _ForbiddenGateway())).validate(
            "feature://momentum/v1",
            _snapshot_range(),
        )


def test_pit_violation_error_includes_count_and_first_reason() -> None:
    result = PITCheckResult(
        passed=False,
        violations=("look-ahead bias in row 0", "snapshot mismatch in row 1"),
    )

    error = PITViolationError(result)

    assert error.result is result
    assert "2 violation(s)" in str(error)
    assert "look-ahead bias in row 0" in str(error)


def test_pit_result_rejects_passed_with_violations() -> None:
    with pytest.raises(ValueError, match="cannot be true with violations"):
        PITCheckResult(passed=True, violations=("violation",))


def test_backtest_package_does_not_import_provider_or_http_clients() -> None:
    backtest_dir = Path(__file__).resolve().parents[1] / "src/audit_eval/backtest"
    forbidden_terms = ("openai", "anthropic", "requests", "httpx")

    source = "\n".join(path.read_text() for path in backtest_dir.rglob("*.py"))

    for term in forbidden_terms:
        assert term not in source
