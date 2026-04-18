"""Point-in-time feature availability checks for offline backtests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import datetime
from threading import Lock
from typing import Protocol, TypeGuard

from audit_eval._boundary import assert_no_forbidden_write
from audit_eval.backtest.errors import BacktestInputError
from audit_eval.backtest.schema import FeatureAvailability, PITCheckResult
from audit_eval.contracts.common import JsonObject

_MANIFEST_SNAPSHOT_REFS_KEY = "manifest_snapshot_refs"
_SNAPSHOT_COLLECTION_KEYS: frozenset[str] = frozenset(
    {
        _MANIFEST_SNAPSHOT_REFS_KEY,
        "manifest_snapshot_set",
        "formal_snapshot_refs",
        "snapshot_refs",
        "snapshot_refs_by_object",
        "snapshot_set",
        "snapshots",
    }
)


class PointInTimeFeatureGateway(Protocol):
    """Input boundary for manifest-bound historical feature availability."""

    def load_feature_availability(
        self,
        feature_ref: str,
        snapshot_range: JsonObject,
    ) -> Sequence[FeatureAvailability]:
        """Return point-in-time availability rows for feature_ref and range."""


class PointInTimeChecker:
    """Validate feature availability before an offline backtest can run."""

    def __init__(self, gateway: PointInTimeFeatureGateway | None = None) -> None:
        self.gateway = gateway

    def validate(self, feature_ref: str, snapshot_range: JsonObject) -> PITCheckResult:
        """Return fail-closed PIT validation result for one feature/range pair."""

        violations: list[str] = []
        normalized_feature_ref = _normalize_optional_string(
            feature_ref,
            field_name="feature_ref",
            violations=violations,
        )
        if not isinstance(snapshot_range, dict):
            violations.append("formal_snapshot_range must be a JSON object")
            return PITCheckResult(passed=False, violations=tuple(violations))

        assert_no_forbidden_write(snapshot_range, path="$.formal_snapshot_range")
        manifest_snapshot_refs = _extract_manifest_snapshot_refs(
            snapshot_range,
            violations,
        )
        if not manifest_snapshot_refs:
            violations.append(
                "formal_snapshot_range must declare manifest-bound snapshot refs"
            )

        if self.gateway is None:
            violations.append("No point-in-time feature gateway configured")
            return PITCheckResult(passed=False, violations=tuple(violations))

        if normalized_feature_ref is None:
            return PITCheckResult(passed=False, violations=tuple(violations))

        try:
            availability_rows = tuple(
                self.gateway.load_feature_availability(
                    normalized_feature_ref,
                    deepcopy(snapshot_range),
                )
            )
        except (BacktestInputError, KeyError, TypeError) as exc:
            violations.append(f"feature availability unavailable: {exc}")
            return PITCheckResult(passed=False, violations=tuple(violations))

        if not availability_rows:
            violations.append(
                f"No feature availability rows for feature_ref "
                f"{normalized_feature_ref!r}"
            )
            return PITCheckResult(passed=False, violations=tuple(violations))

        for index, availability in enumerate(availability_rows):
            violations.extend(
                _validate_availability_row(
                    availability,
                    index=index,
                    expected_feature_ref=normalized_feature_ref,
                    manifest_snapshot_refs=manifest_snapshot_refs,
                )
            )

        return PITCheckResult(passed=not violations, violations=tuple(violations))


class InMemoryPointInTimeFeatureGateway:
    """In-memory PIT availability gateway for tests and Lite workflows."""

    def __init__(
        self,
        availability_by_feature_ref: Mapping[
            str,
            Sequence[FeatureAvailability],
        ],
    ) -> None:
        self.availability_by_feature_ref = {
            feature_ref: tuple(rows)
            for feature_ref, rows in availability_by_feature_ref.items()
        }
        self.load_calls: list[tuple[str, JsonObject]] = []
        self._lock = Lock()

    def load_feature_availability(
        self,
        feature_ref: str,
        snapshot_range: JsonObject,
    ) -> Sequence[FeatureAvailability]:
        assert_no_forbidden_write(snapshot_range, path="$.formal_snapshot_range")
        with self._lock:
            self.load_calls.append((feature_ref, deepcopy(snapshot_range)))
        try:
            return tuple(deepcopy(self.availability_by_feature_ref[feature_ref]))
        except KeyError as exc:
            raise BacktestInputError(
                f"Feature availability not found for feature_ref: {feature_ref}"
            ) from exc


def get_default_pit_feature_gateway() -> PointInTimeFeatureGateway:
    """Return configured PIT feature gateway, or fail closed."""

    raise BacktestInputError(
        "No default point-in-time feature gateway is configured; "
        "pass feature_gateway=..."
    )


def _validate_availability_row(
    availability: object,
    *,
    index: int,
    expected_feature_ref: str,
    manifest_snapshot_refs: frozenset[str],
) -> tuple[str, ...]:
    path = f"$.availability[{index}]"
    _assert_no_forbidden_availability_write(availability, path=path)
    violations: list[str] = []

    row_feature_ref = _get_availability_value(availability, "feature_ref")
    normalized_row_feature_ref = _normalize_optional_string(
        row_feature_ref,
        field_name=f"{path}.feature_ref",
        violations=violations,
    )
    if (
        normalized_row_feature_ref is not None
        and normalized_row_feature_ref != expected_feature_ref
    ):
        violations.append(
            f"{path}.feature_ref {normalized_row_feature_ref!r} does not match "
            f"requested feature_ref {expected_feature_ref!r}"
        )

    as_of = _get_availability_value(availability, "as_of")
    available_at = _get_availability_value(availability, "available_at")
    snapshot_ref = _normalize_optional_string(
        _get_availability_value(availability, "snapshot_ref"),
        field_name=f"{path}.snapshot_ref",
        violations=violations,
    )

    if not isinstance(as_of, datetime):
        violations.append(f"{path}.as_of binding is missing or not a datetime")
    if not isinstance(available_at, datetime):
        violations.append(
            f"{path}.available_at binding is missing or not a datetime"
        )
    if snapshot_ref is None:
        violations.append(f"{path}.snapshot_ref binding is missing")

    if isinstance(as_of, datetime) and isinstance(available_at, datetime):
        try:
            if available_at > as_of:
                violations.append(
                    f"{path} has look-ahead bias: available_at "
                    f"{available_at.isoformat()} is after as_of "
                    f"{as_of.isoformat()}"
                )
        except TypeError:
            violations.append(
                f"{path}.available_at and {path}.as_of must be comparable datetimes"
            )

    if snapshot_ref is not None and snapshot_ref not in manifest_snapshot_refs:
        violations.append(
            f"{path}.snapshot_ref {snapshot_ref!r} is not declared in "
            "manifest-bound formal_snapshot_range"
        )

    return tuple(violations)


def _assert_no_forbidden_availability_write(
    availability: object,
    *,
    path: str,
) -> None:
    if isinstance(availability, Mapping):
        assert_no_forbidden_write(availability, path=path)
        return
    metadata = getattr(availability, "metadata", None)
    if metadata is not None:
        assert_no_forbidden_write(metadata, path=f"{path}.metadata")


def _get_availability_value(availability: object, field_name: str) -> object:
    if isinstance(availability, Mapping):
        return availability.get(field_name)
    return getattr(availability, field_name, None)


def _normalize_optional_string(
    value: object,
    *,
    field_name: str,
    violations: list[str],
) -> str | None:
    if not isinstance(value, str):
        violations.append(f"{field_name} must be a string")
        return None
    stripped = value.strip()
    if not stripped:
        violations.append(f"{field_name} must not be empty")
        return None
    return stripped


def _extract_manifest_snapshot_refs(
    snapshot_range: JsonObject,
    violations: list[str],
) -> frozenset[str]:
    for path in _iter_unsupported_snapshot_collection_paths(
        snapshot_range,
        path="$.formal_snapshot_range",
        root=True,
    ):
        violations.append(
            f"{path} is not an authoritative manifest snapshot field; "
            "use $.formal_snapshot_range.manifest_snapshot_refs"
        )

    raw_refs = snapshot_range.get(_MANIFEST_SNAPSHOT_REFS_KEY)
    if raw_refs is None:
        return frozenset()
    if not _is_snapshot_ref_sequence(raw_refs):
        violations.append(
            "$.formal_snapshot_range.manifest_snapshot_refs must be a "
            "non-empty list of snapshot ref strings"
        )
        return frozenset()

    refs: set[str] = set()
    for index, raw_ref in enumerate(raw_refs):
        if not isinstance(raw_ref, str):
            violations.append(
                "$.formal_snapshot_range.manifest_snapshot_refs"
                f"[{index}] must be a string"
            )
            continue
        stripped = raw_ref.strip()
        if not stripped:
            violations.append(
                "$.formal_snapshot_range.manifest_snapshot_refs"
                f"[{index}] must not be empty"
            )
            continue
        refs.add(stripped)
    return frozenset(refs)


def _is_snapshot_ref_sequence(value: object) -> TypeGuard[Sequence[object]]:
    return isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    )


def _iter_unsupported_snapshot_collection_paths(
    value: object,
    *,
    path: str,
    root: bool = False,
) -> tuple[str, ...]:
    if isinstance(value, Mapping):
        paths: list[str] = []
        for key, nested_value in value.items():
            nested_path = f"{path}.{key}"
            is_allowed_manifest_field = (
                root
                and isinstance(key, str)
                and key == _MANIFEST_SNAPSHOT_REFS_KEY
            )
            if (
                isinstance(key, str)
                and key in _SNAPSHOT_COLLECTION_KEYS
                and not is_allowed_manifest_field
            ):
                paths.append(nested_path)
            paths.extend(
                _iter_unsupported_snapshot_collection_paths(
                    nested_value,
                    path=nested_path,
                )
            )
        return tuple(paths)

    if _is_snapshot_ref_sequence(value):
        paths = []
        for index, nested_value in enumerate(value):
            paths.extend(
                _iter_unsupported_snapshot_collection_paths(
                    nested_value,
                    path=f"{path}[{index}]",
                )
            )
        return tuple(paths)

    return ()


__all__ = [
    "InMemoryPointInTimeFeatureGateway",
    "PointInTimeChecker",
    "PointInTimeFeatureGateway",
    "get_default_pit_feature_gateway",
]
