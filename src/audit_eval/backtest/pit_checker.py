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

_MANIFEST_CYCLE_ID_KEY = "manifest_cycle_id"
_MANIFEST_REF_KEY = "manifest_ref"
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


class PointInTimeManifestGateway(Protocol):
    """Authority boundary for PIT manifest snapshot sets."""

    def load_manifest_snapshot_refs(
        self,
        *,
        manifest_cycle_id: str | None = None,
        manifest_ref: str | None = None,
    ) -> object:
        """Return authoritative snapshot refs for a manifest cycle or ref."""


class PointInTimeChecker:
    """Validate feature availability before an offline backtest can run."""

    def __init__(
        self,
        gateway: PointInTimeFeatureGateway | None = None,
        manifest_gateway: PointInTimeManifestGateway | None = None,
    ) -> None:
        self.gateway = gateway
        self.manifest_gateway = manifest_gateway

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
        requested_manifest_snapshot_refs = _extract_requested_manifest_snapshot_refs(
            snapshot_range,
            violations,
        )
        manifest_lookup = _extract_manifest_lookup(snapshot_range, violations)

        if self.manifest_gateway is None:
            violations.append("No authoritative PIT manifest gateway configured")
        if self.gateway is None:
            violations.append("No point-in-time feature gateway configured")
            return PITCheckResult(passed=False, violations=tuple(violations))

        if (
            normalized_feature_ref is None
            or manifest_lookup is None
            or self.manifest_gateway is None
        ):
            return PITCheckResult(passed=False, violations=tuple(violations))

        manifest_cycle_id, manifest_ref = manifest_lookup
        try:
            raw_manifest_snapshot_refs = (
                self.manifest_gateway.load_manifest_snapshot_refs(
                    manifest_cycle_id=manifest_cycle_id,
                    manifest_ref=manifest_ref,
                )
            )
        except (BacktestInputError, KeyError, TypeError) as exc:
            violations.append(f"manifest snapshot refs unavailable: {exc}")
            return PITCheckResult(passed=False, violations=tuple(violations))

        manifest_snapshot_refs = _normalize_authoritative_manifest_snapshot_refs(
            raw_manifest_snapshot_refs,
            violations,
        )
        if not manifest_snapshot_refs:
            return PITCheckResult(passed=False, violations=tuple(violations))

        requested_snapshot_ref_violations = _validate_requested_snapshot_refs(
            requested_manifest_snapshot_refs,
            manifest_snapshot_refs,
        )
        violations.extend(requested_snapshot_ref_violations)
        if requested_snapshot_ref_violations:
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


class InMemoryPointInTimeManifestGateway:
    """In-memory authoritative PIT manifest gateway for tests and Lite workflows."""

    def __init__(
        self,
        snapshot_refs_by_manifest_cycle_id: Mapping[str, object] | None = None,
        snapshot_refs_by_manifest_ref: Mapping[str, object] | None = None,
    ) -> None:
        self.snapshot_refs_by_manifest_cycle_id = deepcopy(
            dict(snapshot_refs_by_manifest_cycle_id or {})
        )
        self.snapshot_refs_by_manifest_ref = deepcopy(
            dict(snapshot_refs_by_manifest_ref or {})
        )
        self.load_calls: list[tuple[str | None, str | None]] = []
        self._lock = Lock()

    def load_manifest_snapshot_refs(
        self,
        *,
        manifest_cycle_id: str | None = None,
        manifest_ref: str | None = None,
    ) -> object:
        with self._lock:
            self.load_calls.append((manifest_cycle_id, manifest_ref))
        if (
            manifest_ref is not None
            and manifest_ref in self.snapshot_refs_by_manifest_ref
        ):
            return deepcopy(self.snapshot_refs_by_manifest_ref[manifest_ref])
        if (
            manifest_cycle_id is not None
            and manifest_cycle_id in self.snapshot_refs_by_manifest_cycle_id
        ):
            return deepcopy(self.snapshot_refs_by_manifest_cycle_id[manifest_cycle_id])
        if manifest_ref is not None:
            raise BacktestInputError(
                f"Manifest snapshot refs not found for manifest_ref: {manifest_ref}"
            )
        raise BacktestInputError(
            "Manifest snapshot refs not found for manifest_cycle_id: "
            f"{manifest_cycle_id}"
        )


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
            f"{path}.snapshot_ref {snapshot_ref!r} is not returned by the "
            "authoritative PIT manifest gateway"
        )

    return tuple(violations)


def _validate_requested_snapshot_refs(
    requested_snapshot_refs: frozenset[str],
    authoritative_snapshot_refs: frozenset[str],
) -> tuple[str, ...]:
    violations: list[str] = []
    for snapshot_ref in sorted(requested_snapshot_refs - authoritative_snapshot_refs):
        violations.append(
            "$.formal_snapshot_range.manifest_snapshot_refs contains "
            f"{snapshot_ref!r}, which is not returned by the authoritative "
            "PIT manifest gateway"
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


def _extract_requested_manifest_snapshot_refs(
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
            "snapshot refs must be loaded through the authoritative PIT "
            "manifest gateway"
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
    if not refs:
        violations.append(
            "$.formal_snapshot_range.manifest_snapshot_refs must contain at "
            "least one snapshot ref"
        )
    return frozenset(refs)


def _extract_manifest_lookup(
    snapshot_range: JsonObject,
    violations: list[str],
) -> tuple[str | None, str | None] | None:
    has_manifest_cycle_id = _MANIFEST_CYCLE_ID_KEY in snapshot_range
    has_manifest_ref = _MANIFEST_REF_KEY in snapshot_range
    if not has_manifest_cycle_id and not has_manifest_ref:
        violations.append(
            "formal_snapshot_range must declare manifest_cycle_id or manifest_ref "
            "for authoritative PIT manifest lookup"
        )
        return None

    manifest_cycle_id = (
        _normalize_optional_string(
            snapshot_range.get(_MANIFEST_CYCLE_ID_KEY),
            field_name=f"$.formal_snapshot_range.{_MANIFEST_CYCLE_ID_KEY}",
            violations=violations,
        )
        if has_manifest_cycle_id
        else None
    )
    manifest_ref = (
        _normalize_optional_string(
            snapshot_range.get(_MANIFEST_REF_KEY),
            field_name=f"$.formal_snapshot_range.{_MANIFEST_REF_KEY}",
            violations=violations,
        )
        if has_manifest_ref
        else None
    )
    if manifest_cycle_id is None and manifest_ref is None:
        return None
    return manifest_cycle_id, manifest_ref


def _normalize_authoritative_manifest_snapshot_refs(
    raw_refs: object,
    violations: list[str],
) -> frozenset[str]:
    refs: set[str] = set()
    path = "$.authoritative_manifest_snapshot_refs"
    if isinstance(raw_refs, Mapping):
        items = tuple(raw_refs.items())
        if not items:
            violations.append(
                "authoritative PIT manifest gateway returned no snapshot refs"
            )
            return frozenset()
        for key, raw_ref in items:
            if not isinstance(raw_ref, str):
                violations.append(
                    f"{path}.{key} must be a snapshot ref string"
                )
                continue
            stripped = raw_ref.strip()
            if not stripped:
                violations.append(f"{path}.{key} must not be empty")
                continue
            refs.add(stripped)
        return frozenset(refs)

    if not _is_snapshot_ref_sequence(raw_refs):
        violations.append(
            "authoritative PIT manifest gateway must return a non-empty mapping "
            "or list of snapshot ref strings"
        )
        return frozenset()

    if not raw_refs:
        violations.append("authoritative PIT manifest gateway returned no snapshot refs")
        return frozenset()
    for index, raw_ref in enumerate(raw_refs):
        if not isinstance(raw_ref, str):
            violations.append(f"{path}[{index}] must be a snapshot ref string")
            continue
        stripped = raw_ref.strip()
        if not stripped:
            violations.append(f"{path}[{index}] must not be empty")
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
    "InMemoryPointInTimeManifestGateway",
    "InMemoryPointInTimeFeatureGateway",
    "PointInTimeChecker",
    "PointInTimeFeatureGateway",
    "PointInTimeManifestGateway",
    "get_default_pit_feature_gateway",
]
