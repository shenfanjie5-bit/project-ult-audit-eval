"""Shared package boundary guards."""

FORBIDDEN_WRITE_FIELDS: frozenset[str] = frozenset({"feature_weight_multiplier"})


class BoundaryViolationError(Exception):
    """Raised when a payload attempts to write a forbidden field."""


def assert_no_forbidden_write(payload: dict[str, object]) -> None:
    """Reject payloads containing fields this package must never write."""
    forbidden_fields = FORBIDDEN_WRITE_FIELDS.intersection(payload)
    if forbidden_fields:
        fields = ", ".join(sorted(forbidden_fields))
        raise BoundaryViolationError(f"Forbidden write field(s): {fields}")


__all__ = [
    "BoundaryViolationError",
    "FORBIDDEN_WRITE_FIELDS",
    "assert_no_forbidden_write",
]
