"""Public error taxonomy for backtest inputs and PIT validation."""

from __future__ import annotations

from audit_eval.backtest.schema import PITCheckResult


class BacktestInputError(RuntimeError):
    """Raised when backtest input data is unavailable or invalid."""


class PITViolationError(BacktestInputError):
    """Raised when point-in-time checks fail before a backtest result is published."""

    def __init__(
        self,
        result: PITCheckResult | None = None,
        message: str | None = None,
    ) -> None:
        self.result = result
        if message is None:
            message = _format_pit_violation_message(result)
        super().__init__(message)


def _format_pit_violation_message(result: PITCheckResult | None) -> str:
    if result is None:
        return "PIT check failed"
    violation_count = len(result.violations)
    first_reason = result.violations[0] if result.violations else "unknown reason"
    return (
        f"PIT check failed with {violation_count} violation(s); "
        f"first: {first_reason}"
    )


__all__ = [
    "BacktestInputError",
    "PITViolationError",
]
