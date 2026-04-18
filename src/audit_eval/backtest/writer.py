"""Validated backtest result persistence boundary."""

from __future__ import annotations

from audit_eval._boundary import BoundaryViolationError, assert_no_forbidden_write
from audit_eval.backtest.errors import BacktestStorageError
from audit_eval.backtest.storage import (
    BacktestResultStorage,
    get_default_backtest_result_storage,
)
from audit_eval.contracts.backtest_result import BacktestResult


def persist_backtest_result(
    result: BacktestResult,
    storage: BacktestResultStorage | None = None,
) -> str:
    """Persist one validated PIT-passing backtest result."""

    if result.pit_check_passed is not True:
        raise BacktestStorageError(
            "Refusing to persist backtest_result before PIT check passes"
        )

    try:
        row = result.model_dump(mode="python")
        assert_no_forbidden_write(row, path="$.backtest_result")
    except BoundaryViolationError as exc:
        raise BacktestStorageError("Backtest result contains forbidden fields") from exc

    result_storage = storage or get_default_backtest_result_storage()
    try:
        return result_storage.append_backtest_result(result)
    except BacktestStorageError:
        raise
    except Exception as exc:
        raise BacktestStorageError("Backtest result storage append failed") from exc


__all__ = ["persist_backtest_result"]
