PYTHON ?= python3
PYTHONPATH ?= src
export PYTHONPATH
export PYTHONDONTWRITEBYTECODE ?= 1

.PHONY: install install-backtest test lint typecheck bytecode-clean backtest-smoke ci

install:
	$(PYTHON) -m pip install -e ".[dev]"

install-backtest:
	$(PYTHON) -m pip install -e ".[dev,backtest]"

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

typecheck:
	$(PYTHON) -m mypy src tests

bytecode-clean:
	test -z "$$(find . -path './.venv' -prune -o \( -path '*/__pycache__/*' -o -name '*.pyc' -o -name '*.pyo' \) -print -quit)"

backtest-smoke:
	AUDIT_EVAL_REQUIRE_ALPHALENS_SMOKE=1 $(PYTHON) -m pytest tests/test_alphalens_adapter.py::test_alphalens_adapter_smoke_with_installed_dependency

ci: lint typecheck test bytecode-clean
