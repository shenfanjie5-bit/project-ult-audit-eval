PYTHON ?= python3
PYTHONPATH ?= src
export PYTHONPATH
export PYTHONDONTWRITEBYTECODE ?= 1

.PHONY: install test lint typecheck bytecode-clean ci

install:
	$(PYTHON) -m pip install -e ".[dev]"

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

typecheck:
	$(PYTHON) -m mypy src tests

bytecode-clean:
	test -z "$$(find . -path './.venv' -prune -o \( -path '*/__pycache__/*' -o -name '*.pyc' -o -name '*.pyo' \) -print -quit)"

ci: lint typecheck test bytecode-clean
