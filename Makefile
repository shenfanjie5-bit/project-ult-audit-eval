PYTHON ?= python3
PYTHONPATH ?= src
export PYTHONPATH

.PHONY: install test lint typecheck ci

install:
	$(PYTHON) -m pip install -e ".[dev]"

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

typecheck:
	$(PYTHON) -m mypy src tests

ci: lint typecheck test
