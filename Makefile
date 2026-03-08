GIT_ROOT ?= $(shell git rev-parse --show-toplevel)

help: ## Show all Makefile targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[33m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: sync format format-check check lint fix fix-unsafe type clean run-ci test

sync: ## Install/sync dependencies with uv
	@echo "(uv) Syncing dependencies..."
	@uv sync --group dev

format: ## Format code with ruff formatter
	@echo "(ruff) Formatting codebase..."
	@uv run ruff format src tests

format-check: ## Check formatting without modifying files
	@echo "(ruff) Verifying formatting..."
	@uv run ruff format --check src tests

check: ## Lint with ruff
	@echo "(ruff) Running checks..."
	@uv run ruff check src tests

lint: check ## Alias for check

fix: ## Auto-fix safe issues with ruff
	@echo "(ruff) Fixing issues..."
	@uv run ruff check src tests --fix

fix-unsafe: ## Auto-fix including unsafe fixes
	@echo "(ruff) Applying unsafe fixes..."
	@uv run ruff check src tests --fix --unsafe-fixes

type: ## Running type checker: ty
	@echo "(ty) Typechecking codebase..."
	@uv run ty check src

clean: ## Clean all generated files
	@echo "Cleaning all generated files..."
	@cd $(GIT_ROOT) || exit 1
	@find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete

run-ci: format-check check type test ## Running all CI checks

test: ## Run tests
	@echo "Running tests..."
	@uv run pytest tests $(shell if [ -n "$(k)" ]; then echo "-k $(k)"; fi)
