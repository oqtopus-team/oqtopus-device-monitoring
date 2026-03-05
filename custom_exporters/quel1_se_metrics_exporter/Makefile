up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build --no-cache

fmt-lint:
	uv run ruff format
	uv run ruff check . --fix
	uv run mypy --config-file ./pyproject.toml

test:
	uv run pytest tests -vv --cov=src/ --cov-report=xml:coverage.xml --cov-report=html:htmlcov

clean:
	docker compose down --rmi local -v
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
