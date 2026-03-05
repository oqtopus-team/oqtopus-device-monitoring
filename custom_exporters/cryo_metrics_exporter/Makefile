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
	@export POWERTOOLS_METRICS_NAMESPACE=worker && \
	export POWERTOOLS_SERVICE_NAME=worker && \
	uv run pytest tests -vv --cov=src/ --cov-report=xml:coverage-worker.xml --cov-report=html:htmlcov-worker
	@mv .coverage .coverage-worker
