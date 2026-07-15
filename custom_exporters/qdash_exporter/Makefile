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
	uv run pytest tests -vv --cov=src/ --cov-report=xml --cov-report=html
