SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

.PHONY: generate-api-server-schema build-image run fmt-lint test help


generate-api-server-schema:
	@echo "Generating API server schema..."
	@uv run datamodel-codegen \
		--input ../docs/api-server/oas/openapi.yaml \
		--output ./src/schemas/ \
		--use-schema-description \
		--target-python-version 3.13 \
		--field-constraints \
		--use-annotated \
		--use-field-description \
		--input-file-type openapi \
		--collapse-root-models \
		--enum-field-as-literal one \
		--output-datetime-class AwareDatetime \
		--use-union-operator \
		--use-subclass-enum \
		--custom-file-header-path "./datamodel_codegen/datamodel-codegen-template.j2" \
		--output-model-type pydantic_v2.BaseModel \
		--disable-timestamp \
		--use-standard-collections \
		--strict-nullable \
		--use-default \
		--formatters ruff-format ruff-check
	@echo "API server schema generated."

build-image:
	@echo "Building Docker image for API server..."
	docker compose build api-server
	@echo "Docker image built successfully."

up:
	@echo "Starting API server..."
	docker compose up -d api-server
	@echo "API server is up and running."

down:
	@echo "Stopping API server..."
	docker compose down api-server -v
	@echo "API server has been stopped."

run:
	@echo "Running API server..."
	docker compose up api-server
	@echo "API server is running."

stop:
	@echo "Stopping API server..."
	docker compose down api-server
	@echo "API server has been stopped."

fmt-lint:
	uv run ruff format
	uv run ruff check . --fix
	uv run mypy --config-file ./pyproject.toml

test:
	@echo "Running tests..."
	uv run pytest tests/
	@echo "Tests completed."
