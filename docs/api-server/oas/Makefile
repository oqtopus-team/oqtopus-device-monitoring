SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

.PHONY: lint generate help
IMAGE_REDOCLY ?= redocly/cli:2.12.0

lint:
	@docker run --rm -v $(PWD):/spec $(IMAGE_REDOCLY) lint openapi.yaml

generate:
	@docker pull $(IMAGE_REDOCLY)
	@docker run --rm -v $(PWD):/spec $(IMAGE_REDOCLY) bundle /spec/root.yaml -o /spec/openapi.yaml
	@$(MAKE) lint

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(filter-out .env,$(MAKEFILE_LIST)) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
