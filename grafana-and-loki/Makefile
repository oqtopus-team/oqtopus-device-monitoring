SHELL := bash
SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

.PHONY: help run down

COMPOSE_CMD = docker compose

MOUNT_DIR := /etc/grafana/dashboards
TARGET_DIR := /var/lib/grafana/dashboards
CONTAINER_NAME := grafana

# get IP address of the default network interface by ip command
DEV := $(shell ip route show default | awk '/default/ {print $$5}')
IP_ADDR := $(shell ip -4 addr show "$(DEV)" | awk '/inet/ {sub(/\/.*/, "", $$2); print $$2}')

help: ## show this help message
	@echo "Usage:"
	@echo "  make up		# docker compose up -d"
	@echo "  make down		# docker compose down"
	@echo "$(IP_ADDR)"

up: ## replace IP address and docker compose up -d
	@$(COMPOSE_CMD) up -d
	@docker exec $(CONTAINER_NAME) bash -lc '\
		set -eu; \
		mkdir -p $(TARGET_DIR); \
		cp $(MOUNT_DIR)/*.json $(TARGET_DIR)/; \
		for file in $(TARGET_DIR)/*.json; do \
			sed -E "s|(http://)[^:/]+(:3000/public/img)|\1$(IP_ADDR)\2|g" $$file > $$file.tmp && mv $$file.tmp $$file; \
		done; \
	'
	@echo "Updated dashboard JSON files with current IP address $(IP_ADDR)."

down: ## docker compose down
	@$(COMPOSE_CMD) down
