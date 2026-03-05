SHELL := bash
SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

.PHONY: help up down


help:
	@echo "Usage:"
	@echo "  make up		# docker compose up -d"
	@echo "  make down		# docker compose down"

up:
	docker compose up -d

down:
	docker compose down
