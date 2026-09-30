SHELL := /bin/bash
COMPOSE ?= docker compose
PY ?= python3

.DEFAULT_GOAL := help

help: ## show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- development
venv: ## create .venv with dev dependencies
	$(PY) -m venv .venv && .venv/bin/pip install -U pip && .venv/bin/pip install -r requirements-dev.txt

test: ## run unit + local end-to-end tests
	.venv/bin/pytest -q

test-db: ## run PostgreSQL integration tests (needs TEST_DATABASE_URL)
	.venv/bin/pytest -q tests/test_repository_pg.py

lint: ## ruff lint
	.venv/bin/ruff check src tests

fmt: ## ruff autofix + format
	.venv/bin/ruff check --fix src tests && .venv/bin/ruff format src tests

check: ## validate one proxy, e.g. make check P=socks5://203.0.113.5:1080
	$(COMPOSE) run --rm --no-deps worker python -m proxy_quality check $(P)

collect: ## fetch the configured sources once and print counts
	$(COMPOSE) run --rm --no-deps worker python -m proxy_quality collect

# ---------------------------------------------------------------- operations
build: ## build the image
	$(COMPOSE) build

up: ## start / update the stack
	$(COMPOSE) up -d --build --remove-orphans

down: ## stop the stack (data is kept)
	$(COMPOSE) down

restart: ## restart worker and api
	$(COMPOSE) restart worker api

logs: ## follow worker + api logs
	$(COMPOSE) logs -f --tail=100 worker api

ps: ## container status
	$(COMPOSE) ps

migrate: ## apply database migrations
	$(COMPOSE) run --rm migrate

stats: ## print current statistics
	@curl -fsS http://127.0.0.1:$${HTTP_PORT:-80}/api/v1/stats | $(PY) -m json.tool

health: ## health + readiness
	bash scripts/healthcheck.sh

backup: ## pg_dump into ./backups
	bash scripts/backup.sh

deploy: ## git pull + rebuild + migrate + restart
	bash scripts/deploy.sh

psql: ## open psql
	$(COMPOSE) exec postgres sh -c 'psql -U $$POSTGRES_USER $$POSTGRES_DB'

.PHONY: help venv test test-db lint fmt check collect build up down restart logs ps migrate stats health backup deploy psql
