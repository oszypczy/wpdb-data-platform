# WPDB mini data platform. Targets are the entry points documented in
# docs/design/08-operations.md; the agents' stack-ops / run-tests skills use them.
OLIST_DIR := data/olist
OLIST_URL := https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce
OLIST_STAMP := $(OLIST_DIR)/.downloaded
WAIT_HEALTHY := .claude/skills/stack-ops/scripts/wait-healthy.sh
COMPOSE := docker compose

.PHONY: help sync env up down reset ps logs lint test-unit test-integration test-integration-full test-integration-fresh test-e2e data clean-data

help:
	@echo "make sync             - create the uv venv with the test/tooling dependencies (uv sync)"
	@echo "make up               - docker compose up -d --build, then wait until every service is healthy"
	@echo "make down             - stop the stack, keep volumes (data, offsets, checkpoints)"
	@echo "make reset            - stop the stack and WIPE volumes (docker compose down -v)"
	@echo "make ps               - state and health of the containers"
	@echo "make logs [S=svc]     - follow logs (all services or one: make logs S=connect)"
	@echo "make lint             - ruff check + format check"
	@echo "make test-unit        - pytest -m unit (no Docker)"
	@echo "make test-integration - fast set: pytest -m 'integration and not slow' (starts the stack if it is down)"
	@echo "make test-integration-full  - all integration tests (incl. slow) on the running stack"
	@echo "make test-integration-fresh - all integration tests from wiped volumes (WPDB_FRESH=1; incl. the 180 s startup budget test)"
	@echo "make test-e2e         - pytest -m e2e on a fresh full stack (WPDB_FRESH=1)"
	@echo "make data             - download the Olist dataset into $(OLIST_DIR)/ (skipped if present)"
	@echo "make clean-data       - remove downloaded CSV files"

sync:
	uv sync

# .env holds the secrets; create it from the example on first use.
env: .env
.env:
	cp .env.example .env
	@echo "created .env from .env.example (edit it to change secrets)"

up: .env data
	$(COMPOSE) up -d --build
	$(WAIT_HEALTHY) 300

down:
	$(COMPOSE) down --remove-orphans

reset:
	$(COMPOSE) down -v --remove-orphans

ps:
	$(COMPOSE) ps -a

logs:
	$(COMPOSE) logs -f --tail=200 $(S)

lint: sync
	uv run ruff check .
	uv run ruff format --check .

test-unit: sync
	uv run pytest -m unit

test-integration: .env sync
	uv run pytest -m "integration and not slow"

test-integration-full: .env sync
	uv run pytest -m integration

test-integration-fresh: .env sync
	WPDB_FRESH=1 uv run pytest -m integration

test-e2e: .env sync
	WPDB_FRESH=1 uv run pytest -m e2e

data: $(OLIST_STAMP)

$(OLIST_STAMP):
	@echo "Downloading Olist dataset..."
	@tmp=$$(mktemp -d) && \
	curl -fsSL -o $$tmp/olist.zip $(OLIST_URL) && \
	unzip -oq $$tmp/olist.zip -d $(OLIST_DIR) -x olist_geolocation_dataset.csv && \
	rm -rf $$tmp
	@touch $@
	@echo "Done: $$(ls $(OLIST_DIR)/*.csv | wc -l | tr -d ' ') CSV files in $(OLIST_DIR)/"

clean-data:
	rm -f $(OLIST_DIR)/*.csv $(OLIST_STAMP)
