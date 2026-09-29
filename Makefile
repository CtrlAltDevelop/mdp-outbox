# Everyday commands. Tests read REDIS_URL and DATABASE_URL: `make services`
# starts both in Docker on the ports below, and the suite skips what it cannot reach.

REDIS_PORT ?= 56380
TIMESCALE_PORT ?= 55433
export REDIS_URL ?= redis://localhost:$(REDIS_PORT)/0
export DATABASE_URL ?= postgresql://mdp:mdp@localhost:$(TIMESCALE_PORT)/mdp

.PHONY: install lint format typecheck test check services services-down up down bench requirements

install:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy

test:
	uv run pytest

check: lint typecheck test

services:
	docker run -d --rm --name mdp-redis -p $(REDIS_PORT):6379 redis:7-alpine
	docker run -d --rm --name mdp-ts -p $(TIMESCALE_PORT):5432 -e POSTGRES_USER=mdp \
		-e POSTGRES_PASSWORD=mdp -e POSTGRES_DB=mdp timescale/timescaledb:latest-pg16

services-down:
	docker rm -f mdp-redis mdp-ts

up:
	docker compose up --build -d

down:
	docker compose down

bench:
	uv run python bench/aggregator.py

# CI installs with pip from this file; regenerate it whenever uv.lock changes.
requirements:
	uv export --frozen --no-hashes --no-emit-project --all-groups -o requirements.txt
