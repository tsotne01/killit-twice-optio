.PHONY: help up down restart status logs seed verify clean

SHELL := /bin/bash

help: ## Display this help message
	@echo "Kill It Twice - Fault-Tolerant Replication Pipeline"
	@echo "Optio Platform Engineering Evaluation"
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

up: ## Start all services in the background
	docker compose up -d --build

down: ## Stop all services and tear down containers
	docker compose down

down-v: ## Stop all services and remove persistent volumes
	docker compose down -v

restart: ## Restart the entire application stack
	docker compose restart

status: ## Check the status and health of all stack containers
	docker compose ps

logs: ## Tail logs from the replication pipeline
	docker compose logs -f pipeline

logs-all: ## Tail logs from all containers
	docker compose logs -f

seed: ## Generate 1,000,000 high-throughput test records in PostgreSQL
	@bash ./scripts/seed.sh

verify: ## Run automated chaos verification suite for Gates G1-G5
	@bash ./scripts/verify.sh

clean: ## Clean local build artifacts and temporary files
	rm -rf node_modules dist
