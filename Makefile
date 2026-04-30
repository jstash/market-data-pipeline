.PHONY: up down logs ps reset topics test help

## ── Lifecycle ───────────────────────────────────────────────────────────────

up:          ## Start all services in the background
	docker compose up -d

down:        ## Stop all services (preserves data volumes)
	docker compose down

logs:        ## Stream logs from all services
	docker compose logs -f

ps:          ## Show running service status
	docker compose ps

reset:       ## !! Destroy all data volumes and restart clean
	docker compose down -v

## ── Kafka setup ─────────────────────────────────────────────────────────────

topics:      ## Create Kafka topics (run once after 'make up')
	docker compose exec redpanda rpk topic create \
		raw.prices \
		processed.ohlcv \
		alerts.anomalies \
		--partitions 3 \
		--replicas 1 \
		--config retention.ms=86400000   # 24h for raw; tighten per-topic as needed
	@echo ""
	docker compose exec redpanda rpk topic list

## ── Development helpers ─────────────────────────────────────────────────────

# Tail a single service:  make logs-ingester
logs-%:
	docker compose logs -f $*

# Open a psql shell into the database
psql:
	docker compose exec postgres psql -U pipeline -d marketdata

# Consume from a topic (for debugging):  make consume-topic TOPIC=raw.prices
consume-topic:
	docker compose exec redpanda rpk topic consume $(TOPIC) --offset start

## ── Testing ──────────────────────────────────────────────────────────────────

test:        ## Run tests for all services (filled in per phase)
	@for svc in services/*/; do \
		if [ -f "$$svc/Makefile" ]; then $(MAKE) -C "$$svc" test; fi \
	done

## ── Help ─────────────────────────────────────────────────────────────────────

help:        ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
