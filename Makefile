# kawa - Kafka access gateway
#
# One-command Docker Compose workflows. `make up` ALWAYS rebuilds the gateway image
# and force-recreates containers, so code and config changes are picked up reliably.
# A plain `docker compose up` can silently keep running a stale image/container.

COMPOSE      := docker compose
COMPOSE_SASL := docker compose -f docker-compose.sasl.yml

.PHONY: help up up-sasl down down-sasl logs logs-sasl ps build test clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up: build ## Build + start the plain demo stack (always rebuilds image and recreates containers)
	$(COMPOSE) up -d --build --force-recreate

up-sasl: ## Build + start the SASL demo stack (always rebuilds image and recreates containers)
	$(COMPOSE_SASL) up -d --build --force-recreate

down: ## Stop and remove the plain stack
	$(COMPOSE) down

down-sasl: ## Stop and remove the SASL stack
	$(COMPOSE_SASL) down

logs: ## Follow logs of the plain stack
	$(COMPOSE) logs -f

logs-sasl: ## Follow logs of the SASL stack
	$(COMPOSE_SASL) logs -f

ps: ## Show status of the plain stack
	$(COMPOSE) ps

build: ## Build the gateway jar locally (no Docker)
	./mvnw -pl kawa-server -am package -DskipTests

test: ## Run all unit tests
	./mvnw test

clean: ## Clean all build outputs
	./mvnw clean
