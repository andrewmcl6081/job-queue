-include .env
export

COMPOSE=docker compose -f deployments/docker-compose.yml
MIGRATIONS_PATH=./migrations

.PHONY: up down restart logs api dispatcher worker test fmt vet \
				wait-postgres wait-redis status \
				migrate-up migrate-down migrate-version migrate-force \
				psql redis-cli clean

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f

status:
	$(COMPOSE) ps

wait-postgres:
	@echo "Waiting for Postgres to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' jobqueue-postgres 2>/dev/null)" = "healthy" ]; do \
		sleep 1; \
	done
	@echo "Postgres is healthy."

wait-redis:
	@echo "Waiting for Redis to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' jobqueue-redis 2>/dev/null)" = "healthy" ]; do \
		sleep 1; \
	done
	@echo "Redis is healthy."

api:
	go run ./cmd/api

dispatcher:
	go run ./cmd/dispatcher

worker:
	go run ./cmd/worker

test:
	go test ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

migrate-up: wait-postgres
	migrate -path $(MIGRATIONS_PATH) -database "$(DATABASE_URL)" up

migrate-down: wait-postgres
	migrate -path $(MIGRATIONS_PATH) -database "$(DATABASE_URL)" down 1

migrate-version: wait-postgres
	migrate -path $(MIGRATIONS_PATH) -database "$(DATABASE_URL)" version

migrate-force: wait-postgres
	@if [ -z "$(VERSION)" ]; then echo "Usage: make migrate-force VERSION=1"; exit 1; fi
	migrate -path $(MIGRATIONS_PATH) -database "$(DATABASE_URL)" force $(VERSION)

psql: wait-postgres
	docker exec -it jobqueue-postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

redis-cli: wait-redis
	docker exec -it jobqueue-redis redis-cli

clean:
	$(COMPOSE) down -v