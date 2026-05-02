include .env
export

COMPOSE=docker compose -f deployments/docker-compose.yml
MIGRATIONS_PATH=./migrations

.PHONY: up down restart logs api dispatcher worker test fmt vet \
				migrate-up migrate-down migrate-version migrate-force \
				psql redis-cli

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f

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

migrate-up:
	migrate -path $(MIGRATIONS_PATH) -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path $(MIGRATIONS_PATH) -database "$(DATABASE_URL)" down 1

migrate-version:
	migrate -path $(MIGRATIONS_PATH) -database "$(DATABASE_URL)" version

migrate-force:
	@if [ -z "$(VERSION)" ]; then echo "Usage: make migrate-force VERSION=1"; exit 1; fi
	migrate -path $(MIGRATIONS_PATH) -database "$(DATABASE_URL)" force $(VERSION)

psql:
	docker exec -it jobqueue-postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

redis-cli:
	docker exec -it jobqueue-redis redis-cli