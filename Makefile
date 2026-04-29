.PHONY: dev up down restart logs ps api-logs web-logs db-logs db-shell api-shell web-shell clean

dev:
	docker compose up --build

up:
	docker compose up -d --build

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d --build

logs:
	docker compose logs -f

api-logs:
	docker compose logs -f api

web-logs:
	docker compose logs -f web

db-logs:
	docker compose logs -f postgres

ps:
	docker compose ps

db-shell:
	docker exec -it onehaven-postgres psql -U onehaven -d onehaven_market

api-shell:
	docker exec -it onehaven-api sh

web-shell:
	docker exec -it onehaven-web sh

clean:
	docker compose down -v
