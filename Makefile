.PHONY: dev up down restart logs ps api-logs web-logs db-logs db-shell api-shell web-shell clean migrate migration downgrade

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

migrate:
	docker compose exec api alembic upgrade head

migration:
	docker compose exec api alembic revision -m "$(name)"

downgrade:
	docker compose exec api alembic downgrade -1

clean:
	docker compose down -v

.PHONY: dbt-install dbt-debug dbt-seed dbt-build dbt-test dbt-quality

dbt-install:
	pip install -r requirements-dbt.txt

dbt-debug:
	cd dbt && DBT_PROFILES_DIR=. dbt debug

dbt-seed:
	cd dbt && DBT_PROFILES_DIR=. dbt seed --full-refresh

dbt-build:
	cd dbt && DBT_PROFILES_DIR=. dbt build --fail-fast

dbt-test:
	cd dbt && DBT_PROFILES_DIR=. dbt test --fail-fast

dbt-quality:
	./scripts/smoke_dbt_quality.sh

.PHONY: smoke smoke-api smoke-geo smoke-frontend smoke-source-freshness smoke-pipeline-observability smoke-logging

smoke:
	./apps/api/scripts/smoke_all.sh

smoke-api:
	./apps/api/scripts/smoke_epic5.sh

smoke-geo:
	./apps/api/scripts/smoke_geo.sh

smoke-frontend:
	./apps/api/scripts/smoke_frontend.sh

smoke-source-freshness:
	./apps/api/scripts/smoke_source_freshness.sh

smoke-pipeline-observability:
	./apps/api/scripts/smoke_pipeline_observability.sh

smoke-logging:
	./apps/api/scripts/smoke_application_logging.sh

