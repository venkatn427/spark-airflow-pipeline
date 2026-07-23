.PHONY: up down restart logs ps clean build init

up:
	@./scripts/start.sh

down:
	docker compose down --remove-orphans

restart: down up

build:
	docker compose build --no-cache

logs:
	docker compose logs -f

ps:
	docker compose ps

clean:
	docker compose down -v --remove-orphans
	@echo "All containers, networks and volumes removed."

init:
	@mkdir -p logs/airflow logs/spark airflow/plugins
	@chmod -R 777 logs/
	@echo "Local directories ready."
