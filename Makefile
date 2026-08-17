# Declare phony targets (commands that do not correspond to filesystem files)
.PHONY: help build up down restart logs ps clean test eda airflow-cli

# Default help screen listing available operations
help:
	@echo "Available commands for Flight Price Analytics Pipeline:"
	@echo "  make build       - Build all Docker images (Airflow & Jupyter)"
	@echo "  make up          - Start all containers in detached mode"
	@echo "  make down        - Stop and remove all running containers"
	@echo "  make restart     - Restart the container stack"
	@echo "  make ps          - Show container state, health, and restart counts"
	@echo "  make logs        - Tail live logs from all running services"
	@echo "  make eda         - Show the JupyterLab access link"
	@echo "  make test        - Run pytest suite inside the Airflow container"
	@echo "  make airflow-cli - Open a shell in the scheduler container"
	@echo "  make clean       - Remove all containers, dangling images, and volumes"

# Build images
build:
	docker compose build

# Start services in the background
up:
	docker compose up -d

# Stop services
down:
	docker compose down

# Restart the entire stack
restart:
	docker compose down && docker compose up -d

# Container state at a glance. RestartCount is the column that exposes a
# crash-loop: `docker compose ps` alone reports "Up" between restarts, which is
# how a broken Airflow install once went unnoticed through 68 restarts.
ps:
	@docker compose ps
	@echo ""
	@echo "State / restarts / health (non-zero restarts means a crash-loop):"
	@for c in flight_airflow_webserver flight_airflow_scheduler flight_mysql_staging flight_postgres_analytics flight_jupyter_eda; do \
		printf '  %-30s %s\n' "$$c" "$$(docker inspect $$c --format '{{.State.Status}} restarts={{.RestartCount}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}' 2>/dev/null || echo absent)"; \
	done

# Stream logs from all services
logs:
	docker compose logs -f

# Print JupyterLab access URL, reading the real token from .env rather than
# hardcoding a value that can drift out of sync with the container.
eda:
	@token=$$(grep -E '^JUPYTER_TOKEN=' .env | cut -d= -f2-); \
	port=$$(grep -E '^JUPYTER_PORT=' .env | cut -d= -f2-); \
	echo "Open JupyterLab at: http://localhost:$${port:-8888}/lab?token=$${token}"

# Execute pytest suite inside the Airflow environment
test:
	docker compose exec -T airflow-scheduler pytest /opt/airflow/tests -v

# Interactive shell inside the scheduler for ad-hoc `airflow ...` commands
airflow-cli:
	docker compose exec airflow-scheduler bash

# Completely reset the environment (Warning: deletes database data volumes)
clean:
	docker compose down -v --remove-orphans
	docker system prune -f
