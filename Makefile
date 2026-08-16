# Declare phony targets (commands that do not correspond to filesystem files)
.PHONY: help build up down restart logs clean test eda airflow-cli

# Default help screen listing available operations
help:
	@echo "Available commands for Flight Price Analytics Pipeline:"
	@echo "  make build       - Build all Docker images (Airflow & Jupyter)"
	@echo "  make up          - Start all containers in detached mode"
	@echo "  make down        - Stop and remove all running containers"
	@echo "  make restart     - Restart the container stack"
	@echo "  make logs        - Tail live logs from all running services"
	@echo "  make eda         - Show the JupyterLab access link"
	@echo "  make test        - Run pytest suite inside the Airflow container"
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

# Stream logs from all services
logs:
	docker compose logs -f

# Print JupyterLab direct access URL
eda:
	@echo "Open JupyterLab at: http://localhost:8888/lab?token=easyeda2026"

# Execute pytest suite inside the Airflow environment
test:
	docker compose exec airflow-scheduler pytest /opt/airflow/tests -v

# Completely reset the environment (Warning: deletes database data volumes)
clean:
	docker compose down -v --remove-orphans
	docker system prune -f