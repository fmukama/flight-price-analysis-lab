# Declare phony targets (commands that do not correspond to filesystem files)
.PHONY: help setup build up down restart logs ps clean test eda adminer airflow-cli \
        unpause pause trigger run dag-status psql mysql-cli verify

DAG_ID := flight_price_analysis_pipeline

# Default help screen listing available operations
help:
	@echo "Available commands for Flight Price Analytics Pipeline:"
	@echo ""
	@echo " Setup & lifecycle"
	@echo "  make setup       - Create .env from .env.example (never overwrites an existing .env)"
	@echo "  make build       - Build all Docker images (Airflow & Jupyter)"
	@echo "  make up          - Start all containers in detached mode"
	@echo "  make down        - Stop and remove all running containers"
	@echo "  make restart     - Restart the container stack"
	@echo "  make clean       - Remove all containers, dangling images, and volumes"
	@echo ""
	@echo " Running the pipeline"
	@echo "  make unpause     - Unpause the DAG so the scheduler can run it"
	@echo "  make pause       - Pause the DAG again (it ships paused by design)"
	@echo "  make trigger     - Queue a DAG run through the scheduler"
	@echo "  make run         - Execute the DAG end-to-end now and stream the output"
	@echo "  make dag-status  - List recent DAG runs and their states"
	@echo ""
	@echo " Inspecting results"
	@echo "  make verify      - Row counts for the staging table and all 5 analytics tables"
	@echo "  make adminer     - Show the Adminer SQL browser link and connection details"
	@echo "  make psql        - Open a psql shell on the analytics database"
	@echo "  make mysql-cli   - Open a mysql shell on the staging database"
	@echo "  make eda         - Show the JupyterLab access link"
	@echo ""
	@echo " Diagnostics"
	@echo "  make ps          - Show container state, health, and restart counts"
	@echo "  make logs        - Tail live logs from all running services"
	@echo "  make test        - Run pytest suite inside the Airflow container"
	@echo "  make airflow-cli - Open a shell in the scheduler container"

setup:
	@if [ -f .env ]; then \
		echo ".env already exists -- leaving it untouched."; \
	else \
		cp .env.example .env && echo "Created .env from .env.example."; \
	fi

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
	@for c in flight_airflow_webserver flight_airflow_scheduler flight_mysql_staging flight_postgres_analytics flight_jupyter_eda flight_adminer; do \
		printf '  %-30s %s\n' "$$c" "$$(docker inspect $$c --format '{{.State.Status}} restarts={{.RestartCount}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}' 2>/dev/null || echo absent)"; \
	done

# Stream logs from all services
logs:
	docker compose logs -f

unpause:
	docker compose exec -T airflow-scheduler airflow dags unpause $(DAG_ID)

pause:
	docker compose exec -T airflow-scheduler airflow dags pause $(DAG_ID)

trigger:
	docker compose exec -T airflow-scheduler airflow dags trigger $(DAG_ID)

run:
	docker compose exec -T airflow-scheduler airflow dags test $(DAG_ID)

dag-status:
	docker compose exec -T airflow-scheduler airflow dags list-runs -d $(DAG_ID)

# Print JupyterLab access URL, reading the real token from .env rather than
# hardcoding a value that can drift out of sync with the container.
eda:
	@token=$$(grep -E '^JUPYTER_TOKEN=' .env | cut -d= -f2-); \
	port=$$(grep -E '^JUPYTER_PORT=' .env | cut -d= -f2-); \
	echo "Open JupyterLab at: http://localhost:$${port:-8888}/lab?token=$${token}"

adminer:
	@port=$$(grep -E '^ADMINER_PORT=' .env | cut -d= -f2-); \
	pg_user=$$(grep -E '^POSTGRES_USER=' .env | cut -d= -f2-); \
	pg_db=$$(grep -E '^POSTGRES_DB=' .env | cut -d= -f2-); \
	my_user=$$(grep -E '^MYSQL_USER=' .env | cut -d= -f2-); \
	my_db=$$(grep -E '^MYSQL_DATABASE=' .env | cut -d= -f2-); \
	echo "Open Adminer at: http://localhost:$${port:-8081}"; \
	echo ""; \
	printf '  %-12s %-11s %-9s %-15s %s\n' "" System Server User Database; \
	printf '  %-12s %-11s %-9s %-15s %s\n' "analytics" PostgreSQL postgres "$$pg_user" "$$pg_db"; \
	printf '  %-12s %-11s %-9s %-15s %s\n' "staging" MySQL mysql "$$my_user" "$$my_db"; \
	echo ""; \
	echo "  Passwords: POSTGRES_PASSWORD / MYSQL_PASSWORD in .env"; \
	echo "  Analytics tables live under the flight_analytics schema, not public."

psql:
	@pw=$$(grep -E '^POSTGRES_PASSWORD=' .env | cut -d= -f2-); \
	user=$$(grep -E '^POSTGRES_USER=' .env | cut -d= -f2-); \
	db=$$(grep -E '^POSTGRES_DB=' .env | cut -d= -f2-); \
	docker compose exec -e PGPASSWORD=$$pw postgres psql -U $$user -d $$db

mysql-cli:
	@pw=$$(grep -E '^MYSQL_PASSWORD=' .env | cut -d= -f2-); \
	user=$$(grep -E '^MYSQL_USER=' .env | cut -d= -f2-); \
	db=$$(grep -E '^MYSQL_DATABASE=' .env | cut -d= -f2-); \
	docker compose exec mysql mysql -u $$user -p$$pw -D $$db

verify:
	@my_pw=$$(grep -E '^MYSQL_PASSWORD=' .env | cut -d= -f2-); \
	my_user=$$(grep -E '^MYSQL_USER=' .env | cut -d= -f2-); \
	my_db=$$(grep -E '^MYSQL_DATABASE=' .env | cut -d= -f2-); \
	pg_pw=$$(grep -E '^POSTGRES_PASSWORD=' .env | cut -d= -f2-); \
	pg_user=$$(grep -E '^POSTGRES_USER=' .env | cut -d= -f2-); \
	pg_db=$$(grep -E '^POSTGRES_DB=' .env | cut -d= -f2-); \
	echo "MySQL staging:"; \
	docker compose exec -T mysql mysql -u $$my_user -p$$my_pw -D $$my_db \
		-e "SELECT 'raw_flight_prices' AS table_name, COUNT(*) AS row_count FROM raw_flight_prices;" 2>&1 \
		| grep -v 'Using a password'; \
	echo ""; \
	echo "PostgreSQL analytics:"; \
	docker compose exec -T -e PGPASSWORD=$$pg_pw postgres psql -U $$pg_user -d $$pg_db -c \
		"SELECT 'fct_flight_prices_cleaned' AS table_name, COUNT(*) AS rows FROM flight_analytics.fct_flight_prices_cleaned \
		 UNION ALL SELECT 'kpi_avg_fare_by_airline', COUNT(*) FROM flight_analytics.kpi_avg_fare_by_airline \
		 UNION ALL SELECT 'kpi_seasonal_fare_variation', COUNT(*) FROM flight_analytics.kpi_seasonal_fare_variation \
		 UNION ALL SELECT 'kpi_airline_booking_count', COUNT(*) FROM flight_analytics.kpi_airline_booking_count \
		 UNION ALL SELECT 'kpi_popular_routes', COUNT(*) FROM flight_analytics.kpi_popular_routes;"

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
