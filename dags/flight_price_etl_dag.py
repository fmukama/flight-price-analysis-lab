# Main Airflow DAG orchestrating MySQL -> Python -> Postgres
"""
DAG: flight_price_analysis_pipeline
Description: End-to-end ETL orchestrating flight data from CSV -> MySQL (Staging) -> Python (Transform/KPIs) -> PostgreSQL (Analytics).
"""

from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task

# Default arguments inherited by all tasks in the DAG
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

logger = logging.getLogger(__name__)


@dag(
    dag_id="flight_price_analysis_pipeline",
    default_args=default_args,
    description="ETL pipeline for Bangladesh flight prices and analytics KPIs",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bangladesh", "flights", "mysql", "postgres", "kpi"],
)
def flight_price_etl():

    @task(task_id="ingest_csv_to_mysql_staging")
    def task_ingest_raw_data():
        """Reads raw CSV file and populates MySQL staging table."""
        from dags.scripts.ingest_to_mysql import ingest_raw_csv_to_mysql
        logger.info("Executing Task 1: Staging raw CSV into MySQL...")
        row_count = ingest_raw_csv_to_mysql()
        return {"staged_rows": row_count}

    @task(task_id="extract_validate_and_clean")
    def task_validate_and_clean(ingest_meta: dict):
        """Extracts data from MySQL, cleans nulls, enforces types, and removes outliers."""
        from dags.scripts.validate_clean import extract_staged_data, validate_and_clean_flight_data
        logger.info(f"Executing Task 2: Validating staged data (received {ingest_meta['staged_rows']} rows)...")
        raw_df = extract_staged_data()
        clean_df = validate_and_clean_flight_data(raw_df)
        
        # Save temporary parquet for seamless memory-safe handoff between workers
        temp_clean_path = "/opt/airflow/data/processed/clean_flights.parquet"
        clean_df.to_parquet(temp_clean_path, index=False)
        return {"clean_data_path": temp_clean_path, "clean_rows": len(clean_df)}

    @task(task_id="compute_business_kpis")
    def task_compute_kpis(clean_meta: dict):
        """Calculates the 4 business KPI aggregations."""
        import pandas as pd
        from dags.scripts.compute_kpis import compute_all_kpis
        logger.info(f"Executing Task 3: Computing analytical KPIs from {clean_meta['clean_rows']} records...")
        
        clean_df = pd.read_parquet(clean_meta["clean_data_path"])
        kpis = compute_all_kpis(clean_df)

        # Save KPI datasets to disk
        kpi_paths = {}
        for name, df in kpis.items():
            path = f"/opt/airflow/data/processed/{name}.parquet"
            df.to_parquet(path, index=False)
            kpi_paths[name] = path

        return {"clean_data_path": clean_meta["clean_data_path"], "kpi_paths": kpi_paths}

    @task(task_id="load_analytics_to_postgres")
    def task_load_to_postgres(pipeline_payload: dict):
        """Loads clean fact table and all KPI tables into PostgreSQL."""
        import pandas as pd
        from dags.scripts.load_to_postgres import run_full_load_pipeline
        logger.info("Executing Task 4: Loading clean datasets and KPI tables into PostgreSQL...")
        
        clean_df = pd.read_parquet(pipeline_payload["clean_data_path"])
        kpis_dict = {
            name: pd.read_parquet(path)
            for name, path in pipeline_payload["kpi_paths"].items()
        }

        run_full_load_pipeline(clean_df, kpis_dict)
        logger.info("Pipeline complete! All data committed to PostgreSQL.")
        return "SUCCESS"

    # Define DAG Task Dependencies
    staged = task_ingest_raw_data()
    cleaned = task_validate_and_clean(staged)
    kpis = task_compute_kpis(cleaned)
    task_load_to_postgres(kpis)


# Instantiate the DAG
dag_instance = flight_price_etl()