# Main Airflow DAG orchestrating MySQL -> Python -> Postgres
"""
DAG: flight_price_analysis_pipeline
Description: End-to-end ETL orchestrating flight data from CSV -> MySQL (Staging)
-> Python (Transform/KPIs) -> PostgreSQL (Analytics).

Task hand-off uses parquet files under data/processed/ rather than XCom, because
the cleaned fact table is ~57k rows and XCom is not a data channel. Only small
metadata dictionaries travel through XCom.
"""

from datetime import datetime, timedelta
import logging
import os

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

# Shared location for the inter-task parquet hand-off
PROCESSED_DIR = os.getenv("PROCESSED_DATA_DIR", "/opt/airflow/data/processed")
CLEAN_PARQUET = os.path.join(PROCESSED_DIR, "clean_flights.parquet")


@dag(
    dag_id="flight_price_analysis_pipeline",
    default_args=default_args,
    description="ETL pipeline for Bangladesh flight prices and analytics KPIs",
    # `schedule` supersedes the deprecated `schedule_interval` in Airflow 2.9.
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    # Paused on creation: with a past start_date an unpaused DAG fires the
    # instant the stack boots, which once double-loaded staging. Unpause in the UI.
    is_paused_upon_creation=True,
    tags=["bangladesh", "flights", "mysql", "postgres", "kpi"],
)
def flight_price_etl():

    @task(task_id="ingest_csv_to_mysql_staging")
    def task_ingest_raw_data():
        """Reads raw CSV file and populates MySQL staging table."""
        from dags.scripts.ingest_to_mysql import ingest_raw_csv_to_mysql

        logger.info("Executing Task 1: Staging raw CSV into MySQL...")
        meta = ingest_raw_csv_to_mysql()
        logger.info(
            "Task 1 complete: %s rows staged from %s across %s chunk(s).",
            f"{meta['staged_rows']:,}", meta["csv_path"], meta["chunks"],
        )
        return meta

    @task(task_id="extract_validate_and_clean")
    def task_validate_and_clean(ingest_meta: dict):
        """Extracts data from MySQL, cleans nulls, enforces types, and flags anomalies."""
        from dags.scripts.validate_clean import (
            extract_staged_data,
            validate_and_clean_flight_data,
            DataValidationError,
        )

        staged_rows = ingest_meta["staged_rows"]
        logger.info("Executing Task 2: Validating staged data (%s rows staged)...", f"{staged_rows:,}")

        raw_df = extract_staged_data()
        if len(raw_df) != staged_rows:
            raise DataValidationError(
                f"Staging changed underneath the pipeline: task 1 staged {staged_rows:,} rows "
                f"but task 2 read {len(raw_df):,}. Another writer is touching the staging table."
            )

        clean_df, report = validate_and_clean_flight_data(raw_df)

        # The report is the audit trail for every correction and every dropped
        # row, so log it in full rather than only the surviving row count.
        logger.info("Task 2 validation report: %s", report)
        if report["fare_mismatches_corrected"]:
            logger.warning(
                "%s row(s) had Total Fare != Base + Tax and were recomputed; "
                "originals preserved in total_fare_original_bdt.",
                f"{report['fare_mismatches_corrected']:,}",
            )

        # Save temporary parquet for seamless memory-safe handoff between workers
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        clean_df.to_parquet(CLEAN_PARQUET, index=False)
        return {"clean_data_path": CLEAN_PARQUET, "clean_rows": len(clean_df), "report": report}

    @task(task_id="compute_business_kpis")
    def task_compute_kpis(clean_meta: dict):
        """Calculates the 4 business KPI aggregations."""
        import pandas as pd
        from dags.scripts.compute_kpis import compute_all_kpis

        logger.info(
            "Executing Task 3: Computing analytical KPIs from %s records...",
            f"{clean_meta['clean_rows']:,}",
        )

        clean_df = pd.read_parquet(clean_meta["clean_data_path"])
        if len(clean_df) != clean_meta["clean_rows"]:
            raise ValueError(
                f"Parquet hand-off mismatch: task 2 wrote {clean_meta['clean_rows']:,} rows "
                f"but {clean_meta['clean_data_path']} holds {len(clean_df):,}."
            )

        kpis = compute_all_kpis(clean_df)

        # Save KPI datasets to disk
        kpi_paths = {}
        for name, kpi_df in kpis.items():
            path = os.path.join(PROCESSED_DIR, f"{name}.parquet")
            kpi_df.to_parquet(path, index=False)
            kpi_paths[name] = path
            logger.info("Task 3: %s -> %s rows.", name, len(kpi_df))

        return {
            "clean_data_path": clean_meta["clean_data_path"],
            "clean_rows": clean_meta["clean_rows"],
            "kpi_paths": kpi_paths,
        }

    @task(task_id="load_analytics_to_postgres")
    def task_load_to_postgres(pipeline_payload: dict):
        """Loads clean fact table and all KPI tables into PostgreSQL atomically."""
        import pandas as pd
        from dags.scripts.load_to_postgres import run_full_load_pipeline

        logger.info("Executing Task 4: Loading clean datasets and KPI tables into PostgreSQL...")

        clean_df = pd.read_parquet(pipeline_payload["clean_data_path"])
        kpis_dict = {
            name: pd.read_parquet(path)
            for name, path in pipeline_payload["kpi_paths"].items()
        }

        counts = run_full_load_pipeline(clean_df, kpis_dict)

        loaded_fact = counts["fct_flight_prices_cleaned"]
        if loaded_fact != pipeline_payload["clean_rows"]:
            raise ValueError(
                f"Load verification failed: expected {pipeline_payload['clean_rows']:,} fact rows "
                f"but PostgreSQL holds {loaded_fact:,}."
            )

        logger.info("Pipeline complete! All data committed to PostgreSQL: %s", counts)
        return counts

    # Define DAG Task Dependencies
    staged = task_ingest_raw_data()
    cleaned = task_validate_and_clean(staged)
    kpis = task_compute_kpis(cleaned)
    task_load_to_postgres(kpis)


# Instantiate the DAG
dag_instance = flight_price_etl()
