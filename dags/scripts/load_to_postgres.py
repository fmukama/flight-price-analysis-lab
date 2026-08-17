# Idempotent write to PostgreSQL analytics tables
# This module writes the cleaned Fact dataset and the 4 computed KPI summary tables into the PostgreSQL database under the flight_analytics schema, using transactions and truncates to guarantee idempotency.

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "analytics_db")


def get_postgres_engine():
    """Constructs an SQLAlchemy engine for the PostgreSQL analytics database."""
    connection_uri = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(connection_uri, pool_pre_ping=True)


def load_fact_table(df_clean: pd.DataFrame, engine, schema: str = "flight_analytics"):
    """Truncates and reloads the primary cleaned flight prices fact table."""
    logger.info(f"Loading {len(df_clean):,} rows into {schema}.fct_flight_prices_cleaned...")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.fct_flight_prices_cleaned RESTART IDENTITY;"))
        df_clean.to_sql(
            "fct_flight_prices_cleaned",
            con=conn,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )
    logger.info("Successfully loaded fct_flight_prices_cleaned.")


def load_kpi_tables(kpis_dict: dict, engine, schema: str = "flight_analytics"):
    """Truncates and writes all 4 computed KPI metric tables."""
    for table_name, kpi_df in kpis_dict.items():
        logger.info(f"Loading KPI table: {schema}.{table_name} ({len(kpi_df)} rows)...")
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name};"))
            kpi_df.to_sql(
                table_name,
                con=conn,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi"
            )
        logger.info(f"Successfully loaded {table_name}.")


def run_full_load_pipeline(df_clean: pd.DataFrame, kpis_dict: dict):
    """Orchestrates complete loading sequence into PostgreSQL."""
    engine = get_postgres_engine()
    load_fact_table(df_clean, engine)
    load_kpi_tables(kpis_dict, engine)
    logger.info("All tables loaded into PostgreSQL successfully.")