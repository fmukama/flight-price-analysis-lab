# Idempotent write of the cleaned fact table and 4 KPI tables into the
# PostgreSQL flight_analytics schema.
#
# All five tables load in ONE transaction. TRUNCATE is transactional in
# PostgreSQL, so readers see either the whole previous snapshot or the whole new
# one. Committing per-table (the earlier behaviour) could leave KPIs disagreeing
# with the fact table they came from.

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# NOTE: POSTGRES_PORT is the in-network port (5432), not the host-published one.
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "analytics_db")

ANALYTICS_SCHEMA = "flight_analytics"
FACT_TABLE = "fct_flight_prices_cleaned"

# Whitelist of loadable KPI tables. The load helper interpolates table names into
# SQL, so the set of permitted names is fixed here rather than trusted from the
# caller's dictionary keys.
ALLOWED_KPI_TABLES = {
    "kpi_avg_fare_by_airline",
    "kpi_seasonal_fare_variation",
    "kpi_airline_booking_count",
    "kpi_popular_routes",
}

# Rows per INSERT. 23 columns x 2000 rows stays well under PostgreSQL's
# 65,535 bound parameters per statement; larger chunks risk overflowing it.
FACT_CHUNK_SIZE = 2000


class AnalyticsLoadError(RuntimeError):
    """Raised when the analytics load cannot be completed consistently."""


def get_postgres_engine():
    """Constructs an SQLAlchemy engine for the PostgreSQL analytics database."""
    connection_uri = (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    return create_engine(connection_uri, pool_pre_ping=True)


def get_table_columns(conn, table: str, schema: str = ANALYTICS_SCHEMA) -> set:
    """Returns the column names of a target table, or raises if it is absent."""
    rows = conn.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table"
        ),
        {"schema": schema, "table": table},
    ).fetchall()
    if not rows:
        raise AnalyticsLoadError(
            f"Target table {schema}.{table} does not exist. "
            "Did sql/postgres/init_analytics.sql run on this database?"
        )
    return {r[0] for r in rows}


def validate_frame_against_table(df: pd.DataFrame, conn, table: str,
                                 schema: str = ANALYTICS_SCHEMA) -> None:
    """
    Confirms every DataFrame column has a home in the target table before any
    write is attempted, so a schema drift is reported clearly instead of
    surfacing as a mid-load database error.
    """
    if df is None or df.empty:
        raise AnalyticsLoadError(f"Refusing to load an empty DataFrame into {schema}.{table}.")

    table_columns = get_table_columns(conn, table, schema)
    unknown = [c for c in df.columns if c not in table_columns]
    if unknown:
        raise AnalyticsLoadError(
            f"{schema}.{table}: DataFrame has column(s) {unknown} that the table "
            f"does not define. Table columns: {sorted(table_columns)}"
        )


def load_fact_table(df_clean: pd.DataFrame, conn, schema: str = ANALYTICS_SCHEMA) -> int:
    """
    Truncates and reloads the primary cleaned flight prices fact table.

    Runs on a caller-supplied connection so it participates in the caller's
    transaction rather than committing on its own.
    """
    logger.info(f"Loading {len(df_clean):,} rows into {schema}.{FACT_TABLE}...")
    validate_frame_against_table(df_clean, conn, FACT_TABLE, schema)

    conn.execute(text(f"TRUNCATE TABLE {schema}.{FACT_TABLE} RESTART IDENTITY;"))
    df_clean.to_sql(
        FACT_TABLE,
        con=conn,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=FACT_CHUNK_SIZE,
    )

    written = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{FACT_TABLE}")).scalar()
    if written != len(df_clean):
        raise AnalyticsLoadError(
            f"{schema}.{FACT_TABLE}: expected {len(df_clean):,} rows after load but found {written:,}."
        )
    logger.info(f"Successfully loaded {FACT_TABLE} ({written:,} rows verified).")
    return written


def load_kpi_tables(kpis_dict: dict, conn, schema: str = ANALYTICS_SCHEMA) -> dict:
    """
    Truncates and writes all computed KPI metric tables on the caller's
    connection, so they land in the same transaction as the fact table.
    """
    unexpected = set(kpis_dict) - ALLOWED_KPI_TABLES
    if unexpected:
        raise AnalyticsLoadError(
            f"Unrecognised KPI target table(s): {sorted(unexpected)}. "
            f"Allowed: {sorted(ALLOWED_KPI_TABLES)}"
        )

    missing = ALLOWED_KPI_TABLES - set(kpis_dict)
    if missing:
        raise AnalyticsLoadError(f"KPI dataset(s) not supplied: {sorted(missing)}")

    loaded = {}
    for table_name, kpi_df in kpis_dict.items():
        logger.info(f"Loading KPI table: {schema}.{table_name} ({len(kpi_df)} rows)...")
        validate_frame_against_table(kpi_df, conn, table_name, schema)

        conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name};"))
        kpi_df.to_sql(
            table_name,
            con=conn,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
        )

        written = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}")).scalar()
        if written != len(kpi_df):
            raise AnalyticsLoadError(
                f"{schema}.{table_name}: expected {len(kpi_df)} rows after load but found {written}."
            )
        loaded[table_name] = written
        logger.info(f"Successfully loaded {table_name} ({written} rows verified).")
    return loaded


def run_full_load_pipeline(df_clean: pd.DataFrame, kpis_dict: dict) -> dict:
    """
    Orchestrates the complete loading sequence into PostgreSQL as one atomic
    unit, and returns the verified row count per table.
    """
    logger.info(f"Connecting to PostgreSQL analytics database at {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}...")
    try:
        engine = get_postgres_engine()
        # A single engine.begin() block: every TRUNCATE and every INSERT below
        # commits together, or none of them do.
        with engine.begin() as conn:
            counts = {FACT_TABLE: load_fact_table(df_clean, conn)}
            counts.update(load_kpi_tables(kpis_dict, conn))
    except SQLAlchemyError as exc:
        raise AnalyticsLoadError(
            f"Database failure while loading analytics into "
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}; transaction rolled back: {exc}"
        ) from exc

    logger.info(f"All {len(counts)} tables committed to PostgreSQL in one transaction: {counts}")
    return counts
