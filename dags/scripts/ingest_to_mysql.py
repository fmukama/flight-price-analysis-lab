# Ingests the raw Kaggle CSV into MySQL staging (staging_db.raw_flight_prices).
#
# Guards, in order: validate the CSV header, take an advisory lock so a second
# concurrent ingest fails instead of interleaving, load inside one transaction,
# then reconcile the staged row count against the rows read. Two overlapping
# loads once produced 114,000 rows from a 57,000-row CSV and reported success.

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Set up logging for Airflow task execution visibility
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Default connection parameters.
# NOTE: MYSQL_PORT is the port reachable *inside* the compose network (3306).
# The host-published port is MYSQL_HOST_PORT and is deliberately not read here.
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.getenv("MYSQL_USER", "staging_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "staging_password")
MYSQL_DB = os.getenv("MYSQL_DATABASE", "staging_db")
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "/opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv")

STAGING_TABLE = "raw_flight_prices"

# Named MySQL advisory lock serialising ingest across processes
INGEST_LOCK_NAME = "flight_price_ingest"

# Maps the raw CSV headers onto the staging schema column names. The keys are
# also the contract for header validation: every one must be present in the CSV.
COLUMN_MAPPING = {
    "Airline": "airline",
    "Source": "source",
    "Source Name": "source_name",
    "Destination": "destination",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_datetime",
    "Arrival Date & Time": "arrival_datetime",
    "Duration (hrs)": "duration_hrs",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    "Class": "class",
    "Booking Source": "booking_source",
    "Base Fare (BDT)": "base_fare",
    "Tax & Surcharge (BDT)": "tax_and_surcharge",
    "Total Fare (BDT)": "total_fare",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}


class StagingIntegrityError(RuntimeError):
    """Raised when the staged data does not faithfully match the source CSV."""


def get_mysql_engine():
    """Constructs an SQLAlchemy engine for the MySQL staging database."""
    connection_uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    return create_engine(connection_uri, pool_pre_ping=True)


def validate_csv_source(csv_path: str) -> list:
    """
    Validates the CSV is present, non-empty, and carries every expected header
    before any database work begins. Returns the raw header list.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Raw CSV dataset not found at expected path: {csv_path}")

    if os.path.getsize(csv_path) == 0:
        raise ValueError(f"Raw CSV dataset at {csv_path} is empty (0 bytes).")

    try:
        header_df = pd.read_csv(csv_path, nrows=0)
    except pd.errors.EmptyDataError as exc:
        raise ValueError(f"Raw CSV dataset at {csv_path} has no readable header row.") from exc
    except pd.errors.ParserError as exc:
        raise ValueError(f"Raw CSV dataset at {csv_path} is malformed and cannot be parsed: {exc}") from exc

    headers = list(header_df.columns)
    missing = [col for col in COLUMN_MAPPING if col not in headers]
    if missing:
        raise KeyError(
            f"Raw CSV is missing {len(missing)} required column(s): {missing}. "
            f"Found headers: {headers}"
        )

    # Extra columns are tolerated (they are dropped), but surface them so a
    # changed upstream export does not go unnoticed.
    extra = [col for col in headers if col not in COLUMN_MAPPING]
    if extra:
        logger.warning(f"Ignoring {len(extra)} unmapped CSV column(s): {extra}")

    logger.info(f"CSV header validated: all {len(COLUMN_MAPPING)} required columns present.")
    return headers


def get_staging_column_widths(conn) -> dict:
    """
    Reads the declared VARCHAR widths of the staging table from MySQL. Used to
    reject values that would otherwise be silently truncated on a server that
    is not running in strict mode.
    """
    rows = conn.execute(
        text(
            "SELECT column_name, character_maximum_length "
            "FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table "
            "AND character_maximum_length IS NOT NULL"
        ),
        {"schema": MYSQL_DB, "table": STAGING_TABLE},
    ).fetchall()

    if not rows:
        raise StagingIntegrityError(
            f"Staging table {MYSQL_DB}.{STAGING_TABLE} not found or has no character columns. "
            "Did sql/mysql/init_staging.sql run?"
        )
    return {name: int(width) for name, width in rows}


def check_chunk_fits_schema(chunk: pd.DataFrame, widths: dict, chunk_number: int) -> None:
    """Fails loudly if any value is wider than its target column can hold."""
    for column, width in widths.items():
        if column not in chunk.columns:
            continue
        lengths = chunk[column].dropna().astype(str).str.len()
        if lengths.empty:
            continue
        longest = int(lengths.max())
        if longest > width:
            offending = chunk.loc[lengths.idxmax(), column]
            raise StagingIntegrityError(
                f"Chunk {chunk_number}: value in column '{column}' is {longest} characters "
                f"but {STAGING_TABLE}.{column} holds only {width}. "
                f"Truncation would silently corrupt the data. Offending value: {offending!r}"
            )


def ingest_raw_csv_to_mysql(csv_path: str = CSV_FILE_PATH, batch_size: int = 10000) -> dict:
    """
    Reads the raw flight price CSV and ingests it into the MySQL staging table.

    The table is emptied and reloaded inside a single transaction, so the task
    is idempotent and can never leave staging partially populated.

    Returns a dict of load metadata for the downstream Airflow task.
    """
    headers = validate_csv_source(csv_path)
    logger.info(f"Connecting to MySQL staging database at {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}...")

    try:
        engine = get_mysql_engine()
        with engine.connect() as conn:
            # Timeout 0: a concurrent ingest is a bug, not something to queue behind.
            acquired = conn.execute(
                text("SELECT GET_LOCK(:name, 0)"), {"name": INGEST_LOCK_NAME}
            ).scalar()
            if acquired != 1:
                raise StagingIntegrityError(
                    f"Could not acquire ingest lock '{INGEST_LOCK_NAME}' -- another ingest is "
                    "already running against this staging table. Refusing to run concurrently, "
                    "because two overlapping loads would duplicate every row."
                )

            try:
                widths = get_staging_column_widths(conn)
                rows_read = 0
                chunk_number = 0

                # DELETE, not TRUNCATE: TRUNCATE is DDL in MySQL and forces an
                # implicit commit, which would break this transaction.
                with conn.begin():
                    deleted = conn.execute(text(f"DELETE FROM {STAGING_TABLE}")).rowcount
                    logger.info(f"Cleared {deleted:,} pre-existing rows from {STAGING_TABLE}.")

                    logger.info(f"Reading CSV from {csv_path} and inserting into MySQL staging...")
                    for chunk in pd.read_csv(csv_path, chunksize=batch_size, dtype=str):
                        chunk_number += 1
                        chunk = chunk.rename(columns=COLUMN_MAPPING)
                        # Retain only recognized columns
                        chunk = chunk[[col for col in COLUMN_MAPPING.values() if col in chunk.columns]]
                        check_chunk_fits_schema(chunk, widths, chunk_number)

                        chunk.to_sql(
                            STAGING_TABLE,
                            con=conn,
                            if_exists="append",
                            index=False,
                            method="multi",
                        )
                        rows_read += len(chunk)
                        logger.info(f"Ingested chunk {chunk_number}: {rows_read:,} rows staged so far.")

                if rows_read == 0:
                    raise StagingIntegrityError(
                        f"Raw CSV at {csv_path} contained a header but zero data rows."
                    )

                # Turns a silent duplication into a hard failure.
                staged = conn.execute(text(f"SELECT COUNT(*) FROM {STAGING_TABLE}")).scalar()
                if staged != rows_read:
                    raise StagingIntegrityError(
                        f"Staging row count mismatch: read {rows_read:,} rows from the CSV but "
                        f"{MYSQL_DB}.{STAGING_TABLE} holds {staged:,}. "
                        "A concurrent writer or a partial commit is the usual cause."
                    )
            finally:
                # Release even on failure, so a crash does not block the next run.
                conn.execute(text("SELECT RELEASE_LOCK(:name)"), {"name": INGEST_LOCK_NAME})

    except SQLAlchemyError as exc:
        raise StagingIntegrityError(
            f"Database failure while staging {csv_path} into "
            f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}: {exc}"
        ) from exc

    logger.info(f"Successfully staged {staged:,} records into MySQL (verified against source).")
    return {
        "staged_rows": staged,
        "rows_read": rows_read,
        "chunks": chunk_number,
        "source_columns": len(headers),
        "csv_path": csv_path,
    }


if __name__ == "__main__":
    ingest_raw_csv_to_mysql()
