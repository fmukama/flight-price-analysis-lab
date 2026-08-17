#Ingests raw CSV into MySQL staging table
# This script reads the raw Kaggle CSV file and streams it into the MySQL staging database (staging_db.raw_flight_prices).

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# Set up logging for Airflow task execution visibility
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Default connection parameters
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.getenv("MYSQL_USER", "staging_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "staging_password")
MYSQL_DB = os.getenv("MYSQL_DATABASE", "staging_db")
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "/opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv")


def get_mysql_engine():
    """Constructs an SQLAlchemy engine for the MySQL staging database."""
    connection_uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    return create_engine(connection_uri, pool_pre_ping=True)


def ingest_raw_csv_to_mysql(csv_path: str = CSV_FILE_PATH, batch_size: int = 10000):
    """
    Reads the raw flight price CSV and ingests it into MySQL staging table.
    Truncates the staging table beforehand to enforce task idempotency.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Raw CSV dataset not found at expected path: {csv_path}")

    logger.info(f"Connecting to MySQL staging database at {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}...")
    engine = get_mysql_engine()

    # Truncate staging table prior to loading for idempotency
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw_flight_prices;"))
        logger.info("Truncated raw_flight_prices staging table.")

    # Read and map CSV headers to staging schema column names
    column_mapping = {
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
        "Days Before Departure": "days_before_departure"
    }

    logger.info(f"Reading CSV from {csv_path} and inserting into MySQL staging...")
    total_rows = 0

    for chunk in pd.read_csv(csv_path, chunksize=batch_size, dtype=str):
        chunk = chunk.rename(columns=column_mapping)
        # Retain only recognized columns
        chunk = chunk[[col for col in column_mapping.values() if col in chunk.columns]]
        chunk.to_sql("raw_flight_prices", con=engine, if_exists="append", index=False, method="multi")
        total_rows += len(chunk)
        logger.info(f"Ingested chunk: {total_rows} total rows staged so far.")

    logger.info(f"Successfully staged {total_rows:,} records into MySQL.")
    return total_rows


if __name__ == "__main__":
    ingest_raw_csv_to_mysql()