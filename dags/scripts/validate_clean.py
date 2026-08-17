# This module extracts the staged records from MySQL, validates mandatory schema fields, strips string padding, imputes/recalculates numerical fare discrepancies, parses dates, and eliminates anomalies (e.g., negative or zero fares).

import os
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.getenv("MYSQL_USER", "staging_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "staging_password")
MYSQL_DB = os.getenv("MYSQL_DATABASE", "staging_db")


def extract_staged_data() -> pd.DataFrame:
    """Extracts raw unvalidated data from MySQL staging database."""
    connection_uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    engine = create_engine(connection_uri)
    query = "SELECT * FROM raw_flight_prices"
    logger.info("Extracting raw staged records from MySQL...")
    df = pd.read_sql(query, con=engine)
    logger.info(f"Retrieved {len(df):,} raw rows from MySQL staging.")
    return df


def validate_and_clean_flight_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies data quality, null-handling, type conversions, and business validation rules.
    """
    if df.empty:
        raise ValueError("Provided DataFrame is empty. Ingestion may have failed.")

    df = df.copy()

    # 1. Verify all required business columns exist
    required_cols = [
        "airline", "source", "source_name", "destination", "destination_name",
        "departure_datetime", "arrival_datetime", "duration_hrs", "stopovers",
        "class", "base_fare", "tax_and_surcharge", "total_fare", "seasonality"
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing mandatory columns in staged dataset: {missing_cols}")

    # 2. Rename columns to standardized analytical naming convention
    df = df.rename(columns={
        "source": "source_code",
        "destination": "destination_code",
        "class": "flight_class",
        "base_fare": "base_fare_bdt",
        "tax_and_surcharge": "tax_and_surcharge_bdt",
        "total_fare": "total_fare_bdt"
    })

    # 3. String standardization & whitespace stripping
    text_columns = [
        "airline", "source_code", "source_name", "destination_code",
        "destination_name", "stopovers", "aircraft_type", "flight_class",
        "booking_source", "seasonality"
    ]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(["nan", "None", ""], np.nan)

    # 4. Handle missing categorical attributes
    df["airline"] = df["airline"].fillna("Unknown Airline")
    df["source_code"] = df["source_code"].fillna("UNKNOWN")
    df["destination_code"] = df["destination_code"].fillna("UNKNOWN")
    df["aircraft_type"] = df["aircraft_type"].fillna("Standard Jet")
    df["booking_source"] = df["booking_source"].fillna("Direct")
    df["flight_class"] = df["flight_class"].fillna("Economy")
    df["stopovers"] = df["stopovers"].fillna("Direct")
    df["seasonality"] = df["seasonality"].fillna("Regular")

    # 5. Numerical cleaning, coercion, and formula enforcement
    df["base_fare_bdt"] = pd.to_numeric(df["base_fare_bdt"], errors="coerce").fillna(0.0)
    df["tax_and_surcharge_bdt"] = pd.to_numeric(df["tax_and_surcharge_bdt"], errors="coerce").fillna(0.0)
    df["duration_hrs"] = pd.to_numeric(df["duration_hrs"], errors="coerce").fillna(1.0)
    df["days_before_departure"] = pd.to_numeric(df["days_before_departure"], errors="coerce").fillna(0).astype(int)

    # Enforce formula: Total Fare = Base Fare + Tax & Surcharge
    df["total_fare_bdt"] = (df["base_fare_bdt"] + df["tax_and_surcharge_bdt"]).round(2)
    df["base_fare_bdt"] = df["base_fare_bdt"].round(2)
    df["tax_and_surcharge_bdt"] = df["tax_and_surcharge_bdt"].round(2)
    df["duration_hrs"] = df["duration_hrs"].round(2)

    # 6. Filter out invalid rows (fare must be strictly positive)
    initial_count = len(df)
    df = df[df["base_fare_bdt"] > 0]
    dropped_invalid_fares = initial_count - len(df)
    if dropped_invalid_fares > 0:
        logger.warning(f"Filtered out {dropped_invalid_fares} records with invalid/non-positive base fares.")

    # 7. Parse Datetimes and derive date/route helper fields
    df["departure_datetime"] = pd.to_datetime(df["departure_datetime"], errors="coerce")
    df["arrival_datetime"] = pd.to_datetime(df["arrival_datetime"], errors="coerce")

    # Drop records with invalid unparseable timestamps
    df = df.dropna(subset=["departure_datetime", "arrival_datetime"])
    df["flight_date"] = df["departure_datetime"].dt.date

    # Standardize route dimensions
    df["route_code"] = df["source_code"] + " -> " + df["destination_code"]
    df["route_name"] = df["source_name"] + " -> " + df["destination_name"]

    # Reorder columns to align exactly with Postgres target fact table
    fact_columns = [
        "airline", "source_code", "source_name", "destination_code", "destination_name",
        "route_code", "route_name", "departure_datetime", "arrival_datetime", "flight_date",
        "duration_hrs", "stopovers", "aircraft_type", "flight_class", "booking_source",
        "base_fare_bdt", "tax_and_surcharge_bdt", "total_fare_bdt", "seasonality",
        "days_before_departure"
    ]
    df_cleaned = df[fact_columns].reset_index(drop=True)
    logger.info(f"Data validation complete: {len(df_cleaned):,} high-quality records ready.")
    return df_cleaned