# Extracts staged records from MySQL, validates the schema, coerces types,
# reconciles fare arithmetic, and drops rows that cannot support analysis.
#
# Two rules shape this module:
#   * Flag, do not silently rewrite. ~4.4% of source rows have
#     Total Fare != Base + Tax (largest gap ~93,000 BDT); the original is kept
#     alongside a flag instead of being overwritten and lost.
#   * Never impute into a real category. Filling missing `seasonality` with
#     "Regular" would make imputed rows indistinguishable from genuine ones.

import os
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# NOTE: MYSQL_PORT is the in-network port (3306), not the host-published one.
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.getenv("MYSQL_USER", "staging_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "staging_password")
MYSQL_DB = os.getenv("MYSQL_DATABASE", "staging_db")

STAGING_TABLE = "raw_flight_prices"

# Every column this module reads. Checked up front so a schema drift produces
# one clear error instead of a KeyError from deep inside the cleaning steps.
REQUIRED_STAGING_COLUMNS = [
    "airline", "source", "source_name", "destination", "destination_name",
    "departure_datetime", "arrival_datetime", "duration_hrs", "stopovers",
    "aircraft_type", "class", "booking_source",
    "base_fare", "tax_and_surcharge", "total_fare",
    "seasonality", "days_before_departure",
]

# Categorical columns and the sentinel used when a value is missing. The
# sentinel is deliberately NOT a value that occurs naturally in the data.
CATEGORICAL_DEFAULTS = {
    "airline": "Unknown",
    "source_code": "UNK",
    "source_name": "Unknown",
    "destination_code": "UNK",
    "destination_name": "Unknown",
    "aircraft_type": "Unknown",
    "booking_source": "Unknown",
    "flight_class": "Unknown",
    "stopovers": "Unknown",
    "seasonality": "Unknown",
}

# Tolerance when comparing the CSV's Total Fare against base + tax, in BDT.
FARE_RECONCILIATION_TOLERANCE = 0.01

# Final fact table column order, matching flight_analytics.fct_flight_prices_cleaned
FACT_COLUMNS = [
    "airline", "source_code", "source_name", "destination_code", "destination_name",
    "route_code", "route_name", "departure_datetime", "arrival_datetime", "flight_date",
    "duration_hrs", "stopovers", "aircraft_type", "flight_class", "booking_source",
    "base_fare_bdt", "tax_and_surcharge_bdt", "total_fare_bdt", "total_fare_original_bdt",
    "fare_mismatch_flag", "invalid_route_flag", "seasonality", "days_before_departure",
]


class DataValidationError(ValueError):
    """Raised when staged data cannot be validated into an analysable shape."""


def extract_staged_data() -> pd.DataFrame:
    """Extracts raw unvalidated data from MySQL staging database."""
    connection_uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    logger.info(f"Extracting raw staged records from {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}...")
    try:
        engine = create_engine(connection_uri, pool_pre_ping=True)
        df = pd.read_sql(f"SELECT * FROM {STAGING_TABLE}", con=engine)
    except SQLAlchemyError as exc:
        raise DataValidationError(
            f"Could not read {MYSQL_DB}.{STAGING_TABLE} from {MYSQL_HOST}:{MYSQL_PORT}: {exc}"
        ) from exc

    if df.empty:
        raise DataValidationError(
            f"{MYSQL_DB}.{STAGING_TABLE} is empty. The ingest task must run first."
        )

    logger.info(f"Retrieved {len(df):,} raw rows from MySQL staging.")
    return df


def validate_and_clean_flight_data(df: pd.DataFrame):
    """
    Applies data quality, null-handling, type conversion, and business validation
    rules to the staged dataset.

    Returns a tuple of (cleaned DataFrame, validation report dict). The report is
    logged by the DAG and records every correction and every dropped row, so the
    pipeline's effect on the data is auditable rather than implicit.
    """
    if df is None or df.empty:
        raise DataValidationError("Provided DataFrame is empty. Ingestion may have failed.")

    df = df.copy()
    report = {"rows_in": len(df)}

    # 1. Verify all required business columns exist before touching any of them
    missing_cols = [col for col in REQUIRED_STAGING_COLUMNS if col not in df.columns]
    if missing_cols:
        raise DataValidationError(
            f"Missing mandatory columns in staged dataset: {missing_cols}. "
            f"Present columns: {sorted(df.columns)}"
        )

    # 2. Rename columns to standardized analytical naming convention
    df = df.rename(columns={
        "source": "source_code",
        "destination": "destination_code",
        "class": "flight_class",
        "base_fare": "base_fare_bdt",
        "tax_and_surcharge": "tax_and_surcharge_bdt",
        "total_fare": "total_fare_bdt",
    })

    # 3. String standardization, whitespace stripping, and null normalisation.
    #    Staging stores everything as text, so "nan"/"None"/"" all mean missing.
    for col in CATEGORICAL_DEFAULTS:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace(["nan", "None", "NaN", "null", ""], np.nan)

    # 4. Handle missing categorical attributes with auditable sentinels
    imputed = {}
    for col, default in CATEGORICAL_DEFAULTS.items():
        n_missing = int(df[col].isna().sum())
        if n_missing:
            imputed[col] = n_missing
        df[col] = df[col].fillna(default)
    report["categorical_values_imputed"] = imputed

    # 5. Numeric coercion. Values that are not parseable become NaN and are
    #    counted, rather than being silently turned into zero.
    numeric_cols = ["base_fare_bdt", "tax_and_surcharge_bdt", "duration_hrs", "days_before_departure"]
    non_numeric = {}
    for col in numeric_cols:
        coerced = pd.to_numeric(df[col], errors="coerce")
        n_bad = int(coerced.isna().sum() - df[col].isna().sum())
        if n_bad > 0:
            non_numeric[col] = n_bad
        df[col] = coerced
    report["non_numeric_values"] = non_numeric

    # A fare that will not parse cannot be analysed, so those rows go. Duration
    # and lead time are descriptive only, so they are imputed with the median.
    for col in ["duration_hrs", "days_before_departure"]:
        if df[col].isna().any():
            median = df[col].median()
            fallback = 0 if pd.isna(median) else median
            logger.warning(
                f"Imputing {int(df[col].isna().sum())} missing '{col}' value(s) with median {fallback}."
            )
            df[col] = df[col].fillna(fallback)

    n_unparseable_fares = int(df["base_fare_bdt"].isna().sum() + df["tax_and_surcharge_bdt"].isna().sum())
    df = df.dropna(subset=["base_fare_bdt", "tax_and_surcharge_bdt"])
    report["dropped_unparseable_fares"] = n_unparseable_fares

    # 6. Reconcile the fare arithmetic: Total Fare = Base Fare + Tax & Surcharge.
    #    The CSV's own total is kept so the correction stays inspectable.
    df["total_fare_original_bdt"] = pd.to_numeric(df["total_fare_bdt"], errors="coerce")
    recomputed = (df["base_fare_bdt"] + df["tax_and_surcharge_bdt"]).round(2)
    df["fare_mismatch_flag"] = (
        df["total_fare_original_bdt"].isna()
        | ((df["total_fare_original_bdt"] - recomputed).abs() > FARE_RECONCILIATION_TOLERANCE)
    )
    df["total_fare_bdt"] = recomputed

    n_mismatch = int(df["fare_mismatch_flag"].sum())
    report["fare_mismatches_corrected"] = n_mismatch
    if n_mismatch:
        worst = float((df["total_fare_original_bdt"] - df["total_fare_bdt"]).abs().max())
        report["largest_fare_discrepancy_bdt"] = round(worst, 2)
        logger.warning(
            f"{n_mismatch:,} of {len(df):,} rows ({n_mismatch / len(df):.2%}) had "
            f"Total Fare != Base + Tax. Recomputed and flagged; largest gap {worst:,.2f} BDT. "
            "Original values retained in total_fare_original_bdt."
        )

    for col in ["base_fare_bdt", "tax_and_surcharge_bdt", "total_fare_bdt", "total_fare_original_bdt", "duration_hrs"]:
        df[col] = df[col].round(2)
    df["days_before_departure"] = df["days_before_departure"].astype(int)

    # 7. Filter out invalid rows (fare must be strictly positive)
    initial_count = len(df)
    df = df[df["base_fare_bdt"] > 0]
    report["dropped_non_positive_fares"] = initial_count - len(df)
    if report["dropped_non_positive_fares"] > 0:
        logger.warning(
            f"Filtered out {report['dropped_non_positive_fares']} record(s) with "
            "invalid/non-positive base fares."
        )

    # 8. Parse datetimes; rows without a usable timestamp cannot be analysed
    df["departure_datetime"] = pd.to_datetime(df["departure_datetime"], errors="coerce")
    df["arrival_datetime"] = pd.to_datetime(df["arrival_datetime"], errors="coerce")

    before_dates = len(df)
    df = df.dropna(subset=["departure_datetime", "arrival_datetime"])
    report["dropped_unparseable_dates"] = before_dates - len(df)
    if report["dropped_unparseable_dates"] > 0:
        logger.warning(
            f"Dropped {report['dropped_unparseable_dates']} record(s) with unparseable timestamps."
        )

    if df.empty:
        raise DataValidationError(
            "Every staged row failed validation; nothing remains to load. "
            f"Report so far: {report}"
        )

    df["flight_date"] = df["departure_datetime"].dt.date

    # 9. Validate the route. An airport code must be exactly three letters, the
    #    names must be present, and an origin cannot equal its destination.
    #    These are flagged rather than dropped: the booking is still real, but
    #    the row should not be trusted in route analytics.
    code_pattern = r"^[A-Za-z]{3}$"
    bad_code = (
        ~df["source_code"].str.match(code_pattern, na=False)
        | ~df["destination_code"].str.match(code_pattern, na=False)
    )
    same_endpoint = df["source_code"].str.upper() == df["destination_code"].str.upper()
    unknown_city = (
        (df["source_name"] == CATEGORICAL_DEFAULTS["source_name"])
        | (df["destination_name"] == CATEGORICAL_DEFAULTS["destination_name"])
    )
    df["invalid_route_flag"] = bad_code | same_endpoint | unknown_city

    report["invalid_routes_flagged"] = int(df["invalid_route_flag"].sum())
    report["invalid_route_reasons"] = {
        "malformed_airport_code": int(bad_code.sum()),
        "source_equals_destination": int(same_endpoint.sum()),
        "missing_city_name": int(unknown_city.sum()),
    }
    if report["invalid_routes_flagged"]:
        logger.warning(
            f"Flagged {report['invalid_routes_flagged']} record(s) with invalid route data: "
            f"{report['invalid_route_reasons']}"
        )

    # Normalise codes to upper case before deriving route dimensions
    df["source_code"] = df["source_code"].str.upper()
    df["destination_code"] = df["destination_code"].str.upper()
    df["route_code"] = df["source_code"] + " -> " + df["destination_code"]
    df["route_name"] = df["source_name"] + " -> " + df["destination_name"]

    # 10. Sanity check: an arrival before its departure indicates a bad record
    n_negative_duration = int((df["arrival_datetime"] < df["departure_datetime"]).sum())
    report["arrivals_before_departure"] = n_negative_duration
    if n_negative_duration:
        logger.warning(f"{n_negative_duration} record(s) arrive before they depart.")

    df_cleaned = df[FACT_COLUMNS].reset_index(drop=True)
    report["rows_out"] = len(df_cleaned)
    report["rows_dropped"] = report["rows_in"] - report["rows_out"]

    logger.info(
        f"Data validation complete: {report['rows_out']:,} of {report['rows_in']:,} records "
        f"retained ({report['rows_dropped']:,} dropped, {n_mismatch:,} fares corrected)."
    )
    return df_cleaned, report
