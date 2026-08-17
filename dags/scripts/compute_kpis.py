# This module computes the 4 required business KPIs and returns aggregated Pandas DataFrames matching the PostgreSQL schemas.

import logging
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def compute_kpi_avg_fare_by_airline(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPI 1: Mean base fare, tax/surcharge, and total fare grouped by airline.
    """
    logger.info("Computing KPI 1: Average Fare by Airline...")
    kpi_df = df.groupby("airline").agg(
        avg_base_fare=("base_fare_bdt", "mean"),
        avg_tax_surcharge=("tax_and_surcharge_bdt", "mean"),
        avg_total_fare=("total_fare_bdt", "mean"),
        min_total_fare=("total_fare_bdt", "min"),
        max_total_fare=("total_fare_bdt", "max")
    ).reset_index()

    # Round monetary figures
    for col in ["avg_base_fare", "avg_tax_surcharge", "avg_total_fare", "min_total_fare", "max_total_fare"]:
        kpi_df[col] = kpi_df[col].round(2)

    return kpi_df.sort_values(by="avg_total_fare", ascending=False)


def compute_kpi_seasonal_fare_variation(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPI 2: Compare flight volume, mean, median, min, and max total fares across seasonality types.
    """
    logger.info("Computing KPI 2: Seasonal Fare Variation...")
    kpi_df = df.groupby("seasonality").agg(
        flight_count=("total_fare_bdt", "count"),
        avg_total_fare=("total_fare_bdt", "mean"),
        median_total_fare=("total_fare_bdt", "median"),
        min_total_fare=("total_fare_bdt", "min"),
        max_total_fare=("total_fare_bdt", "max")
    ).reset_index()

    for col in ["avg_total_fare", "median_total_fare", "min_total_fare", "max_total_fare"]:
        kpi_df[col] = kpi_df[col].round(2)

    return kpi_df.sort_values(by="avg_total_fare", ascending=False)


def compute_kpi_airline_booking_count(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPI 3: Total number of bookings and percentage market share per airline.
    """
    logger.info("Computing KPI 3: Booking Count by Airline...")
    total_market_bookings = len(df)
    
    kpi_df = df.groupby("airline").agg(
        booking_count=("airline", "count")
    ).reset_index()

    kpi_df["market_share_pct"] = ((kpi_df["booking_count"] / total_market_bookings) * 100).round(2)
    return kpi_df.sort_values(by="booking_count", ascending=False)


def compute_kpi_popular_routes(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPI 4: Top source-destination pairs ranked by total booking volume with average ticket prices.
    """
    logger.info("Computing KPI 4: Most Popular Routes...")
    kpi_df = df.groupby(["route_code", "source_name", "destination_name"]).agg(
        booking_count=("route_code", "count"),
        avg_total_fare=("total_fare_bdt", "mean")
    ).reset_index()

    kpi_df["avg_total_fare"] = kpi_df["avg_total_fare"].round(2)
    kpi_df = kpi_df.sort_values(by="booking_count", ascending=False).reset_index(drop=True)
    
    # Add ranking index column (1-based index)
    kpi_df["route_rank"] = kpi_df.index + 1
    
    # Reorder columns to match PostgreSQL target schema
    return kpi_df[["route_rank", "route_code", "source_name", "destination_name", "booking_count", "avg_total_fare"]]


def compute_all_kpis(df: pd.DataFrame) -> dict:
    """Calculates all 4 KPIs and returns them as a dictionary of DataFrames."""
    return {
        "kpi_avg_fare_by_airline": compute_kpi_avg_fare_by_airline(df),
        "kpi_seasonal_fare_variation": compute_kpi_seasonal_fare_variation(df),
        "kpi_airline_booking_count": compute_kpi_airline_booking_count(df),
        "kpi_popular_routes": compute_kpi_popular_routes(df)
    }