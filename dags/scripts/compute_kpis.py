# This module computes the 4 required business KPIs and returns aggregated
# Pandas DataFrames matching the PostgreSQL schemas.
#
# Every KPI validates its inputs before aggregating, because a groupby on a
# missing or wrongly-typed column fails deep in pandas with an error that says
# nothing about which KPI broke or why.

import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Seasons treated as peak demand periods. Everything else is the non-peak
# baseline that peak fares are measured against. Derived from the dataset's
# own Seasonality labels: Regular, Winter Holidays, Hajj, Eid.
PEAK_SEASONS = {"Eid", "Hajj", "Winter Holidays"}


class KPIComputationError(ValueError):
    """Raised when the cleaned dataset cannot support a required KPI."""


def _require_columns(df: pd.DataFrame, columns: list, kpi_name: str) -> None:
    """Fails with a KPI-specific message rather than a bare pandas KeyError."""
    if df is None or df.empty:
        raise KPIComputationError(f"{kpi_name}: received an empty DataFrame.")
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KPIComputationError(
            f"{kpi_name}: cleaned dataset is missing required column(s) {missing}. "
            f"Present columns: {sorted(df.columns)}"
        )


def compute_kpi_avg_fare_by_airline(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPI 1: Mean base fare, tax/surcharge, and total fare grouped by airline.
    """
    logger.info("Computing KPI 1: Average Fare by Airline...")
    _require_columns(
        df,
        ["airline", "base_fare_bdt", "tax_and_surcharge_bdt", "total_fare_bdt"],
        "KPI 1 (avg fare by airline)",
    )

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

    logger.info(f"KPI 1: aggregated {len(kpi_df)} airline(s).")
    return kpi_df.sort_values(by="avg_total_fare", ascending=False).reset_index(drop=True)


def compute_kpi_seasonal_fare_variation(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPI 2: Seasonal fare variation, expressed as a peak vs non-peak comparison.

    Each season is labelled peak or non-peak and its average total fare is
    expressed as a percentage uplift over the non-peak baseline, which is what
    makes the seasons comparable rather than just separately aggregated.
    """
    logger.info("Computing KPI 2: Seasonal Fare Variation (peak vs non-peak)...")
    _require_columns(df, ["seasonality", "total_fare_bdt"], "KPI 2 (seasonal fare variation)")

    kpi_df = df.groupby("seasonality").agg(
        flight_count=("total_fare_bdt", "count"),
        avg_total_fare=("total_fare_bdt", "mean"),
        median_total_fare=("total_fare_bdt", "median"),
        min_total_fare=("total_fare_bdt", "min"),
        max_total_fare=("total_fare_bdt", "max")
    ).reset_index()

    kpi_df["is_peak"] = kpi_df["seasonality"].isin(PEAK_SEASONS)

    # Baseline: the mean fare across all non-peak bookings.
    non_peak = df[~df["seasonality"].isin(PEAK_SEASONS)]
    if non_peak.empty:
        baseline = float(df["total_fare_bdt"].mean())
        logger.warning(
            "No non-peak bookings present; using the overall mean fare as the "
            "comparison baseline, so uplift figures are not a true peak/non-peak split."
        )
    else:
        baseline = float(non_peak["total_fare_bdt"].mean())

    if baseline <= 0:
        raise KPIComputationError(
            f"KPI 2: non-peak baseline fare is {baseline}, so uplift cannot be computed."
        )

    kpi_df["avg_fare_uplift_pct"] = (
        (kpi_df["avg_total_fare"] - baseline) / baseline * 100
    ).round(2)

    for col in ["avg_total_fare", "median_total_fare", "min_total_fare", "max_total_fare"]:
        kpi_df[col] = kpi_df[col].round(2)

    logger.info(
        f"KPI 2: non-peak baseline {baseline:,.2f} BDT across "
        f"{len(kpi_df)} season(s); {int(kpi_df['is_peak'].sum())} classified peak."
    )
    return kpi_df.sort_values(by="avg_total_fare", ascending=False).reset_index(drop=True)


def compute_kpi_airline_booking_count(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPI 3: Total number of bookings and percentage market share per airline.
    """
    logger.info("Computing KPI 3: Booking Count by Airline...")
    _require_columns(df, ["airline"], "KPI 3 (booking count by airline)")

    total_market_bookings = len(df)
    if total_market_bookings == 0:
        raise KPIComputationError("KPI 3: no bookings to count.")

    kpi_df = df.groupby("airline").agg(
        booking_count=("airline", "count")
    ).reset_index()

    kpi_df["market_share_pct"] = ((kpi_df["booking_count"] / total_market_bookings) * 100).round(2)

    logger.info(f"KPI 3: {total_market_bookings:,} bookings across {len(kpi_df)} airline(s).")
    return kpi_df.sort_values(by="booking_count", ascending=False).reset_index(drop=True)


def compute_kpi_popular_routes(df: pd.DataFrame) -> pd.DataFrame:
    """
    KPI 4: Source-destination pairs ranked by booking volume, with average fares.

    Rows flagged as having invalid route data are excluded, since a malformed
    airport code or a self-referencing route would pollute the ranking.
    """
    logger.info("Computing KPI 4: Most Popular Routes...")
    _require_columns(
        df,
        ["route_code", "source_name", "destination_name", "total_fare_bdt"],
        "KPI 4 (popular routes)",
    )

    routable = df
    if "invalid_route_flag" in df.columns:
        excluded = int(df["invalid_route_flag"].sum())
        if excluded:
            logger.warning(f"KPI 4: excluding {excluded:,} booking(s) flagged with invalid route data.")
            routable = df[~df["invalid_route_flag"]]

    if routable.empty:
        raise KPIComputationError("KPI 4: every booking was excluded as having an invalid route.")

    kpi_df = routable.groupby(["route_code", "source_name", "destination_name"]).agg(
        booking_count=("route_code", "count"),
        avg_total_fare=("total_fare_bdt", "mean")
    ).reset_index()

    kpi_df["avg_total_fare"] = kpi_df["avg_total_fare"].round(2)

    # Deterministic ordering: volume first, then route code so that ties do not
    # shuffle ranks between otherwise identical runs.
    kpi_df = kpi_df.sort_values(
        by=["booking_count", "route_code"], ascending=[False, True]
    ).reset_index(drop=True)

    # Add ranking index column (1-based index)
    kpi_df["route_rank"] = kpi_df.index + 1

    logger.info(f"KPI 4: ranked {len(kpi_df)} route(s).")
    # Reorder columns to match PostgreSQL target schema
    return kpi_df[["route_rank", "route_code", "source_name", "destination_name", "booking_count", "avg_total_fare"]]


def compute_all_kpis(df: pd.DataFrame) -> dict:
    """
    Calculates all 4 KPIs and returns them as a dictionary of DataFrames keyed by
    their PostgreSQL target table name.
    """
    if df is None or df.empty:
        raise KPIComputationError("Cannot compute KPIs from an empty cleaned dataset.")

    kpis = {
        "kpi_avg_fare_by_airline": compute_kpi_avg_fare_by_airline(df),
        "kpi_seasonal_fare_variation": compute_kpi_seasonal_fare_variation(df),
        "kpi_airline_booking_count": compute_kpi_airline_booking_count(df),
        "kpi_popular_routes": compute_kpi_popular_routes(df),
    }

    # An empty KPI table almost always means an upstream filter removed
    # everything; surface it here rather than writing an empty table to Postgres.
    empty = [name for name, kpi_df in kpis.items() if kpi_df.empty]
    if empty:
        raise KPIComputationError(f"KPI computation produced empty result(s): {empty}")

    return kpis
