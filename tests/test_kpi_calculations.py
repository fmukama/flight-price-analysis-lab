import pytest

from dags.scripts.validate_clean import validate_and_clean_flight_data
from dags.scripts.compute_kpis import (
    compute_kpi_avg_fare_by_airline,
    compute_kpi_seasonal_fare_variation,
    compute_kpi_airline_booking_count,
    compute_kpi_popular_routes,
    compute_all_kpis,
    KPIComputationError,
    PEAK_SEASONS,
)


def test_kpi_avg_fare_by_airline(clean_df):
    kpi1 = compute_kpi_avg_fare_by_airline(clean_df)

    assert "airline" in kpi1.columns
    assert "avg_total_fare" in kpi1.columns
    # Biman has 2 rows: 5225 and 11000 -> Mean = 8112.50
    biman_row = kpi1[kpi1["airline"] == "Biman Bangladesh"].iloc[0]
    assert biman_row["avg_total_fare"] == 8112.50
    assert biman_row["min_total_fare"] == 5225.00
    assert biman_row["max_total_fare"] == 11000.00


def test_kpi_seasonal_fare_variation(clean_df):
    kpi2 = compute_kpi_seasonal_fare_variation(clean_df)

    assert set(kpi2["seasonality"].values) == {"Eid", "Winter Holidays", "Regular"}
    eid_row = kpi2[kpi2["seasonality"] == "Eid"].iloc[0]
    assert eid_row["flight_count"] == 2
    # 3 of the 4 surviving bookings fall in a peak season (2 Eid + 1 Winter)
    assert int(kpi2.loc[kpi2["is_peak"], "flight_count"].sum()) == 3


def test_kpi_seasonal_peak_classification_and_uplift(clean_df):
    """
    The KPI must answer 'peak vs non-peak', not just group by season. Uplift is
    measured against the mean fare of non-peak bookings (the single Regular row
    at 4900 BDT).
    """
    kpi2 = compute_kpi_seasonal_fare_variation(clean_df).set_index("seasonality")

    assert bool(kpi2.loc["Eid", "is_peak"]) is True
    assert bool(kpi2.loc["Winter Holidays", "is_peak"]) is True
    assert bool(kpi2.loc["Regular", "is_peak"]) is False

    # Baseline is Regular itself, so its own uplift is zero
    assert kpi2.loc["Regular", "avg_fare_uplift_pct"] == 0.00
    # Eid averages 8112.50 against a 4900 baseline -> +65.56%
    assert kpi2.loc["Eid", "avg_fare_uplift_pct"] == 65.56
    # Winter Holidays averages 4450 -> cheaper than baseline, so negative
    assert kpi2.loc["Winter Holidays", "avg_fare_uplift_pct"] == -9.18


def test_peak_seasons_match_the_dataset_labels():
    """Guards against a typo silently classifying every season as non-peak."""
    assert PEAK_SEASONS == {"Eid", "Hajj", "Winter Holidays"}


def test_kpi_airline_booking_count_market_share(clean_df):
    kpi3 = compute_kpi_airline_booking_count(clean_df)

    # 4 valid records total; Biman has 2 -> 50.0%
    assert kpi3["market_share_pct"].sum() == 100.00
    biman_share = kpi3[kpi3["airline"] == "Biman Bangladesh"]["market_share_pct"].iloc[0]
    assert biman_share == 50.00
    assert kpi3["booking_count"].sum() == len(clean_df)


def test_kpi_popular_routes_ranking(clean_df):
    kpi4 = compute_kpi_popular_routes(clean_df)

    assert "route_rank" in kpi4.columns
    assert kpi4["route_rank"].iloc[0] == 1
    assert len(kpi4) > 0
    # Ranks must be dense, unique, and 1-based so they can key the KPI table
    assert list(kpi4["route_rank"]) == list(range(1, len(kpi4) + 1))
    assert kpi4["route_code"].is_unique


def test_kpi_popular_routes_ties_break_deterministically(clean_df):
    """
    All four sample routes have one booking each. Ties must resolve on route_code
    so ranks do not shuffle between otherwise identical runs.
    """
    first = compute_kpi_popular_routes(clean_df)
    second = compute_kpi_popular_routes(clean_df)

    assert list(first["route_code"]) == list(second["route_code"])
    assert list(first["route_code"]) == ["CGP -> DAC", "CXB -> DAC", "DAC -> CXB", "DAC -> SPD"]


def test_kpi_popular_routes_excludes_invalid_routes(quality_issues_df):
    """Malformed or self-referencing routes must not pollute the ranking."""
    cleaned, report = validate_and_clean_flight_data(quality_issues_df)
    assert report["invalid_routes_flagged"] == 3

    kpi4 = compute_kpi_popular_routes(cleaned)

    # 5 surviving rows, 3 flagged invalid -> only 2 routes are rankable
    assert len(kpi4) == 2
    assert set(kpi4["route_code"]) == {"DAC -> CXB", "CGP -> DAC"}


def test_compute_all_kpis_returns_every_target_table(clean_df):
    kpis = compute_all_kpis(clean_df)

    assert set(kpis) == {
        "kpi_avg_fare_by_airline",
        "kpi_seasonal_fare_variation",
        "kpi_airline_booking_count",
        "kpi_popular_routes",
    }
    assert all(not df.empty for df in kpis.values())


def test_kpis_reject_empty_input(clean_df):
    """An empty frame must raise, not quietly produce empty KPI tables."""
    with pytest.raises(KPIComputationError):
        compute_all_kpis(clean_df.iloc[0:0])


def test_kpis_name_the_missing_column(clean_df):
    """A dropped column must be reported per-KPI, not as a bare pandas KeyError."""
    with pytest.raises(KPIComputationError, match="total_fare_bdt"):
        compute_kpi_seasonal_fare_variation(clean_df.drop(columns=["total_fare_bdt"]))
