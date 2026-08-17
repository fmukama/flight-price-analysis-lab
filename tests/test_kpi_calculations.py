from dags.scripts.validate_clean import validate_and_clean_flight_data
from dags.scripts.compute_kpis import (
    compute_kpi_avg_fare_by_airline,
    compute_kpi_seasonal_fare_variation,
    compute_kpi_airline_booking_count,
    compute_kpi_popular_routes
)


def test_kpi_avg_fare_by_airline(sample_raw_df):
    clean_df = validate_and_clean_flight_data(sample_raw_df)
    kpi1 = compute_kpi_avg_fare_by_airline(clean_df)
    
    assert "airline" in kpi1.columns
    assert "avg_total_fare" in kpi1.columns
    # Biman has 2 rows: 5225 and 11000 -> Mean = 8112.50
    biman_row = kpi1[kpi1["airline"] == "Biman Bangladesh"].iloc[0]
    assert biman_row["avg_total_fare"] == 8112.50


def test_kpi_seasonal_fare_variation(sample_raw_df):
    clean_df = validate_and_clean_flight_data(sample_raw_df)
    kpi2 = compute_kpi_seasonal_fare_variation(clean_df)
    
    assert set(kpi2["seasonality"].values) == {"Peak", "Regular"}
    peak_row = kpi2[kpi2["seasonality"] == "Peak"].iloc[0]
    assert peak_row["flight_count"] == 3


def test_kpi_airline_booking_count_market_share(sample_raw_df):
    clean_df = validate_and_clean_flight_data(sample_raw_df)
    kpi3 = compute_kpi_airline_booking_count(clean_df)
    
    # 4 valid records total; Biman has 2 -> 50.0%
    assert kpi3["market_share_pct"].sum() == 100.00
    biman_share = kpi3[kpi3["airline"] == "Biman Bangladesh"]["market_share_pct"].iloc[0]
    assert biman_share == 50.00


def test_kpi_popular_routes_ranking(sample_raw_df):
    clean_df = validate_and_clean_flight_data(sample_raw_df)
    kpi4 = compute_kpi_popular_routes(clean_df)
    
    assert "route_rank" in kpi4.columns
    assert kpi4["route_rank"].iloc[0] == 1
    assert len(kpi4) > 0