from dags.scripts.validate_clean import validate_and_clean_flight_data


def test_negative_fare_removal(sample_raw_df):
    """Ensures records with negative or zero base fares are filtered out."""
    clean_df = validate_and_clean_flight_data(sample_raw_df)
    # The fixture had 5 rows, 1 had a base_fare of -500.00
    assert len(clean_df) == 4
    assert (clean_df["base_fare_bdt"] > 0).all()


def test_total_fare_formula_enforcement(sample_raw_df):
    """Verifies that total_fare_bdt is strictly equal to base_fare_bdt + tax_and_surcharge_bdt."""
    clean_df = validate_and_clean_flight_data(sample_raw_df)
    expected_totals = clean_df["base_fare_bdt"] + clean_df["tax_and_surcharge_bdt"]
    assert (clean_df["total_fare_bdt"] == expected_totals).all()


def test_string_trimming_and_routes(sample_raw_df):
    """Verifies whitespace is trimmed and routes are correctly formatted."""
    clean_df = validate_and_clean_flight_data(sample_raw_df)
    # Check trimmed airline
    assert "Air Astra" in clean_df["airline"].values
    assert "  Air Astra  " not in clean_df["airline"].values
    # Check derived route codes
    assert "DAC -> CXB" in clean_df["route_code"].values