import pytest

from dags.scripts.validate_clean import (
    validate_and_clean_flight_data,
    DataValidationError,
    CATEGORICAL_DEFAULTS,
)


def test_negative_fare_removal(clean_df):
    """Ensures records with negative or zero base fares are filtered out."""
    # The fixture had 5 rows, 1 had a base_fare of -500.00
    assert len(clean_df) == 4
    assert (clean_df["base_fare_bdt"] > 0).all()


def test_total_fare_formula_enforcement(clean_df):
    """Verifies that total_fare_bdt is strictly equal to base_fare_bdt + tax_and_surcharge_bdt."""
    expected_totals = clean_df["base_fare_bdt"] + clean_df["tax_and_surcharge_bdt"]
    assert (clean_df["total_fare_bdt"] == expected_totals).all()


def test_string_trimming_and_routes(clean_df):
    """Verifies whitespace is trimmed and routes are correctly formatted."""
    # Check trimmed airline
    assert "Air Astra" in clean_df["airline"].values
    assert "  Air Astra  " not in clean_df["airline"].values
    # Check derived route codes
    assert "DAC -> CXB" in clean_df["route_code"].values


def test_report_accounts_for_every_row(validation_report):
    """The report must reconcile: rows_in == rows_out + rows_dropped."""
    assert validation_report["rows_in"] == 5
    assert validation_report["rows_out"] == 4
    assert validation_report["rows_dropped"] == 1
    assert validation_report["dropped_non_positive_fares"] == 1


def test_clean_sample_has_no_spurious_flags(clean_df, validation_report):
    """The sample rows are internally consistent, so nothing should be flagged."""
    assert validation_report["fare_mismatches_corrected"] == 0
    assert validation_report["invalid_routes_flagged"] == 0
    assert not clean_df["fare_mismatch_flag"].any()
    assert not clean_df["invalid_route_flag"].any()


def test_fare_mismatch_is_flagged_not_silently_overwritten(quality_issues_df):
    """
    The dataset's largest real quality issue is Total Fare != Base + Tax.
    The corrected value must be used AND the original preserved and flagged.
    """
    cleaned, report = validate_and_clean_flight_data(quality_issues_df)

    assert report["fare_mismatches_corrected"] == 1
    mismatched = cleaned[cleaned["fare_mismatch_flag"]]
    assert len(mismatched) == 1

    row = mismatched.iloc[0]
    assert row["base_fare_bdt"] == 1000.00
    assert row["tax_and_surcharge_bdt"] == 100.00
    assert row["total_fare_bdt"] == 1100.00        # recomputed
    assert row["total_fare_original_bdt"] == 5000.00  # source value retained
    assert report["largest_fare_discrepancy_bdt"] == 3900.00


def test_invalid_routes_are_flagged_with_reasons(quality_issues_df):
    """Malformed codes, self-referencing routes, and missing cities are flagged."""
    cleaned, report = validate_and_clean_flight_data(quality_issues_df)

    assert report["invalid_routes_flagged"] == 3
    assert report["invalid_route_reasons"] == {
        "malformed_airport_code": 1,
        "source_equals_destination": 1,
        "missing_city_name": 1,
    }
    assert int(cleaned["invalid_route_flag"].sum()) == 3


def test_unusable_rows_are_dropped_and_counted(quality_issues_df):
    """A bad timestamp and a non-numeric fare each cost exactly one row."""
    cleaned, report = validate_and_clean_flight_data(quality_issues_df)

    assert report["rows_in"] == 7
    assert report["dropped_unparseable_fares"] == 1
    assert report["dropped_unparseable_dates"] == 1
    assert report["non_numeric_values"] == {"base_fare_bdt": 1}
    assert report["rows_out"] == 5
    assert len(cleaned) == 5


def test_missing_categoricals_use_non_colliding_sentinel(quality_issues_df):
    """
    An imputed value must never masquerade as a real category, otherwise imputed
    rows become indistinguishable from genuine ones in the KPIs.
    """
    cleaned, report = validate_and_clean_flight_data(quality_issues_df)

    assert report["categorical_values_imputed"] == {"source_name": 1}
    assert CATEGORICAL_DEFAULTS["source_name"] == "Unknown"
    assert (cleaned["source_name"] == "Unknown").sum() == 1
    # "Regular" is a real seasonality value and must not be used as a filler
    assert CATEGORICAL_DEFAULTS["seasonality"] == "Unknown"


def test_missing_required_column_raises_clearly(sample_raw_df):
    """A schema drift must fail with a named column, not a bare KeyError."""
    broken = sample_raw_df.drop(columns=["base_fare"])
    with pytest.raises(DataValidationError, match="base_fare"):
        validate_and_clean_flight_data(broken)


def test_columns_used_but_not_declared_are_still_validated(sample_raw_df):
    """
    aircraft_type / booking_source / days_before_departure are read by the
    cleaner, so dropping one must raise the same clear error rather than
    exploding later with a KeyError.
    """
    for column in ["aircraft_type", "booking_source", "days_before_departure"]:
        with pytest.raises(DataValidationError, match=column):
            validate_and_clean_flight_data(sample_raw_df.drop(columns=[column]))


def test_empty_input_raises(sample_raw_df):
    """An empty staging read must fail loudly, not produce empty analytics."""
    with pytest.raises(DataValidationError):
        validate_and_clean_flight_data(sample_raw_df.iloc[0:0])
