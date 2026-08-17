import pytest
import pandas as pd

from dags.scripts.validate_clean import validate_and_clean_flight_data


@pytest.fixture
def sample_raw_df():
    """
    Returns a realistic raw staging DataFrame with edge cases and dirty values.

    Every column is a string, mirroring the all-VARCHAR staging table. The
    Seasonality labels are the dataset's real ones (Regular / Eid / Winter
    Holidays) so the peak-vs-non-peak KPI is exercised for real.
    """
    return pd.DataFrame({
        "airline": ["Biman Bangladesh", "US-Bangla", "Novoair", "  Air Astra  ", "Biman Bangladesh"],
        "source": ["DAC", "CGP", "ZYL", "DAC", "CXB"],
        "source_name": [
            "Hazrat Shahjalal International Airport",
            "Shah Amanat International Airport",
            "Osmani International Airport",
            "Hazrat Shahjalal International Airport",
            "Cox's Bazar Airport"
        ],
        "destination": ["CXB", "DAC", "DAC", "SPD", "DAC"],
        "destination_name": [
            "Cox's Bazar Airport",
            "Hazrat Shahjalal International Airport",
            "Hazrat Shahjalal International Airport",
            "Saidpur Airport",
            "Hazrat Shahjalal International Airport"
        ],
        "departure_datetime": [
            "2026-08-01 08:30:00",
            "2026-08-01 12:00:00",
            "2026-08-02 14:15:00",
            "2026-08-02 18:00:00",
            "2026-08-03 20:00:00"
        ],
        "arrival_datetime": [
            "2026-08-01 09:30:00",
            "2026-08-01 12:50:00",
            "2026-08-02 15:05:00",
            "2026-08-02 18:55:00",
            "2026-08-03 21:05:00"
        ],
        "duration_hrs": ["1.00", "0.83", "0.83", "0.91", "1.08"],
        "stopovers": ["Direct", "Direct", "Direct", "Direct", "Direct"],
        "aircraft_type": ["Boeing 737", "ATR 72", "ATR 72", "ATR 72", "Boeing 777"],
        "class": ["Economy", "Economy", "Economy", "Economy", "Business"],
        "booking_source": ["Online Website", "Travel Agency", "Online Website", "Direct Booking", "Online Website"],
        "base_fare": ["4500.00", "3800.00", "-500.00", "4200.00", "9500.00"],  # Row index 2 has negative fare
        "tax_and_surcharge": ["725.00", "650.00", "100.00", "700.00", "1500.00"],
        "total_fare": ["5225.00", "4450.00", "-400.00", "4900.00", "11000.00"],
        "seasonality": ["Eid", "Winter Holidays", "Regular", "Regular", "Eid"],
        "days_before_departure": ["7", "14", "1", "30", "3"]
    })


@pytest.fixture
def quality_issues_df():
    """
    Raw staging rows that each trip exactly one validation rule, so the checks
    can be asserted independently.

      index 0: clean control row
      index 1: Total Fare disagrees with Base + Tax  -> flagged, recomputed
      index 2: malformed airport code ("DACC")       -> route flagged
      index 3: origin equals destination             -> route flagged
      index 4: missing source city name              -> imputed + route flagged
      index 5: unparseable departure timestamp       -> row dropped
      index 6: non-numeric base fare                 -> row dropped
    """
    return pd.DataFrame({
        "airline": ["Biman Bangladesh", "US-Bangla", "Novoair", "Air Astra",
                    "Biman Bangladesh", "Novoair", "US-Bangla"],
        "source": ["DAC", "CGP", "DACC", "DAC", "ZYL", "DAC", "CGP"],
        "source_name": [
            "Hazrat Shahjalal International Airport",
            "Shah Amanat International Airport",
            "Hazrat Shahjalal International Airport",
            "Hazrat Shahjalal International Airport",
            "",  # missing city name
            "Hazrat Shahjalal International Airport",
            "Shah Amanat International Airport",
        ],
        "destination": ["CXB", "DAC", "CXB", "DAC", "DAC", "CGP", "ZYL"],
        "destination_name": [
            "Cox's Bazar Airport",
            "Hazrat Shahjalal International Airport",
            "Cox's Bazar Airport",
            "Hazrat Shahjalal International Airport",
            "Hazrat Shahjalal International Airport",
            "Shah Amanat International Airport",
            "Osmani International Airport",
        ],
        "departure_datetime": [
            "2026-08-01 08:30:00", "2026-08-01 12:00:00", "2026-08-02 14:15:00",
            "2026-08-02 18:00:00", "2026-08-03 20:00:00",
            "not-a-date",  # unparseable
            "2026-08-04 10:00:00",
        ],
        "arrival_datetime": [
            "2026-08-01 09:30:00", "2026-08-01 12:50:00", "2026-08-02 15:05:00",
            "2026-08-02 18:55:00", "2026-08-03 21:05:00",
            "2026-08-03 11:00:00", "2026-08-04 11:00:00",
        ],
        "duration_hrs": ["1.00", "0.83", "0.83", "0.91", "1.08", "1.00", "1.00"],
        "stopovers": ["Direct", "Direct", "Direct", "Direct", "Direct", "Direct", "Direct"],
        "aircraft_type": ["Boeing 737", "ATR 72", "ATR 72", "ATR 72", "Boeing 777", "ATR 72", "ATR 72"],
        "class": ["Economy", "Economy", "Economy", "Economy", "Business", "Economy", "Economy"],
        "booking_source": ["Online Website", "Travel Agency", "Online Website",
                           "Direct Booking", "Online Website", "Travel Agency", "Online Website"],
        "base_fare": ["4000.00", "1000.00", "2000.00", "3000.00", "2500.00", "1500.00", "abc"],
        "tax_and_surcharge": ["500.00", "100.00", "200.00", "300.00", "250.00", "150.00", "100.00"],
        # index 1's total is deliberately wrong: 1000 + 100 != 5000
        "total_fare": ["4500.00", "5000.00", "2200.00", "3300.00", "2750.00", "1650.00", "100.00"],
        "seasonality": ["Regular", "Regular", "Regular", "Regular", "Regular", "Regular", "Regular"],
        "days_before_departure": ["7", "14", "1", "30", "3", "5", "9"],
    })


@pytest.fixture
def clean_df(sample_raw_df):
    """The cleaned fact-shaped DataFrame derived from sample_raw_df."""
    cleaned, _report = validate_and_clean_flight_data(sample_raw_df)
    return cleaned


@pytest.fixture
def validation_report(sample_raw_df):
    """The validation report produced alongside the cleaned sample data."""
    _cleaned, report = validate_and_clean_flight_data(sample_raw_df)
    return report
