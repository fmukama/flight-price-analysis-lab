import pytest
import pandas as pd


@pytest.fixture
def sample_raw_df():
    """Returns a realistic raw staging DataFrame with edge cases and dirty values."""
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
        "booking_source": ["Online Website", "Travel Agent", "Online Website", "Direct", "Online Website"],
        "base_fare": ["4500.00", "3800.00", "-500.00", "4200.00", "9500.00"],  # Row index 2 has negative fare
        "tax_and_surcharge": ["725.00", "650.00", "100.00", "700.00", "1500.00"],
        "total_fare": ["5225.00", "4450.00", "-400.00", "4900.00", "11000.00"],
        "seasonality": ["Peak", "Peak", "Regular", "Regular", "Peak"],
        "days_before_departure": ["7", "14", "1", "30", "3"]
    })