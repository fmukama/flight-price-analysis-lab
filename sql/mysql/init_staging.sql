CREATE DATABASE IF NOT EXISTS staging_db;
USE staging_db;

DROP TABLE IF EXISTS raw_flight_prices; -- Drop table if re-initializing to avoid duplicate schemas

CREATE TABLE raw_flight_prices ( -- Note: All columns are defined as nullable to accept unprocessed raw inputs
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100) NULL,
    source VARCHAR(50) NULL,
    source_name VARCHAR(150) NULL,
    destination VARCHAR(50) NULL,
    destination_name VARCHAR(150) NULL,
    departure_datetime VARCHAR(50) NULL,
    arrival_datetime VARCHAR(50) NULL,
    duration_hrs VARCHAR(50) NULL,
    stopovers VARCHAR(50) NULL,
    aircraft_type VARCHAR(100) NULL,
    class VARCHAR(50) NULL,
    booking_source VARCHAR(100) NULL,
    base_fare VARCHAR(50) NULL,
    tax_and_surcharge VARCHAR(50) NULL,
    total_fare VARCHAR(50) NULL,
    seasonality VARCHAR(50) NULL,
    days_before_departure VARCHAR(50) NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_raw_airline ON raw_flight_prices(airline);
CREATE INDEX idx_raw_route ON raw_flight_prices(source, destination);