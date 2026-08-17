-- SCRIPT: init_analytics.sql (Updated for Exact CSV Schema & KPIs)
\c analytics_db;

CREATE SCHEMA IF NOT EXISTS flight_analytics;

DROP TABLE IF EXISTS flight_analytics.kpi_popular_routes;
DROP TABLE IF EXISTS flight_analytics.kpi_airline_booking_count;
DROP TABLE IF EXISTS flight_analytics.kpi_seasonal_fare_variation;
DROP TABLE IF EXISTS flight_analytics.kpi_avg_fare_by_airline;
DROP TABLE IF EXISTS flight_analytics.fct_flight_prices_cleaned;

-- 1. Main Cleaned Fact Table
CREATE TABLE flight_analytics.fct_flight_prices_cleaned (
    flight_id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    source_code VARCHAR(10) NOT NULL,
    source_name VARCHAR(150) NOT NULL,
    destination_code VARCHAR(10) NOT NULL,
    destination_name VARCHAR(150) NOT NULL,
    route_code VARCHAR(30) NOT NULL,
    route_name VARCHAR(320) NOT NULL,
    departure_datetime TIMESTAMP NOT NULL,
    arrival_datetime TIMESTAMP NOT NULL,
    flight_date DATE NOT NULL,
    duration_hrs NUMERIC(6, 2) NOT NULL,
    stopovers VARCHAR(50) NOT NULL,
    aircraft_type VARCHAR(100),
    flight_class VARCHAR(50) NOT NULL,
    booking_source VARCHAR(100),
    base_fare_bdt NUMERIC(12, 2) NOT NULL,
    tax_and_surcharge_bdt NUMERIC(12, 2) NOT NULL,
    total_fare_bdt NUMERIC(12, 2) NOT NULL,
    -- Total Fare as it appeared in the source CSV; nullable because the source
    -- value may itself be missing. ~4.4% of rows disagree with base + tax.
    total_fare_original_bdt NUMERIC(12, 2),
    -- TRUE where total_fare_original_bdt != base + tax and was recomputed
    fare_mismatch_flag BOOLEAN NOT NULL DEFAULT FALSE,
    -- TRUE on malformed airport code, origin = destination, or missing city
    invalid_route_flag BOOLEAN NOT NULL DEFAULT FALSE,
    seasonality VARCHAR(50) NOT NULL,
    days_before_departure INT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. KPI 1: Average Fare by Airline
CREATE TABLE flight_analytics.kpi_avg_fare_by_airline (
    airline VARCHAR(100) PRIMARY KEY,
    avg_base_fare NUMERIC(12, 2) NOT NULL,
    avg_tax_surcharge NUMERIC(12, 2) NOT NULL,
    avg_total_fare NUMERIC(12, 2) NOT NULL,
    min_total_fare NUMERIC(12, 2) NOT NULL,
    max_total_fare NUMERIC(12, 2) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. KPI 2: Seasonal Fare Variation (peak vs non-peak)
CREATE TABLE flight_analytics.kpi_seasonal_fare_variation (
    seasonality VARCHAR(50) PRIMARY KEY,
    -- Peak demand period (Eid, Hajj, Winter Holidays) vs the Regular baseline
    is_peak BOOLEAN NOT NULL,
    flight_count INT NOT NULL,
    avg_total_fare NUMERIC(12, 2) NOT NULL,
    median_total_fare NUMERIC(12, 2) NOT NULL,
    min_total_fare NUMERIC(12, 2) NOT NULL,
    max_total_fare NUMERIC(12, 2) NOT NULL,
    -- Percentage difference between this season's average total fare and the
    -- mean fare across all non-peak bookings. Signed: negative means cheaper.
    avg_fare_uplift_pct NUMERIC(8, 2) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. KPI 3: Booking Count by Airline
CREATE TABLE flight_analytics.kpi_airline_booking_count (
    airline VARCHAR(100) PRIMARY KEY,
    booking_count INT NOT NULL,
    market_share_pct NUMERIC(5, 2) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. KPI 4: Most Popular Routes
-- Keyed on the route, not the rank: ranks shift between runs as volumes change,
-- so keying on rank would change a route's primary key on every run.
CREATE TABLE flight_analytics.kpi_popular_routes (
    route_code VARCHAR(30) PRIMARY KEY,
    route_rank INT NOT NULL UNIQUE,
    source_name VARCHAR(150) NOT NULL,
    destination_name VARCHAR(150) NOT NULL,
    booking_count INT NOT NULL,
    avg_total_fare NUMERIC(12, 2) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cleaned_date ON flight_analytics.fct_flight_prices_cleaned(flight_date);
CREATE INDEX idx_cleaned_airline ON flight_analytics.fct_flight_prices_cleaned(airline);
CREATE INDEX idx_cleaned_route ON flight_analytics.fct_flight_prices_cleaned(route_code);
-- Supports filtering the fact table down to rows whose fares were corrected
CREATE INDEX idx_cleaned_fare_mismatch ON flight_analytics.fct_flight_prices_cleaned(fare_mismatch_flag);
