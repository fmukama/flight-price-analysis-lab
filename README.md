# Flight Price Analysis Pipeline (Bangladesh)

Airflow pipeline that ingests the Kaggle *Flight Price Dataset of Bangladesh* (57,000 bookings)
into MySQL staging, validates and cleans it, computes four KPIs, and loads the results into
PostgreSQL.

`CSV → MySQL staging → validate & clean → KPIs → PostgreSQL`

**Stack:** Airflow 2.9.2 (LocalExecutor) · MySQL 8.0 (staging) · PostgreSQL 15 (analytics + Airflow metadata) · Python 3.11 · JupyterLab

Staging is all `VARCHAR` so ingestion cannot fail on type coercion — typing happens in task 2.
Tasks hand off parquet files on disk, not XCom; only small dicts go through XCom.

---

## Quick start

```bash
cp .env.example .env && make build && make up
```

- **Airflow UI:** <http://localhost:8080> — `admin` / `admin`
- **JupyterLab:** `make eda` prints the tokenised URL
- **To run:** unpause `flight_price_analysis_pipeline` in the UI, then Trigger

The DAG ships paused — with a past `start_date` it would otherwise fire the moment the
scheduler saw it (challenge 4).

| Command | Purpose |
|---|---|
| `make ps` | State, health, and **restart counts** — a crash-looping container still shows "Up" |
| `make test` | 32 tests inside the scheduler |
| `make logs` | Tail all services |
| `make clean` | Reset everything, **including data volumes** |

Full pipeline run: **~33 s**.

---

## Layout

```
dags/flight_price_etl_dag.py       # DAG definition
dags/scripts/ingest_to_mysql.py    # CSV -> MySQL staging, locked + reconciled
dags/scripts/validate_clean.py     # Type coercion, null handling, quality flags
dags/scripts/compute_kpis.py       # The 4 KPI aggregations
dags/scripts/load_to_postgres.py   # Atomic write of all 5 analytics tables
sql/mysql/init_staging.sql         # Staging DDL
sql/postgres/init_analytics.sql    # Analytics schema + KPI tables
tests/                             # 32 tests: DAG integrity, validation, KPI math
```

> `notebooks/01_eda_and_missing_values.ipynb` holds the original *prototype* cleaner. It
> predates `validate_clean.py` and does not flag fare mismatches — treat it as EDA only.

---

## DAG tasks

`flight_price_analysis_pipeline` — `@daily`, `catchup=False`, `max_active_runs=1`, `retries=2`.
Strictly linear; each task validates its predecessor's output.

| # | Task | Guards |
|---|---|---|
| 1 | `ingest_csv_to_mysql_staging` | Header validated first; advisory lock rejects a concurrent ingest; `DELETE` + inserts in **one transaction**; staged count reconciled against rows read; per-chunk width check |
| 2 | `extract_validate_and_clean` | Fails if staging's row count moved; returns an audit report of every correction and drop |
| 3 | `compute_business_kpis` | Per-KPI column checks; rejects empty results; deterministic tie-breaking |
| 4 | `load_analytics_to_postgres` | Column/table validation before writing; **all 5 tables in one transaction**; per-table counts verified |

---

## KPI definitions

Loaded into `flight_analytics`. `Total Fare = Base Fare + Tax & Surcharge` is recomputed for
every row (challenge 5).

| KPI | Table | Computation |
|---|---|---|
| Average Fare by Airline | `kpi_avg_fare_by_airline` | `groupby(airline)` → mean base / tax / total, plus min and max total. 24 rows |
| Seasonal Fare Variation | `kpi_seasonal_fare_variation` | `groupby(seasonality)` → count, mean, median, min, max; `is_peak` (Eid, Hajj, Winter Holidays) and `avg_fare_uplift_pct` = % difference from the mean fare of all **non-peak** bookings |
| Booking Count by Airline | `kpi_airline_booking_count` | Count per airline + `market_share_pct` of 57,000 |
| Most Popular Routes | `kpi_popular_routes` | `groupby(route_code)` → bookings + mean fare, ranked by volume then route code; invalid routes excluded. 152 rows |

| Season | Peak | Bookings | Avg Fare (BDT) | vs non-peak |
|---|---|---|---|---|
| Hajj | ✔ | 942 | 96,189.95 | **+42.85%** |
| Eid | ✔ | 603 | 90,790.87 | **+34.83%** |
| Winter Holidays | ✔ | 10,930 | 79,256.33 | **+17.70%** |
| Regular | — | 44,525 | 67,337.33 | baseline |

Busiest route: `RJH -> SIN`, 417 bookings, 112,747.25 BDT average.

---

## Data quality

Source profile: 57,000 rows, 24 airlines, 8 origins, 20 destinations, 152 routes,
Jan 2025 – Mar 2026. **No nulls, no non-positive fares, no duplicates** — so the null-imputation
and negative-fare paths never fire on this data and are covered by synthetic fixtures instead.

The one real defect: **2,522 rows (4.42%) where `Total Fare != Base Fare + Tax & Surcharge`**,
largest gap **93,164.55 BDT**. The total is recomputed but the source value is kept in
`total_fare_original_bdt` with `fare_mismatch_flag = TRUE`, so the correction stays auditable:

```sql
SELECT COUNT(*), MAX(ABS(total_fare_original_bdt - total_fare_bdt))
FROM flight_analytics.fct_flight_prices_cleaned WHERE fare_mismatch_flag;
```

KPI averages therefore differ slightly from naive CSV aggregates (Hajj 96,189.95 vs 97,144.47):
the pipeline trusts `base + tax`, not the inconsistent source total.

Rows are **flagged and kept** for fare mismatches and invalid routes (malformed IATA code,
origin equal to destination, missing city name). Rows are **dropped** only when unanalysable:
non-positive fare, unparseable fare, or unparseable timestamp. Missing categoricals become
`"Unknown"`, never a real category value.

---

## Testing

```bash
make test
```

All 32 tests run on in-memory fixtures and DagBag parsing — **no database required** — so the
suite finishes in ~1.5 s and CI needs no service containers. Database-level guarantees
(advisory lock, atomic load, row reconciliation) are verified against the live stack with
`airflow dags test`.

CI installs the image's `requirements.txt` under the same Airflow constraints file and never
re-pins Airflow; the version assertion lives in `tests/test_dag_integrity.py` so it runs locally too.

---

## Configuration

`.env`, copied from `.env.example`. One trap:

| Variable | Meaning |
|---|---|
| `MYSQL_PORT` / `POSTGRES_PORT` | Port dialled **inside** the compose network; must match the DB's real listening port |
| `MYSQL_HOST_PORT` / `POSTGRES_HOST_PORT` | Port published **to your machine**; change freely to dodge a local clash |
