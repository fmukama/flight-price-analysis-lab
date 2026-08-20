# Flight Price Analysis Pipeline (Bangladesh)

Airflow pipeline over the Kaggle *Flight Price Dataset of Bangladesh* (57,000 bookings):

`CSV → MySQL staging → validate & clean → KPIs → PostgreSQL`

**Stack:** Airflow 2.9.2 (LocalExecutor) · MySQL 8.0 (staging) · PostgreSQL 15 (analytics + Airflow metadata) · Python 3.11 · JupyterLab · Adminer

Staging is all `VARCHAR` so ingestion cannot fail on type coercion — typing happens in task 2.
Tasks hand off parquet files on disk; only small metadata dicts go through XCom.

---

## Quick start

```bash
make setup && make build && make up
make run        # execute the DAG now and stream the output
make verify     # row counts across staging and all 5 analytics tables
```

| | |
|---|---|
| Airflow UI | <http://localhost:8080> — `admin` / `admin` |
| Adminer (SQL browser) | <http://localhost:8081> — `make adminer` prints the connection details |
| JupyterLab | `make eda` prints the tokenised URL |

`make help` lists every target. A full run takes ~59 s.

The DAG ships paused — with a past `start_date` it would otherwise fire the moment the scheduler
saw it. Use `make unpause` + `make trigger` to run it through the scheduler instead of inline.
`make clean` drops the data volumes, so the databases come back schema-only: re-run the DAG
before browsing them.

---

## DAG tasks

`flight_price_analysis_pipeline` — `@daily`, `catchup=False`, `max_active_runs=1`, `retries=2`.
Strictly linear; each task validates its predecessor's output.

| # | Task | Guards |
|---|---|---|
| 1 | `ingest_csv_to_mysql_staging` | Header checked first; advisory lock rejects a concurrent ingest; `DELETE` + inserts in **one transaction**; staged count reconciled against rows read |
| 2 | `extract_validate_and_clean` | Fails if staging's row count moved; returns an audit report of every correction and drop |
| 3 | `compute_business_kpis` | Per-KPI column checks; rejects empty results; deterministic tie-breaking |
| 4 | `load_analytics_to_postgres` | Schema validated before writing; **all 5 tables in one transaction**; per-table counts verified |

---

## KPIs

Loaded into the `flight_analytics` schema. `Total Fare = Base Fare + Tax & Surcharge` is
recomputed for every row.

| KPI | Table | Rows |
|---|---|---|
| Average fare by airline | `kpi_avg_fare_by_airline` | 24 |
| Seasonal fare variation | `kpi_seasonal_fare_variation` | 4 |
| Booking count by airline | `kpi_airline_booking_count` | 24 |
| Most popular routes | `kpi_popular_routes` | 152 |

Peak seasons are Eid, Hajj and Winter Holidays. `avg_fare_uplift_pct` measures each season
against the mean fare of all **non-peak** bookings:

| Season | Peak | Bookings | Avg fare (BDT) | vs non-peak |
|---|---|---|---|---|
| Hajj | ✔ | 942 | 96,189.95 | **+42.85%** |
| Eid | ✔ | 603 | 90,790.87 | **+34.83%** |
| Winter Holidays | ✔ | 10,930 | 79,256.33 | **+17.70%** |
| Regular | — | 44,525 | 67,337.33 | baseline |

Busiest route: `RJH -> SIN` — 417 bookings, 112,747.25 BDT average.

---

## Data quality

57,000 rows, 24 airlines, 152 routes, Jan 2025 – Mar 2026. No nulls, no non-positive fares, no
duplicates — those code paths never fire on this data and are covered by synthetic fixtures.

The one real defect: **2,522 rows (4.42%) where `Total Fare != Base + Tax`**, largest gap
**93,164.55 BDT**. The total is recomputed, but the source value is kept in
`total_fare_original_bdt` with `fare_mismatch_flag = TRUE`, so the correction stays auditable.
KPI averages therefore differ slightly from naive CSV aggregates.

Rows are **flagged and kept** for fare mismatches and invalid routes (malformed IATA code,
origin equal to destination, missing city name). Rows are **dropped** only when unanalysable:
non-positive or unparseable fare, unparseable timestamp. Missing categoricals become
`"Unknown"`, never a real category value.

---

## Testing & configuration

`make test` runs 32 tests on in-memory fixtures and DagBag parsing — no database required, ~1.5 s
— so CI needs no service containers. CI installs the image's `requirements.txt` under the
Airflow 2.9.2 constraints file and never re-pins Airflow.

`.env` is copied from `.env.example`. One trap: `MYSQL_PORT` / `POSTGRES_PORT` are dialled
**inside** the compose network and must match each database's real listening port, while
`MYSQL_HOST_PORT` / `POSTGRES_HOST_PORT` are published **to your machine** and can change freely.

Layout: `dags/flight_price_etl_dag.py` (DAG) · `dags/scripts/` (the four task modules) ·
`sql/` (DDL for both databases) · `tests/` · `notebooks/` (EDA only — the prototype cleaner
there predates `validate_clean.py` and does not flag fare mismatches).
