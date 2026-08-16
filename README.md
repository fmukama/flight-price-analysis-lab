```
flight-price-pipeline/
├── .github/
│   └── workflows/
│       ├── ci.yml                 # Runs linter, DAG integrity checks, and pytest in CI
│       └── cd.yml                 # Automated container builds / deployment checks
├── dags/
│   ├── flight_price_etl_dag.py    # Main Airflow DAG orchestrating MySQL -> Python -> Postgres
│   └── scripts/
│       ├── __init__.py
│       ├── ingest_to_mysql.py     # Ingests raw CSV into MySQL staging table
│       ├── validate_clean.py      # Cleans, imputes nulls, and validates data types
│       ├── compute_kpis.py        # Computes 4 required KPI metrics
│       └── load_to_postgres.py    # Idempotent write to PostgreSQL analytics tables
├── data/
│   ├── raw/
│   │   └── Flight_Price_Dataset_of_Bangladesh.csv
│   └── processed/
│       └── .gitkeep
├── docker/
│   ├── airflow/
│   │   ├── Dockerfile             # Extended Airflow image with Python dependencies
│   │   └── requirements.txt       # pandas, numpy, sqlalchemy, pymysql, psycopg2-binary
│   └── jupyter/
│       ├── Dockerfile             # JupyterLab container with database connectors
│       └── requirements.txt       # seaborn, matplotlib, jupyterlab, pandas, sqlalchemy
├── notebooks/
│   ├── 01_eda_and_missing_values.ipynb  # EDA, null exploration, and seasonality logic
│   └── .ipynb_checkpoints/
├── sql/
│   ├── mysql/
│   │   └── init_staging.sql       # MySQL schema and staging table DDL
│   └── postgres/
│       └── init_analytics.sql     # PostgreSQL analytics tables and KPI summary schemas
├── tests/
│   ├── __init__.py
│   ├── conftest.py                # Pytest fixtures (sample mock flight DataFrames)
│   ├── test_dag_integrity.py      # Asserts DAG has no import errors, cycles, or missing tasks
│   ├── test_data_validation.py    # Tests null imputation, type casting, and negative fare checks
│   └── test_kpi_calculations.py   # Tests math accuracy for average fares, routes, and seasonality
├── .dockerignore
├── .env.example                   # Environment variable template for DB credentials and ports
├── .gitignore
├── docker-compose.yml             # Orchestrates Airflow (Webserver, Scheduler), Postgres, MySQL, Jupyter
├── Makefile                       # Developer CLI automation (make up, make down, make test, make eda)
└── README.md                      # Architecture documentation, KPI logic, and setup guide
```