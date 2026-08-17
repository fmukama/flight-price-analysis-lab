import importlib
from pathlib import Path

import pytest
from airflow.models import DagBag

DAG_ID = "flight_price_analysis_pipeline"

# Resolved from this file, not the CWD, so the suite runs from anywhere.
DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"

EXPECTED_TASKS = {
    "ingest_csv_to_mysql_staging",
    "extract_validate_and_clean",
    "compute_business_kpis",
    "load_analytics_to_postgres",
}

# Task bodies import these lazily, so a misplaced module would not register as a
# DAG import error -- it would only fail once that task ran.
PIPELINE_MODULES = [
    "dags.scripts.ingest_to_mysql",
    "dags.scripts.validate_clean",
    "dags.scripts.compute_kpis",
    "dags.scripts.load_to_postgres",
]


@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


@pytest.fixture(scope="session")
def dag(dag_bag):
    # Not DagBag.get_dag(): that queries the metadata DB for serialized-DAG
    # staleness, which would make these structural assertions need a live DB.
    assert DAG_ID in dag_bag.dags, f"{DAG_ID} not found; parsed: {sorted(dag_bag.dags)}"
    return dag_bag.dags[DAG_ID]


def test_airflow_and_sqlalchemy_versions_are_compatible():
    """Airflow 2.9.x requires SQLAlchemy 1.4.x; a >=2.0 pin silently drags in Airflow 3."""
    import airflow
    import sqlalchemy

    assert airflow.__version__ == "2.9.2", f"airflow {airflow.__version__} != 2.9.2"
    assert sqlalchemy.__version__.startswith("1.4"), (
        f"sqlalchemy {sqlalchemy.__version__} must be 1.4.x for Airflow 2.9.x"
    )


@pytest.mark.parametrize("module_path", PIPELINE_MODULES)
def test_pipeline_modules_are_importable(module_path):
    assert importlib.import_module(module_path) is not None


def test_dag_import_errors(dag_bag):
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"


def test_flight_price_dag_structure(dag):
    assert set(dag.task_dict.keys()) == EXPECTED_TASKS
    assert dag.catchup is False


def test_dag_is_a_linear_chain(dag):
    """A missing edge would let the load run against stale parquet files."""
    expected_downstream = {
        "ingest_csv_to_mysql_staging": {"extract_validate_and_clean"},
        "extract_validate_and_clean": {"compute_business_kpis"},
        "compute_business_kpis": {"load_analytics_to_postgres"},
        "load_analytics_to_postgres": set(),
    }
    for task_id, downstream in expected_downstream.items():
        assert dag.get_task(task_id).downstream_task_ids == downstream


def test_dag_does_not_auto_fire_on_deployment(dag):
    """With a past start_date, an unpaused DAG runs the moment the scheduler sees it."""
    assert dag.is_paused_upon_creation is True
    assert dag.max_active_runs == 1
