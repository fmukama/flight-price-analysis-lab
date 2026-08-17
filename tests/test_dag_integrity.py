import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dag_bag():
    """Loads all DAGs from the dags/ directory."""
    return DagBag(dag_folder="dags", include_examples=False)


def test_dag_import_errors(dag_bag):
    """Ensures there are zero DAG syntax or import failures."""
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"


def test_flight_price_dag_structure(dag_bag):
    """Asserts that our specific DAG exists and contains the expected tasks."""
    dag_id = "flight_price_analysis_pipeline"
    assert dag_id in dag_bag.dags
    dag = dag_bag.get_dag(dag_id)
    
    expected_tasks = {
        "ingest_csv_to_mysql_staging",
        "extract_validate_and_clean",
        "compute_business_kpis",
        "load_analytics_to_postgres"
    }
    actual_tasks = set(dag.task_dict.keys())
    assert expected_tasks == actual_tasks
    assert dag.catchup is False