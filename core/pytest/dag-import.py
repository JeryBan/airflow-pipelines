import pytest
from airflow.models import DagBag

@pytest.fixture
def dagbag():
    """Load all DAGs."""
    return DagBag(dag_folder='path/to/dags')

def test_dag_import(dagbag):
    """Test that all DAGs are imported correctly."""
    assert len(dagbag.import_errors) == 0, "DAG import errors: {}".format(dagbag.import_errors)
