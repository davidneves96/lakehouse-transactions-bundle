import sys
from pathlib import Path
import pytest
from pyspark.sql import SparkSession

# Import the root path of the project to the sys.path to allow for relative imports
root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))


@pytest.fixture(scope="session")
def spark():
    """Creates a single SparkSession for the entire test suite."""
    session = (
        SparkSession.builder
        .appName("PytestLakehouseSuite")
        .master("local[1]") # Use a single core for testing to speed up the tests and avoid resource contention
        .config("spark.sql.shuffle.partitions", "1") # Set the number of shuffle partitions to 1 for testing to reduce overhead and speed up the tests
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()