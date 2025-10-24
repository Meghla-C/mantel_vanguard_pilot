import pytest
import sys
import os
from pathlib import Path

# Add src directory to Python path
src_path = str(Path(__file__).parent.parent / "src")
if src_path not in sys.path:
    sys.path.append(src_path)

from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession
from unittest.mock import Mock, MagicMock

# Mock the dlt module since it's only available in Databricks runtime
dlt_mock = MagicMock()
dlt_mock.expect = MagicMock()
dlt_mock.expect_or_drop = MagicMock()
dlt_mock.expect_or_fail = MagicMock()
dlt_mock.create_streaming_table = MagicMock()
dlt_mock.append_flow = MagicMock()
dlt_mock.create_auto_cdc_flow = MagicMock()
dlt_mock.create_auto_cdc_from_snapshot_flow = MagicMock()
dlt_mock.create_sink = MagicMock()

sys.modules['dlt'] = dlt_mock


@pytest.fixture(scope="function")
def spark_mock():
    """
    Mocking SparkSession for unit tests.
    """
    spark_mock = MagicMock()
    return spark_mock


@pytest.fixture(scope="function")
def spark_session():
    """
    Fixture to create a Spark session for testing.
    """
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def dbutils_mock():
    """
    Mocking dbutils for unit tests.
    """
    dbutils_mock = Mock()
    return dbutils_mock


@pytest.fixture(scope="function")
def databricks_spark() -> SparkSession:
    """
    Fixture to create a Databricks Spark session for testing.
    Includes error handling for session initialization.
    """
    try:
        if os.getenv("DATABRICKS_CLUSTER_ID"):
            return DatabricksSession.builder.getOrCreate()
        else:
            return DatabricksSession.builder.serverless().getOrCreate()
    except Exception as e:
        pytest.fail(f"Failed to create Databricks Spark session: {e}")
