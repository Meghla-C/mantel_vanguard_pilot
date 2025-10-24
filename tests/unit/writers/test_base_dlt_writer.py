import pytest
from unittest.mock import Mock
from pyspark.sql import DataFrame


class BaseDLTWriterTest:
    """Base test class for DLT writers with common fixtures and mocks"""

    @pytest.fixture
    def mock_spark(self):
        """Mock SparkSession"""
        return Mock()

    @pytest.fixture
    def mock_dbutils(self):
        """Mock dbutils"""
        return Mock()

    @pytest.fixture
    def mock_logger(self):
        """Mock logger"""
        return Mock()

    @pytest.fixture
    def mock_dataframe(self):
        """Mock DataFrame"""
        df = Mock(spec=DataFrame)
        df.columns = ["col1", "col2", "amount"]
        df.select.return_value = df
        df.filter.return_value = df
        return df

    def test_inheritance(self, writer):
        """Test that DLT writer properly inherits from BaseDeclarativePipelineWriter"""
        from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter
        assert isinstance(writer, BaseDeclarativePipelineWriter)

    def test_implements_interface(self, writer):
        """Test that DLT writer implements IDataWriter interface"""
        from writers.IDataWriter import IDataWriter
        assert isinstance(writer, IDataWriter)
        
        # Check required methods exist
        assert hasattr(writer, '__init__')
        assert hasattr(writer, 'write_to_target_table')
        assert callable(getattr(writer, 'write_to_target_table'))

    def test_init_basic_properties(self, writer, mock_spark, mock_dbutils, mock_logger):
        """Test basic initialization properties"""
        assert writer.spark == mock_spark
        assert writer.dbutils == mock_dbutils
        assert writer.logger == mock_logger
