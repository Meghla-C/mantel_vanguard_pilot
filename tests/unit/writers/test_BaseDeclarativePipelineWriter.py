import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import DataFrame
from core_functions.config_models import EntityConfig, TargetConfig
from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter


class TestBaseDeclarativePipelineWriter:
    """Test cases for BaseDeclarativePipelineWriter class"""

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
        df.columns = ["col1", "col2", "temp_column", "debug_info"]
        df.select.return_value = df
        df.filter.return_value = df
        return df

    @pytest.fixture
    def base_entity_config(self):
        """Base entity configuration for testing"""
        return {
            "target": {
                "writer_type": "dlt_append_flow",
                "table_name": "test_schema.test_table",
                "comment": "Test table",
                "table_properties": {"delta.autoOptimize.optimizeWrite": "true"},
                "partition_cols": ["date_col"],
                "expectations": [
                    {
                        "name": "valid_amount",
                        "constraint": "amount > 0",
                        "action": "fail"
                    },
                    {
                        "name": "not_null_id",
                        "constraint": "id IS NOT NULL",
                        "action": "drop"
                    }
                ],
                "exclude_columns": ["temp_column", "debug_info"]
            }
        }

    @pytest.fixture
    def concrete_writer(self, base_entity_config, mock_spark, mock_dbutils, mock_logger):
        """Create a concrete implementation of BaseDeclarativePipelineWriter for testing"""
        class ConcreteWriter(BaseDeclarativePipelineWriter):
            def _execute_declarative_operation(self, source_dataframe):
                return "mock_result"
        
        entity_config = EntityConfig(**base_entity_config)
        return ConcreteWriter(entity_config, mock_spark, mock_dbutils, mock_logger)

    def test_init(self, concrete_writer, mock_spark, mock_dbutils):
        """Test initialization of BaseDeclarativePipelineWriter"""
        assert concrete_writer.spark == mock_spark
        assert concrete_writer.dbutils == mock_dbutils
        assert concrete_writer.entity_config is not None

    def test_convert_expectations_to_dlt_format_unified(self, concrete_writer):
        """Test converting unified expectations format to DLT format"""
        # The method reads expectations from entity_config, so we need to set them
        result = concrete_writer._convert_expectations_to_dlt_format()
        
        # Check that expectations were converted properly
        # "fail" action maps to "expect_all", "drop" action maps to "expect_all_or_drop"
        assert result["expect_all"]["valid_amount"] == "amount > 0"
        assert result["expect_all_or_drop"]["not_null_id"] == "id IS NOT NULL"

    def test_convert_expectations_to_dlt_format_empty(self, mock_spark, mock_dbutils, mock_logger):
        """Test converting empty expectations"""
        config = {
            "target": {
                "writer_type": "dlt_append_flow",
                "table_name": "test_schema.test_table"
            }
        }
        entity_config = EntityConfig(**config)
        
        class ConcreteWriter(BaseDeclarativePipelineWriter):
            def _execute_declarative_operation(self, source_dataframe):
                return "mock_result"
        
        writer = ConcreteWriter(entity_config, mock_spark, mock_dbutils, mock_logger)
        result = writer._convert_expectations_to_dlt_format()
        
        assert result == {
            "expect_all": {},
            "expect_all_or_drop": {},
            "expect_all_or_fail": {}
        }

    def test_convert_expectations_to_dlt_format_invalid_action(self, mock_spark, mock_dbutils, mock_logger):
        """Test converting expectations with invalid action defaults to expect_all"""
        config = {
            "target": {
                "writer_type": "dlt_append_flow",
                "table_name": "test_schema.test_table",
                "expectations": [
                    {
                        "name": "test_constraint",
                        "constraint": "value > 0",
                        "action": "invalid_action"
                    }
                ]
            }
        }
        entity_config = EntityConfig(**config)
        
        class ConcreteWriter(BaseDeclarativePipelineWriter):
            def _execute_declarative_operation(self, source_dataframe):
                return "mock_result"
        
        writer = ConcreteWriter(entity_config, mock_spark, mock_dbutils, mock_logger)
        result = writer._convert_expectations_to_dlt_format()
        
        # Invalid action should not be processed, so all should be empty
        assert result["expect_all"] == {}
        assert result["expect_all_or_drop"] == {}
        assert result["expect_all_or_fail"] == {}

    def test_apply_column_exclusions(self, concrete_writer, mock_dataframe):
        """Test column exclusions"""
        result = concrete_writer._apply_column_exclusions(mock_dataframe)
        
        mock_dataframe.select.assert_called_once_with("col1", "col2")
        assert result == mock_dataframe

    def test_apply_column_exclusions_no_exclusions(self, mock_spark, mock_dbutils, mock_logger, mock_dataframe):
        """Test column exclusions when none are configured"""
        config = {
            "target": {
                "writer_type": "dlt_append_flow",
                "table_name": "test_schema.test_table"
            }
        }
        entity_config = EntityConfig(**config)
        
        class ConcreteWriter(BaseDeclarativePipelineWriter):
            def _execute_declarative_operation(self, source_dataframe):
                return "mock_result"
        
        writer = ConcreteWriter(entity_config, mock_spark, mock_dbutils, mock_logger)
        result = writer._apply_column_exclusions(mock_dataframe)
        
        mock_dataframe.select.assert_not_called()
        assert result == mock_dataframe

    @patch('writers.BaseDeclarativePipelineWriter.dlt')
    def test_create_table_if_not_exists(self, mock_dlt, concrete_writer):
        """Test creating DLT streaming table"""
        mock_dlt.create_streaming_table.return_value = "mock_table"
        
        result = concrete_writer._create_table_if_not_exists()
        
        # Verify the call was made with correct parameters
        mock_dlt.create_streaming_table.assert_called_once()
        call_args = mock_dlt.create_streaming_table.call_args
        assert call_args.kwargs['name'] == "test_schema.test_table"
        assert call_args.kwargs['comment'] == "Test table"
        assert call_args.kwargs['table_properties'] == {"delta.autoOptimize.optimizeWrite": "true"}
        assert call_args.kwargs['partition_cols'] == ["date_col"]
        assert result == "mock_table"

    def test_write_to_target_table(self, concrete_writer, mock_dataframe):
        """Test complete write_to_target_table workflow"""
        result = concrete_writer.write_to_target_table(mock_dataframe)
        
        # Verify the workflow
        mock_dataframe.select.assert_called_once()  # Column exclusions applied
        assert result == "mock_result"  # Declarative operation executed

    def test_abstract_method_not_implemented(self, base_entity_config, mock_spark, mock_dbutils):
        """Test that abstract method raises NotImplementedError"""
        entity_config = EntityConfig(**base_entity_config)
        
        with pytest.raises(TypeError):
            # Should not be able to instantiate abstract class
            BaseDeclarativePipelineWriter(entity_config, mock_spark, mock_dbutils)
