from abc import ABC, abstractmethod
from writers.IDataWriter import IDataWriter
from pyspark.sql import DataFrame
from typing import Dict, Any

# Framework imports
from readers.DataReaderFactory import DataReaderFactory
from transformers.DataTransformFactory import DataTransformFactory

# Conditional DLT import - only import if available
try:
    import dlt
    DLT_AVAILABLE = True
except:
    DLT_AVAILABLE = False


class BaseDeclarativePipelineWriter(IDataWriter, ABC):
    """
    Base class for Databricks Declarative Pipeline writers that provides common functionality
    for all declarative pipeline writer implementations.
    """

    def __init__(self, entity_config: Dict, spark, dbutils, logger) -> None:
        """
        Constructor for the BaseDeclarativePipelineWriter class

        Inputs:
            - entity_config: Pydantic model, A dictionary of config for the entity to be written
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
            - logger: Logger object for logging operations (required)
        Outputs:
            - None
        """
        self.spark = spark
        self.dbutils = dbutils
        self.entity_config = entity_config
        self.entity_name = entity_config.entity_name
        self.logger = logger
        
        self.logger.info(f"Initialized {self.__class__.__name__} for entity: {entity_config.entity_name}")

    def _convert_expectations_to_dlt_format(self) -> Dict[str, Dict[str, str]]:
        """
        Convert simple expectations format to DLT-native format for table creation.
        
        Inputs:
            - None
        Outputs:
            - Dict, Dictionary containing expect_all, expect_all_or_drop, and expect_all_or_fail
        """
        expectations = getattr(self.entity_config.target, 'expectations', None)
        dlt_expectations = {
            'expect_all': {},
            'expect_all_or_drop': {},
            'expect_all_or_fail': {}
        }
        
        if expectations:
            self.logger.info(f"Converting {len(expectations)} expectations to DLT format")
            
            for expectation in expectations:
                expectation_name = expectation.get("name", "unnamed_expectation")
                constraint = expectation.get("constraint")
                action = expectation.get("action", "fail")
                
                if constraint:
                    if action == "fail":
                        dlt_expectations['expect_all'][expectation_name] = constraint
                    elif action == "drop":
                        dlt_expectations['expect_all_or_drop'][expectation_name] = constraint
                    elif action == "quarantine":
                        dlt_expectations['expect_all_or_fail'][expectation_name] = constraint
                    
                    self.logger.debug(f"Converted expectation '{expectation_name}' with action '{action}'")
            
            self.logger.info("Successfully converted expectations to DLT format")
        else:
            self.logger.debug("No expectations configured")
            
        return dlt_expectations

    def _apply_expectations_as_decorators(self, func):
        """
        Apply data quality expectations as decorators to a function.
        Returns the function with all expectation decorators applied.
        
        Inputs:
            - func: function, The function to decorate with expectations
        Outputs:
            - function, The decorated function with all expectations applied
        """
        # Check if DLT is available
        if not DLT_AVAILABLE:
            self.logger.warning("DLT is not available. Skipping data quality expectations.")
            return func
            
        expectations = getattr(self.entity_config.target, 'expectations', None)
        if not expectations:
            self.logger.debug("No data quality expectations configured")
            return func
            
        self.logger.info(f"Applying {len(expectations)} data quality expectations as decorators")
        
        # Start with the original function
        decorated_func = func
        
        # Apply each expectation as a decorator
        for expectation in expectations:
            expectation_name = expectation.get("name", "unnamed_expectation")
            constraint = expectation.get("constraint")
            action = expectation.get("action", "fail")
            
            self.logger.debug(f"Applying expectation '{expectation_name}' with action '{action}' and constraint '{constraint}'")
            
            if constraint:
                if action == "fail":
                    decorated_func = dlt.expect(expectation_name, constraint)(decorated_func)
                elif action == "drop":
                    decorated_func = dlt.expect_or_drop(expectation_name, constraint)(decorated_func)
                elif action == "quarantine":
                    decorated_func = dlt.expect_or_fail(expectation_name, constraint)(decorated_func)
        
        self.logger.info("Successfully applied all data quality expectations as decorators")
        return decorated_func

    def _apply_column_exclusions(self, df: DataFrame) -> DataFrame:
        """
        Remove excluded columns from the DataFrame if configured.
        
        Inputs:
            - df: DataFrame, The source dataframe
        Outputs:
            - DataFrame, The dataframe with excluded columns removed
        """
        exclude_columns = getattr(self.entity_config.target, 'exclude_columns', None)
        if exclude_columns:
            self.logger.info(f"Applying column exclusions: {exclude_columns}")
            columns_to_keep = [c for c in df.columns if c not in exclude_columns]
            self.logger.debug(f"Columns to keep: {columns_to_keep}")
            return df.select(*columns_to_keep)
        else:
            self.logger.debug("No column exclusions configured")
        return df

    def _create_table_if_not_exists(self) -> Any:
        """
        Create the target table using DLT streaming table if it doesn't exist.
        
        Inputs:
            - None
        Outputs:
            - Any, The DLT streaming table object, or None if DLT is not available
        """
        # Check if DLT is available
        if not DLT_AVAILABLE:
            self.logger.warning("DLT is not available. Cannot create DLT streaming table.")
            return None
            
        table_name = self.entity_config.target.table_name
        self.logger.info(f"Creating DLT streaming table: {table_name}")
        
        comment = getattr(self.entity_config.target, 'comment', None)
        spark_conf = getattr(self.entity_config.target, 'spark_conf', {})
        table_properties = getattr(self.entity_config.target, 'table_properties', {})
        path = getattr(self.entity_config.target, 'path', None)
        partition_cols = getattr(self.entity_config.target, 'partition_cols', None)
        cluster_by_auto = getattr(self.entity_config.target, 'cluster_by_auto', True)
        cluster_by = getattr(self.entity_config.target, 'cluster_by', None)
        schema = getattr(self.entity_config.target, 'schema', None)
        row_filter = getattr(self.entity_config.target, 'row_filter', None)
        
        # Convert expectations from unified format to DLT format
        dlt_expectations = self._convert_expectations_to_dlt_format()
        
        self.logger.debug(f"Table configuration - Comment: {comment}, Path: {path}, Partition columns: {partition_cols}")
        self.logger.debug(f"DLT expectations: {dlt_expectations}")
        
        # Use DLT create_streaming_table to create a streaming table
        result = dlt.create_streaming_table(
            name=table_name,
            comment=comment,
            spark_conf=spark_conf,
            table_properties=table_properties,
            path=path,
            partition_cols=partition_cols,
            cluster_by_auto=cluster_by_auto,
            cluster_by=cluster_by,
            schema=schema,
            expect_all=dlt_expectations['expect_all'],
            expect_all_or_drop=dlt_expectations['expect_all_or_drop'],
            expect_all_or_fail=dlt_expectations['expect_all_or_fail'],
            row_filter=row_filter
        )
        
        self.logger.info(f"Successfully created DLT streaming table: {table_name}")
        return result
    
    def _prepare_data(self) -> DataFrame:
        """
        Prepare data for DLT table registration.
        This method contains the read and transform logic but lets DLT handle the writing.
        """

        # Get the IDataReader object from the factory
        data_reader = DataReaderFactory.get_datareader(
            self.entity_config, self.spark, self.dbutils, self.logger
        )
        
        # Read source data into a DataFrame
        source_df = data_reader.read_source_to_dataframe()
        
        # Get the IDataTransform object from the factory
        transformers = DataTransformFactory.get_datatransform(
            source_df, self.entity_config, self.spark, self.dbutils, self.logger
        )
        
        # Apply transformations to get transformed DataFrame(s)
        transform_dfs = transformers.apply_transform()
        
        # Get the transformed DataFrame for this entity
        transformed_df = transform_dfs[self.entity_name]
        
        return transformed_df

    def _get_prepare_data_with_expectations_function(self):
        """
        Returns the _prepare_data function decorated with expectations.
        This function is designed to be passed to DLT decorators like dlt.table() and dlt.append_flow().
        
        Inputs:
            - None
        Outputs:
            - function, The _prepare_data function decorated with expectations
        """
        # Apply expectations as decorators to the _prepare_data method
        return self._apply_expectations_as_decorators(self._prepare_data)

    @abstractmethod
    def _execute_declarative_operation(self, source_dataframe: DataFrame) -> Any:
        """
        Abstract method to execute the specific declarative pipeline operation.
        Must be implemented by concrete declarative pipeline writer classes.
        
        Inputs:
            - source_dataframe: DataFrame, The source dataframe
        Outputs:
            - Any, The result of the declarative operation
        """
        raise NotImplementedError

    def write_to_target_table(
        self,
        source_dataframe: DataFrame,
        entity_config=None,
        spark=None,
        dbutils=None,
    ) -> None:
        """
        Writes the source dataframe using declarative pipeline functionality.
        This method orchestrates the common declarative pipeline writing process.

        Inputs:
            - source_dataframe: DataFrame, The dataframe to be written
            - entity_config: Pydantic model, A dictionary of config for the entity to be written
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
        Outputs:
            - None
        """
        self.logger.info("Starting declarative pipeline write process")
        
        if not entity_config:
            entity_config = self.entity_config
        if not spark:
            spark = self.spark
        if not dbutils:
            dbutils = self.dbutils

        target_table = self.entity_config.target.table_name
        self.logger.info(f"Writing to target table: {target_table}")

        # Apply column exclusions
        self.logger.debug("Processing column exclusions")
        processed_df = self._apply_column_exclusions(source_dataframe)
        
        # Execute the specific declarative operation
        self.logger.debug("Executing declarative operation")
        result = self._execute_declarative_operation(processed_df)
        
        self.logger.info("Declarative pipeline write process completed successfully")
        return result
