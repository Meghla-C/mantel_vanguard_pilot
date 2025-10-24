from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter
from pyspark.sql import DataFrame
from typing import Any

# Conditional DLT import - only import if available
try:
    import dlt
    DLT_AVAILABLE = True
except:
    DLT_AVAILABLE = False


class DLTTableWriter(BaseDeclarativePipelineWriter):
    """
    This is a class that implements the BaseDeclarativePipelineWriter interface.
    It writes data using DLT table functionality.
    """

    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the DLTTableWriter class

        Inputs:
            - entity_config: Pydantic model, A dictionary of config for the entity to be written
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
            - logger: Logger object for logging operations (required)
        Outputs:
            - None
        """
        super().__init__(entity_config, spark, dbutils, logger)

    def _execute_declarative_operation(self, source_dataframe: DataFrame) -> Any:
        """
        Execute DLT table operation to create a streaming table.
        
        Inputs:
            - source_dataframe: DataFrame, The source dataframe
        Outputs:
            - Any, The result of the DLT table operation, or None if DLT is not available
        """
        # Check if DLT is available
        if not DLT_AVAILABLE:
            self.logger.warning("DLT is not available in this environment. DLTTableWriter cannot be used outside of a DLT pipeline context.")
            return None
            
        target_config = self.entity_config.target
        table_name = target_config.table_name
        
        self.logger.info(f"Executing DLT table operation for table: {table_name}")
        
        # Extract table configuration - include all dlt.table parameters
        table_config = {}
        
        # Basic table metadata
        if hasattr(target_config, 'comment') and target_config.comment:
            table_config['comment'] = target_config.comment
        
        # Spark and table configuration
        if hasattr(target_config, 'spark_conf') and target_config.spark_conf:
            table_config['spark_conf'] = target_config.spark_conf
        if hasattr(target_config, 'table_properties') and target_config.table_properties:
            table_config['table_properties'] = target_config.table_properties
        
        # Storage and partitioning
        if hasattr(target_config, 'path') and target_config.path:
            table_config['path'] = target_config.path
        if hasattr(target_config, 'partition_cols') and target_config.partition_cols:
            table_config['partition_cols'] = target_config.partition_cols
        
        # Clustering configuration
        if hasattr(target_config, 'cluster_by_auto'):
            table_config['cluster_by_auto'] = target_config.cluster_by_auto
        if hasattr(target_config, 'cluster_by') and target_config.cluster_by:
            table_config['cluster_by'] = target_config.cluster_by
        
        # Schema and filtering
        if hasattr(target_config, 'schema') and target_config.schema:
            table_config['schema'] = target_config.schema
        if hasattr(target_config, 'row_filter') and target_config.row_filter:
            table_config['row_filter'] = target_config.row_filter
        
        # Temporary table flag
        if hasattr(target_config, 'temporary'):
            table_config['temporary'] = target_config.temporary

        self.logger.debug(f"DLT table configuration: {table_config}")

              
        # Register the DLT table with the data preparation function that includes expectations
        self.logger.debug("Registering DLT table with data preparation function")
        prepare_data_func = self._get_prepare_data_with_expectations_function()
        result = dlt.table(prepare_data_func, name=table_name, **table_config)
        
        self.logger.info(f"Successfully executed DLT table operation for table: {table_name}")
        return result
