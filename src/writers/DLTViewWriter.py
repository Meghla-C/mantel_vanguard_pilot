from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter
from pyspark.sql import DataFrame
from typing import Any

# Conditional DLT import - only import if available
try:
    import dlt
    DLT_AVAILABLE = True
except:
    DLT_AVAILABLE = False


class DLTViewWriter(BaseDeclarativePipelineWriter):
    """
    This is a class that implements the BaseDeclarativePipelineWriter interface.
    It writes data using DLT view functionality to create views in declarative pipelines.
    """

    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the DLTViewWriter class

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
        Execute DLT view operation to create a view.
        
        Inputs:
            - source_dataframe: DataFrame, The source dataframe
        Outputs:
            - Any, The result of the DLT view operation, or None if DLT is not available
        """
        # Check if DLT is available
        if not DLT_AVAILABLE:
            self.logger.warning("DLT is not available in this environment. DLTViewWriter cannot be used outside of a DLT pipeline context.")
            return None
            
        target_config = self.entity_config.target
        view_name = target_config.view_name
        
        self.logger.info(f"Executing DLT view operation for view: {view_name}")
        
        # Extract view configuration
        view_config = {}
        if hasattr(target_config, 'comment') and target_config.comment:
            view_config['comment'] = target_config.comment
        if hasattr(target_config, 'table_properties') and target_config.table_properties:
            view_config['table_properties'] = target_config.table_properties

        self.logger.debug(f"DLT view configuration: {view_config}")

        # Register the DLT view with the data preparation function that includes expectations
        self.logger.debug("Registering DLT view with data preparation function")
        prepare_data_func = self._get_prepare_data_with_expectations_function()
        result = dlt.view(prepare_data_func, name=view_name, **view_config)
        
        self.logger.info(f"Successfully executed DLT view operation for view: {view_name}")
        return result
