from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter
from pyspark.sql import DataFrame
from typing import Any

# Conditional DLT import - only import if available
try:
    import dlt
    DLT_AVAILABLE = True
except:
    DLT_AVAILABLE = False


class DLTSinkWriter(BaseDeclarativePipelineWriter):
    """
    This is a class that implements the BaseDeclarativePipelineWriter interface.
    It writes data using DLT create_sink functionality.
    """

    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the DLTSinkWriter class

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
        Execute DLT create_sink operation to create a sink for data output.
        
        Inputs:
            - source_dataframe: DataFrame, The source dataframe
        Outputs:
            - Any, The result of the DLT create_sink operation, or None if DLT is not available
        """
        # Check if DLT is available
        if not DLT_AVAILABLE:
            self.logger.warning("DLT is not available in this environment. DLTSinkWriter cannot be used outside of a DLT pipeline context.")
            return None
            
        sink_name = getattr(self.entity_config.target, 'sink_name', None)
        format_type = getattr(self.entity_config.target, 'format', None)
        options = getattr(self.entity_config.target, 'options', {})
        
        self.logger.info(f"Executing DLT create_sink operation for sink: {sink_name}")
        self.logger.debug(f"Sink configuration - Format: {format_type}, Options: {options}")
        
        # Use DLT create_sink to create a data sink
        result = dlt.create_sink(
            name=sink_name,
            format=format_type,
            options=options
        )
        
        self.logger.info(f"Successfully executed DLT create_sink operation for sink: {sink_name}")
        return result
