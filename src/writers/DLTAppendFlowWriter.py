from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter
from pyspark.sql import DataFrame

# Conditional DLT import - only import if available
try:
    import dlt
    DLT_AVAILABLE = True
except:
    DLT_AVAILABLE = False


class DLTAppendFlowWriter(BaseDeclarativePipelineWriter):
    """
    This is a class that implements the BaseDeclarativePipelineWriter interface.
    It writes data using DLT append_flow functionality.
    """

    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the DLTAppendFlowWriter class

        Inputs:
            - entity_config: Pydantic model, A dictionary of config for the entity to be written
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
            - logger: Logger object for logging operations (required)
        Outputs:
            - None
        """
        super().__init__(entity_config, spark, dbutils, logger)

    def _execute_declarative_operation(self, source_dataframe: DataFrame) -> None:
        """
        Execute DLT append_flow operation to append data to the target table.
        
        Inputs:
            - source_dataframe: DataFrame, The source dataframe
        Outputs:
            - None, or None if DLT is not available
        """
        # Check if DLT is available
        if not DLT_AVAILABLE:
            self.logger.warning("DLT is not available in this environment. DLTAppendFlowWriter cannot be used outside of a DLT pipeline context.")
            return None
            
        table_name = self.entity_config.target.table_name
        flow_name = getattr(self.entity_config.target, 'flow_name', None) or table_name.replace(".", "_")
        once = getattr(self.entity_config.target, 'once', False)
        spark_conf = getattr(self.entity_config.target, 'spark_conf', {})
        comment = getattr(self.entity_config.target, 'comment', None)
        
        self.logger.info(f"Executing DLT append_flow operation for table: {table_name}")
        self.logger.debug(f"Flow name: {flow_name}, Once: {once}, Comment: {comment}")
        
        # Create table if it doesn't exist
        self.logger.debug("Creating target table if it doesn't exist")
        self._create_table_if_not_exists()
        
        # Create the append flow function that processes the source dataframe
        self.logger.debug("Creating DLT append flow")
        result = dlt.append_flow(
            target=table_name,
            name=flow_name,
            once=once,
            spark_conf=spark_conf,
        ) (self._prepare_data)
        
        self.logger.info(f"Successfully executed DLT append_flow operation for table: {table_name}")
        return result
