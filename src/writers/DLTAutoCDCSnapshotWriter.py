from writers.BaseDeclarativePipelineWriter import BaseDeclarativePipelineWriter
from pyspark.sql import DataFrame
from typing import Any

# Conditional DLT import - only import if available
try:
    import dlt
    DLT_AVAILABLE = True
except:
    DLT_AVAILABLE = False


class DLTAutoCDCSnapshotWriter(BaseDeclarativePipelineWriter):
    """
    This is a class that implements the BaseDeclarativePipelineWriter interface.
    It writes data using DLT create_auto_cdc_from_snapshot_flow functionality.
    """

    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the DLTAutoCDCSnapshotWriter class

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
        Execute DLT create_auto_cdc_from_snapshot_flow operation for snapshot-based change data capture.
        
        Inputs:
            - source_dataframe: DataFrame, The source dataframe
        Outputs:
            - Any, The result of the DLT create_auto_cdc_from_snapshot_flow operation, or None if DLT is not available
        """
        # Check if DLT is available
        if not DLT_AVAILABLE:
            self.logger.warning("DLT is not available in this environment. DLTAutoCDCSnapshotWriter cannot be used outside of a DLT pipeline context.")
            return None
            
        table_name = self.entity_config.target.table_name
        source_table = self.entity_config.source.table_name
        keys = getattr(self.entity_config.target, 'keys', None)
        stored_as_scd_type = getattr(self.entity_config.target, 'stored_as_scd_type', None)
        track_history_column_list = getattr(self.entity_config.target, 'track_history_column_list', None)
        track_history_except_column_list = getattr(self.entity_config.target, 'track_history_except_column_list', None)
        
        self.logger.info(f"Executing DLT create_auto_cdc_from_snapshot_flow operation for table: {table_name}")
        self.logger.debug(f"Snapshot CDC configuration - Keys: {keys}, SCD type: {stored_as_scd_type}")
        self.logger.debug(f"Track history columns: {track_history_column_list}, Except columns: {track_history_except_column_list}")

        # Create table if it doesn't exist
        self.logger.debug("Creating target table if it doesn't exist")
        self._create_table_if_not_exists()
        
        # Use DLT create_auto_cdc_from_snapshot_flow for snapshot-based change data capture
        result = dlt.create_auto_cdc_from_snapshot_flow(
            target=table_name,
            source=source_table,
            keys=keys,
            stored_as_scd_type=stored_as_scd_type,
            track_history_column_list=track_history_column_list,
            track_history_except_column_list=track_history_except_column_list
        )
        
        self.logger.info(f"Successfully executed DLT create_auto_cdc_from_snapshot_flow operation for table: {table_name}")
        return result
