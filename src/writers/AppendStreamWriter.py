from writers.IDataWriter import IDataWriter
import core_functions.utils as utils
from pyspark.sql import DataFrame
from typing import Dict


class AppendStreamWriter(IDataWriter):
    """
    This is a class that implements the IDataWriter interface.
    It writes data to a Delta table in streaming mode.
    """

    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the AppendStreamWriter class

        Inputs:
            - spark SparkSession, instance of a spark session
            - entity_config : Pydantic model, A dictionary of config for the entity to be written
            - dbutils Object, Databricks Utilities is not used in this class but required for the interface
        Outputs:
            - None.
        """
        self.spark = spark
        self.dbutils = dbutils
        self.entity_config = entity_config
        self.logger = logger

    def write_to_target_table(
        self,
        source_dataframe: DataFrame,
        entity_config=None,
        spark=None,
        dbutils=None,
    ) -> None:
        """
        Method that takes the entity_config and source_dataframe from the constructor
        and persist the dataframe into target place of Delta Lake

        Inputs:
            - source_dataframe : Stream DataFrame, Data from the entity to be written into target place of Delta Lake
            - spark SparkSession, instance of a spark session, Optional
            - entity_config : Pydantic model, A dictionary of config for the entity to be written
            - dbutils Object, Databricks Utilities is not used in this class but required for the interface
        Outputs:
            - None
        """
        table_name = self.entity_config.target.table_name
        self.logger.info(f"Starting streaming write operation to target table: {table_name}")
        
        try:
            if not entity_config:
                entity_config = self.entity_config
            if not spark:
                spark = self.spark
            if not dbutils:
                dbutils = self.dbutils

            # Log streaming configuration details
            trigger_config = self.entity_config.target.trigger_type_value
            self.logger.debug(f"Streaming configuration - Trigger: {trigger_config}")
            
            checkpoint_location = utils.get_checkpoint_location(self.entity_config, spark)
            self.logger.info(f"Using checkpoint location: {checkpoint_location}")
            
            # Log dataframe schema for debugging
            self.logger.debug(f"Source dataframe schema: {source_dataframe.schema}")
            
            self.logger.info(f"Configuring streaming write with format=delta, outputMode=append")
            
            # Start the streaming write operation
            streaming_query = (
                source_dataframe.writeStream.format("delta")
                .option("checkpointLocation", checkpoint_location)
                .outputMode("append")
                .trigger(**trigger_config)
                .toTable(table_name)
            )
            
            self.logger.info(f"Streaming query started for table: {table_name}")
            self.logger.debug(f"Query ID: {streaming_query.id}")
            
            # Wait for termination
            self.logger.info("Waiting for streaming query termination...")
            streaming_query.awaitTermination()
            
            self.logger.info(f"Streaming write operation completed successfully for table: {table_name}")
            
        except Exception as e:
            self.logger.error(f"Error during streaming write operation to table {table_name}: {str(e)}")
            self.logger.error(f"Exception type: {type(e).__name__}")
            raise
