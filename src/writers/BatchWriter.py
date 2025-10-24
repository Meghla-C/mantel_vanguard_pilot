from writers.IDataWriter import IDataWriter
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pyspark.sql.functions as F


class BatchWriter(IDataWriter):
    """
    This is a class that implements the IDataWriter interface.
    It writes data to a Delta table in batch mode.
    """

    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the BatchWriter class

        Inputs:
            - entity_config: Pydantic model, A dictionary of config for the entity to be written
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
        Outputs:
            - None
        """
        self.spark = spark
        self.dbutils = dbutils
        self.entity_config = entity_config
        self.logger = logger

    def _write_watermark(self, source_dataframe: DataFrame) -> None:
        """
        Method that extracts metadata from the _metadata column of source_dataframe and writes it to the meta table

        Inputs:
            - source_dataframe : DataFrame, its _metadata column contains metadata to be written to the meta table
        Outputs:
            - None
        """

        self.logger.info(f"Writing watermark for entity: {self.entity_config.entity_name}")

        metadata_json_schema = StructType(
            [
                StructField("file_path", StringType(), True),
                StructField("file_name", StringType(), True),
                StructField("file_modification_time", TimestampType(), True),
            ]
        )

        self.logger.debug(f"Metadata JSON schema: {metadata_json_schema}")

        # Extract the metadata from the _metadata column
        source_dataframe = source_dataframe.withColumn(
            "json_metadata",
            F.from_json(F.to_json(F.col("_metadata")), metadata_json_schema),
        )
        metadata_dataframe = source_dataframe.select(
            "json_metadata.file_path",
            "json_metadata.file_name",
            "json_metadata.file_modification_time",
        )
        metadata_dataframe.write.format("delta").mode("append").saveAsTable(
            self.entity_config.target.meta_table_name
        )

    def write_to_target_table(
        self,
        source_dataframe: DataFrame,
        entity_config=None,
        spark=None,
        dbutils=None,
    ) -> None:
        """
        Writes the source dataframe to a Delta table in batch mode.

        Inputs:
            - source_dataframe: DataFrame, The dataframe to be written to the Delta table
            - entity_config: Pydantic model, A dictionary of config for the entity to be written
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
        Outputs:
            - None
        """
        self.logger.info(f"Writing to target table: {self.entity_config.target.table_name}")
        if not entity_config:
            entity_config = self.entity_config
        if not spark:
            spark = self.spark
        if not dbutils:
            dbutils = self.dbutils

        if self.entity_config.target.exclude_columns:
            source_dataframe = source_dataframe.drop(
                *self.entity_config.target.exclude_columns
            )
            self.logger.debug(f"Dropped {len(self.entity_config.target.exclude_columns)} columns from source dataframe")

        if self.entity_config.target.delete_condition:
            delete_statement = f"DELETE FROM {self.entity_config.target.table_name} WHERE {self.entity_config.target.delete_condition}"
            
            self.spark.sql(delete_statement)

            self.logger.debug(f"Deleted statement executed: {delete_statement}")

        source_dataframe.write.format("delta").mode(
            self.entity_config.target.writer_mode
        ).options(**self.entity_config.target.write_options).saveAsTable(
            self.entity_config.target.table_name
        )

        self.logger.info(f"Completed writing to target table: {self.entity_config.target.table_name}")

        if self.entity_config.target.enable_file_watermarking:
            self._write_watermark(source_dataframe)
