from readers.IDataReader import IDataReader
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, from_json
import core_functions.utils as utils


class FileStreamReader(IDataReader):
    """
    This is a class that implements the IDataReader interface.
    It uses the Databricks Auto Loader to read files from a specified path.
    """

    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the FileStreamReader class

        Inputs:
            - entity_config: Pydantic model, A dictionary of config for the entity to be read
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
            - logger: Logger object for logging operations (required)
        Outputs:
            - None
        """

        self.spark = spark
        self.entity_config = entity_config
        self.dbutils = dbutils
        self.logger = logger
        
        self.logger.info(f"Initialized {self.__class__.__name__} for entity: {entity_config.entity_name}")

    def _add_file_metadata(self, dataframe: DataFrame, entity_config=None) -> DataFrame:
        """
        Add metadata columns to the dataframe

        Inputs:
            - dataframe: DataFrame, The dataframe to which metadata columns will be added
            - entity_config: Pydantic model, A dictionary of config for the entity to be read
        Outputs:
            - DataFrame, The dataframe with metadata columns added
        """

        entity_name = self.entity_config.entity_name
        self.logger.debug(f"Adding file metadata to DataFrame for entity: {entity_name}")

        df = utils.add_generic_metadata(
            dataframe, entity_name, "_metadata", "_metadata.file_name"
        )
        df = df.withColumn(
            "md_file_modification_time", col("_metadata.file_modification_time")
        )

        self.logger.debug("Successfully added file metadata to DataFrame")
        return df

    def read_source_to_dataframe(
        self, entity_config=None, spark=None, dbutils=None
    ) -> DataFrame:
        """
        Reads the source data from the specified path and returns a DataFrame.

        Inputs:
            - entity_config: Pydantic model, A dictionary of config for the entity to be read
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
        Outputs:
            - DataFrame: A Spark DataFrame containing the data read from the source.
        """

        self.logger.info("Starting file stream reading process")
        
        if not entity_config:
            entity_config = self.entity_config
        if not spark:
            spark = self.spark
        if not dbutils:
            dbutils = self.dbutils

        # Get configuration parameters
        self.logger.debug("Retrieving file reading configuration")
        read_options = self.entity_config.source.read_options
        source_volume_path = utils.get_source_volume_path(self.entity_config, spark)
        schema_location = utils.get_schema_location(self.entity_config, spark)
        source_format = self.entity_config.source.format
        explode_column = self.entity_config.source.explode_column

        # Log configuration details
        self.logger.info(f"Reading files from source path: {source_volume_path}")
        self.logger.debug(f"File format: {source_format}")
        self.logger.debug(f"Schema location: {schema_location}")
        self.logger.debug(f"Read options: {read_options}")
        if explode_column:
            self.logger.debug(f"Explode column: {explode_column}")

        # Create streaming DataFrame
        self.logger.info("Creating streaming DataFrame with Auto Loader")
        try:
            source_df = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", source_format)
                .options(**read_options)
                .option("cloudFiles.schemaLocation", schema_location)
                .load(source_volume_path)
            )
            self.logger.info("Successfully created streaming DataFrame")
            self.logger.debug(f"Streaming DataFrame schema: {source_df.schema}")
        except Exception as e:
            error_msg = f"Failed to create streaming DataFrame: {str(e)}"
            self.logger.error(error_msg)
            raise e

        # Apply explode operation if specified
        if explode_column:
            self.logger.info(f"Applying explode operation on column: {explode_column}")
            try:
                source_df = source_df.withColumn(
                    explode_column, explode(from_json(col(explode_column), "array<string>"))
                )
                self.logger.info("Successfully applied explode operation")
                self.logger.debug(f"DataFrame schema after explode: {source_df.schema}")
            except Exception as e:
                error_msg = f"Failed to apply explode operation on column {explode_column}: {str(e)}"
                self.logger.error(error_msg)
                raise

        # Add file metadata
        source_df = self._add_file_metadata(source_df)

        self.logger.info("File stream reading process completed successfully")
        return source_df
