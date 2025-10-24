from readers.IDataReader import IDataReader
from pyspark.sql import DataFrame
import core_functions.utils as utils


class TableStreamReader(IDataReader):
    """
    This is a class that implements the IDataReader interface.
    It reads data from a table in streaming mode using Spark.
    """

    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the TableStreamReader class

        Inputs:
            - entity_config: Pydantic model, A dictionary of config for the entity to be read
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
            - logger: Logger object for logging operations (required)
        Outputs:
            - None
        """

        self.spark = spark
        self.dbutils = dbutils
        self.entity_config = entity_config
        self.logger = logger
        
        self.logger.info(f"Initialized {self.__class__.__name__} for entity: {entity_config.entity_name}")

    def _add_table_metadata(
            self, dataframe: DataFrame, entity_config=None
        ) -> DataFrame:
        """
        Add metadata columns to the dataframe
        Inputs:
            - dataframe Dataframe, The Dataframe to add the metadata columns too
            - entity_config Pydantic model, A dictionary of config for the entity to be read
        Outputs:
            - Dataframe which now includes the metadata columns
        """

        self.logger.debug("Adding table metadata to DataFrame")
        
        if self.entity_config.source.keep_source_metadata:
            self.logger.debug("Keeping source metadata columns")
            df = utils.rename_source_metadata_columns(
                dataframe, self.entity_config.source.source_metadata_columns_to_keep
            )
        else:
            self.logger.debug("Dropping source metadata columns")
            df = utils.drop_metadata_columns(dataframe)

        df = utils.add_generic_metadata(
            df,
            self.entity_config.entity_name,
            "map()",
            f"'{self.entity_config.source.table_name}'",
            self.entity_config.source.source_metadata_columns_to_keep,
        )

        if len(self.entity_config.source.generated_metadata_columns_to_remove) > 0:
            self.logger.debug(f"Removing generated metadata columns: {self.entity_config.source.generated_metadata_columns_to_remove}")
            df = df.drop(
                *self.entity_config.source.generated_metadata_columns_to_remove
            )

        self.logger.debug("Successfully added table metadata to DataFrame")
        return df

    def read_source_to_dataframe(
        self, entity_config=None, spark=None, dbutils=None
    ) -> DataFrame:
        """
        Reads the source data from the specified table and returns a DataFrame.

        Inputs:
            - entity_config: Pydantic model, A dictionary of config for the entity to be read
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
        Outputs:
            - DataFrame, The dataframe with metadata columns added
        """

        self.logger.info("Starting table stream reading process")
        
        if not entity_config:
            entity_config = self.entity_config
        if not spark:
            spark = self.spark
        if not dbutils:
            dbutils = self.dbutils

        # Get configuration parameters
        self.logger.debug("Retrieving table streaming configuration")
        read_options = self.entity_config.source.read_options
        select_columns = self.entity_config.source.select_columns
        filter_condition = self.entity_config.source.filter_condition
        source_table_name = self.entity_config.source.table_name

        # Log configuration details
        self.logger.info(f"Reading from source table: {source_table_name}")
        self.logger.debug(f"Read options: {read_options}")         

        # Validate table name format
        if source_table_name == "" or len(source_table_name.split(".")) < 2:
            error_msg = "table_name with its schema name is required in entity_config"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        source_df = spark.readStream.options(**read_options).table(source_table_name)

        # Apply filter condition if specified
        if select_columns and select_columns != "":
            source_df = source_df.select(select_columns)
            self.logger.debug(f"select columns: {select_columns}")
        else:
            self.logger.debug("No select columns specified")

        # Apply filter condition if specified
        if filter_condition and filter_condition != "":
            source_df = source_df.filter(filter_condition)
            self.logger.debug(f"Filter condition: {filter_condition}")
        else:
            self.logger.debug("No filter condition specified")

        source_df = self._add_table_metadata(source_df)

        self.logger.info("Table stream reading process completed successfully")
        return source_df
