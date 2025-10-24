from readers.IDataReader import IDataReader
from pyspark.sql import DataFrame
import core_functions.utils as utils
from datetime import timedelta
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as F


class TableBatchReader(IDataReader):
    """
    Table Batch Reader class implementing the IDataReader class
    """

    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the TableBatchReader class

        Inputs:
            - spark SparkSession, instance of a spark session
            - entity_config Pydantic model, A dictionary of config for the entity to be read
            - dbutils Object, Databricks Utilities is not used in this class
            - logger: Logger object for logging operations (required)
        Outputs:
            - None.
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

    def _filter_by_time_range(
        self, dataframe: DataFrame, min_updated_datetime: timedelta
    ) -> DataFrame:
        """
        Filter the dataframe by max_updated_time and min_updated_datetime.

        Inputs:
            - dataframe, The dataframe to filter
            - min_updated_datetime, The minimum updated time in the dataframe
        Outputs:
            - Filtered dataframe.
        """
        self.logger.debug(f"Filtering DataFrame by time range, min_updated_datetime: {min_updated_datetime}")
        
        filter_condition = (
            dataframe[self.entity_config.source.source_watermark_column]
            >= min_updated_datetime
        )

        filtered_df = dataframe.filter(filter_condition)
        return filtered_df

    def read_source_to_dataframe(
        self, entity_config=None, spark=None, dbutils=None
    ) -> DataFrame:
        """
        Method to read the latest batch of data from the source table.
        If a select_query is provided in the entity_config, it will be executed to subset the data
        from the source table. Otherwise, the entire source table will be read.

        If a filter_condition is provided in the entity_config, it will be applied to the data read from the source table.

        If a watermark_table_name is provided in the entity_config, the min time to subset from source table will be calculated based on
        the last time the watermark table was updated (max time in the watermark table).

        Inputs:
            - entity_config Pydantic model, A dictionary of config for the entity to be read
            - spark SparkSession, instance of a spark session
            - dbutils Object, Databricks Utilities is not used in this class
        Outputs:
            - Streaming DataFrame, latest batch of Data from the entity from the ingest layer defined by
              the entity_config
        """

        self.logger.info("Starting table batch reading process")
        
        if not entity_config:
            entity_config = self.entity_config
        if not spark:
            spark = self.spark
        if not dbutils:
            dbutils = self.dbutils

        # Read data from source table
        if self.entity_config.source.select_query:
            self.logger.info("Using custom SQL query to read source data")
            self.logger.debug(f"SQL query: {self.entity_config.source.select_query}")
            try:
                source_df = spark.sql(self.entity_config.source.select_query)
                self.logger.info("Successfully executed custom SQL query")
            except AnalysisException as E:
                error_msg = f"Failed to execute SQL query: {E}"
                self.logger.error(error_msg)
                raise AnalysisException(error_msg)
        else:
            self.logger.info(f"Reading entire table: {self.entity_config.source.table_name}")
            self.logger.debug(f"Read options: {self.entity_config.source.read_options}")
            source_df = spark.read.options(
                **self.entity_config.source.read_options
            ).table(self.entity_config.source.table_name)

        # Log initial data metrics
        self.logger.info(f"Initial source data loaded columns: {len(source_df.columns)}")
        self.logger.debug(f"Source DataFrame schema: {source_df.schema}")

        # Apply filter condition if specified
        if self.entity_config.source.filter_condition:
            self.logger.info("Applying filter condition to source data")
            self.logger.debug(f"Filter condition: {self.entity_config.source.filter_condition}")
            source_df = source_df.filter(self.entity_config.source.filter_condition)

        min_time = None

        # Handle different load types
        if self.entity_config.source.load_type == "full":
            self.logger.info("Full load type detected - loading all data without time filtering")
            source_df = self._add_table_metadata(source_df)
            self.logger.info("Table batch reading process completed successfully (full load)")
            return source_df
            
        elif self.entity_config.source.watermark_table_name:
            self.logger.info(f"Using watermark table: {self.entity_config.source.watermark_table_name}")
            if spark.catalog.tableExists(self.entity_config.source.watermark_table_name):
                self.logger.debug("Watermark table exists, calculating min time from watermark")
                min_time = (
                    spark.read.table(self.entity_config.source.watermark_table_name)
                    .select(self.entity_config.source.watermark_table_datetime_column)
                    .agg(
                        {
                            self.entity_config.source.watermark_table_datetime_column: "max"
                        }
                    )
                    .collect()[0][0]
                )
                self.logger.info(f"Watermark table min time: {min_time}")
            if not min_time:
                min_time = "1900-01-01 00:00:00"
                self.logger.warning("No watermark time found, using default start date: 1900-01-01")
                
        elif self.entity_config.source.enable_file_watermarking:
            self.logger.info("File watermarking enabled, using default start date")
            min_time = "1900-01-01 00:00:00"
            
        else:
            self.logger.info("Calculating min time from source data")
            max_updated_time = (
                source_df.select(self.entity_config.source.source_watermark_column)
                .agg({self.entity_config.source.source_watermark_column: "max"})
                .collect()[0][0]
            )

            if max_updated_time is None:
                self.logger.warning("No max updated time found in source data, returning None")
                return None
            # later we are subsetting anything from source where data is > min_time, so we need
            # slight offset to ensure we get the latest batch when we use max_updated_time on souce (time delta = 0-0.000001)
            # and that we dont get the same batch again when we use watermark table)
            min_time = max_updated_time - timedelta(
                minutes=self.entity_config.source.batch_window
            )

        source_df = self._filter_by_time_range(source_df, min_time)

        source_df = self._add_table_metadata(source_df)

        self.logger.info(f"Table batch reading process completed successfully - Final DataFrame: {len(source_df.columns)} columns")
        return source_df
