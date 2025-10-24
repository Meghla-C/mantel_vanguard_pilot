from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from readers.AbstractAPIReader import AbstractAPIReader
import core_functions.utils as utils


class APIReader(AbstractAPIReader):
    """
    This is a class that implements the AbstractAPIReader class.
    It makes one call to api_url and returns the result as a DataFrame.
    """

    def read_source_to_dataframe(
        self, entity_config=None, spark=None, dbutils=None
    ) -> Optional[DataFrame]:
        """
        Method that takes the entity_config and gets data from api defined within
        and puts the data into a DataFrame

        Inputs:
            - entity_config: Pydantic model, A dictionary of config for the entity to be read
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
        Outputs:
            - DataFrame, The dataframe with metadata columns added
        """
        self.logger.info("Starting API data reading process")
        
        utils.set_attributes(
            self, entity_config=entity_config, spark=spark, dbutils=dbutils
        )

        # get credentials
        self.logger.debug("Retrieving API credentials")
        api_credentials = self.get_api_credentials()
        self.logger.debug(f"Retrieved credentials for host: {api_credentials.get('host_endpoint', 'Unknown')}")

        # get data
        self.logger.info("Making HTTP request to API endpoint")
        self.logger.debug(f"API URL: {api_credentials.get('api_url', 'Unknown')}")
        
        data_as_strings = self.make_http_request(
            host_endpoint=api_credentials["host_endpoint"],
            api_url=api_credentials["api_url"],
            username=api_credentials["username"],
            password=api_credentials["password"],
            entity_config=self.entity_config,
        )
        
        # Convert the JSON data to a DataFrame
        self.logger.info("Converting API response to Spark DataFrame")
        try:
            source_df = spark.createDataFrame(data_as_strings, StringType())
            self.logger.info(f"Successfully created DataFrame with {len(source_df.columns)} columns")
            self.logger.debug(f"DataFrame schema: {source_df.schema}")
        except Exception as e:
            self.logger.error(f"Failed to create DataFrame from API response: {str(e)}")
            raise e

        source_df = self._add_api_metadata(source_df)

        self.logger.info("API data reading process completed successfully")
        return source_df
