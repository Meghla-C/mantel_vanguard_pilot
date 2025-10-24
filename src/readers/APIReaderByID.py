import json
from typing import Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from readers.AbstractAPIReader import AbstractAPIReader
import core_functions.utils as utils


class APIReaderByID(AbstractAPIReader):
    """
    This is a class that implements the AbstractAPIReader class.
    It makes initial call to api_url and then makes concurrent calls to the api_url
    for each id in the response. It returns the result as a DataFrame.
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
        self.logger.info("Starting API data reading process with ID-based concurrent requests")
        self.logger.debug(f"Entity config: {entity_config.entity_name if entity_config else 'None'}")
        
        utils.set_attributes(
            self, entity_config=entity_config, spark=spark, dbutils=dbutils
        )

        # get credentials
        self.logger.debug("Retrieving API credentials")
        api_credentials = self.get_api_credentials()
        self.logger.debug(f"Retrieved credentials for host: {api_credentials.get('host_endpoint', 'Unknown')}")

        # get initial data to extract IDs
        self.logger.info("Making initial HTTP request to get ID list")
        self.logger.debug(f"Initial API URL: {api_credentials.get('api_url', 'Unknown')}")
        
        data_as_strings = self.make_http_request(
            host_endpoint=api_credentials["host_endpoint"],
            api_url=api_credentials["api_url"],
            username=api_credentials["username"],
            password=api_credentials["password"],
            entity_config=self.entity_config,
        )

        # Extract IDs and build URL list
        self.logger.info("Extracting IDs from initial response and building URL list")
        try:
            api_url_list = [
                api_credentials["api_url"]
                + "/"
                + json.loads(data_val)[self.entity_config.source.id_key]
                for data_val in data_as_strings
            ]
            self.logger.info(f"Built {len(api_url_list)} URLs for concurrent requests")
            self.logger.debug(f"First few URLs: {api_url_list[:3] if len(api_url_list) >= 3 else api_url_list}")
        except KeyError as e:
            error_msg = f"Failed to extract ID key '{self.entity_config.source.id_key}' from response: {str(e)}"
            self.logger.error(error_msg)
            raise KeyError(error_msg)
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse JSON from response: {str(e)}"
            self.logger.error(error_msg)
            raise json.JSONDecodeError(error_msg, e.doc, e.pos)
        except Exception as e:
            error_msg = f"Unexpected error while building URL list: {str(e)}"
            self.logger.error(error_msg)
            raise

        call_results = self.make_concurrent_http_requests(
            url_list=api_url_list,
            host_endpoint=api_credentials["host_endpoint"],
            username=api_credentials["username"],
            password=api_credentials["password"],
        )

        # Convert the JSON data to a DataFrame
        self.logger.info("Converting API response to Spark DataFrame")
        try:
            source_df = spark.createDataFrame(call_results, StringType())
            self.logger.info(f"Successfully created DataFrame with {len(source_df.columns)} columns")
            self.logger.debug(f"DataFrame schema: {source_df.schema}")
        except Exception as e:
            self.logger.error(f"Failed to create DataFrame from API response: {str(e)}")
            raise

        source_df = self._add_api_metadata(source_df)

        self.logger.info("API data reading process with ID-based concurrent requests completed successfully")
        return source_df
