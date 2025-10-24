import json
from datetime import datetime, timezone
from typing import Dict, List
import requests
import concurrent.futures
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, MapType
from pyspark.sql.functions import from_json, lit
from readers.IDataReader import IDataReader
import core_functions.utils as utils


class AbstractAPIReader(IDataReader):
    """
    This is an abstract class that contains the methods to read data from an API.
    """

    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the APIReader class.

        Inputs:
            - entity_config:  Pydantic model, A dictionary of config for the entity to be read
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
            - logger: Logger object for logging operations (required)
        Outputs:
            - None.
        """
        
        if logger is None:
            raise ValueError("Logger is required for Config class")

        self.entity_config = entity_config
        self.spark = spark
        self.dbutils = dbutils
        self.logger = logger
        self.header_metadata = None
        
        self.logger.info(f"Initialized {self.__class__.__name__} for entity: {entity_config.entity_name}")

    def _add_api_metadata(self, dataframe: DataFrame, entity_config=None) -> DataFrame:
        """
        Add metadata columns to the dataframe

        Inputs:
            - dataframe: DataFrame, The dataframe to which metadata columns will be added
            - entity_config: Pydantic model, A dictionary of config for the entity to be read
        Outputs:
            - DataFrame, The dataframe with metadata columns added
        """
        utils.set_attributes(self, entity_config=entity_config)

        entity_name = self.entity_config.entity_name
        self.logger.debug(f"Adding API metadata to DataFrame for entity: {entity_name}")

        if self._header_metadata is not None:
            metadata = json.dumps(self._header_metadata)
            metadata_schema = MapType(StringType(), StringType())
            df = dataframe.select(
                "*", from_json(lit(metadata), metadata_schema).alias("metadata_map")
            )
            df = utils.add_generic_metadata(
                df, entity_name, "metadata_map", "metadata_map.api_name"
            )
            df = df.drop("metadata_map")
            
            self.logger.debug("Successfully added API metadata to DataFrame")
            return df
        else:
            error_msg = "Error: No response headers info provided. Please run read_source_to_dataframe() first."
            self.logger.error(error_msg)
            raise AttributeError(error_msg)

    def get_api_credentials(self, entity_config=None, spark=None) -> Dict:
        """
        Retrieves the API credentials from Databricks secrets.

        Inputs:
            - entity_config: Pydantic model, A dictionary of config for the entity to be read
            - spark: SparkSession, instance of a spark session
        Outputs:
            - Dict, A dictionary containing the API credentials
        """
        utils.set_attributes(self, entity_config=entity_config, spark=spark)

        self.logger.debug("Retrieving API credentials from Databricks secrets")
        
        scope_name = "dbw" + utils.get_current_catalog(self.spark) + "config"
        host_endpoint = self.dbutils.secrets.get(
            scope=scope_name, key=self.entity_config.source.host_name_secret_key
        )
        api_url = host_endpoint + self.entity_config.source.path
        username = self.dbutils.secrets.get(
            scope=scope_name, key=self.entity_config.source.username_secret_key
        )
        password = self.dbutils.secrets.get(
            scope=scope_name, key=self.entity_config.source.password_secret_key
        )

        self.logger.debug(f"Retrieved credentials for host: {host_endpoint}")
        
        return {
            "host_endpoint": host_endpoint,
            "api_url": api_url,
            "username": username,
            "password": password,
        }

    def make_http_request(
        self,
        host_endpoint: str,
        api_url: str,
        username: str,
        password: str,
        entity_config=None,
    ) -> List | str:
        """
        Makes an HTTP GET request to the specified API URL with basic authentication.

        Inputs:
            - host_endpoint: str, The host endpoint for the API
            - api_url: str, The API URL to which the request will be made
            - username: str, The username for basic authentication
            - password: str, The password for basic authentication
            - entity_config: Pydantic model, A dictionary of config for the entity to be read
        Outputs:
            - List or str, The response data from the API
        """

        utils.set_attributes(self, entity_config=entity_config)

        self.logger.info(f"Making HTTP request to API: {api_url}")
        self.logger.debug(f"Host endpoint: {host_endpoint}")

        # Make the API request with basic authentication
        response = requests.get(
            api_url,
            auth=(username, password),
            headers={"Accept": "application/json"},
            timeout=(15, 180),
        )
        api_response_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Check if the request was successful
        if response.status_code == 200:
            self.logger.info(f"API request successful - Status: {response.status_code}")
            
            # Save header info of this response
            self._header_metadata = dict(response.headers)
            self._header_metadata["api_response_time"] = api_response_time
            self._header_metadata["api_endpoint"] = host_endpoint
            self._header_metadata["api_name"] = self.entity_config.source.path

            data = response.json()
            self._header_metadata["api_response_data_volume"] = str(len(data))
            
            self.logger.debug(f"API response data volume: {len(data)} records")
            if len(data) == 0:
                self.logger.warning("API response data volume is 0")

            if isinstance(data, list):
                return [json.dumps(d) for d in data]
            elif isinstance(data, dict):
                return json.dumps(data)
            else:
                error_msg = f"Return type doesnt match expectation (List, Dict) got ({type(data)}). Status code: {response.status_code}"
                self.logger.error(error_msg)
                raise TypeError(error_msg)

        else:
            error_msg = f"Failed to fetch data from API. Status code: {response.status_code}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

    def make_concurrent_http_requests(
        self,
        url_list: List,
        host_endpoint: str,
        username: str,
        password: str,
        entity_config=None,
    ) -> List:
        """
        Makes concurrent requests to urls specified in url_list.

        Inputs:
            - url_list: List, A list of URLs to which the requests will be made
            - host_endpoint: str, The host endpoint for the API
            - username: str, The username for basic authentication
            - password: str, The password for basic authentication
            - entity_config: Pydantic model, A dictionary of config for the entity to be read
        Outputs:
            - List, A list of the results from the requests
        """

        utils.set_attributes(self, entity_config=entity_config)

        self.logger.info(f"Making concurrent HTTP requests to {len(url_list)} URLs")
        self.logger.debug(f"Host endpoint: {host_endpoint}")

        call_results = []

        # run the tests in parallel to accelerate api calls
        with concurrent.futures.ThreadPoolExecutor() as executor:

            future_to_result = {
                executor.submit(
                    self.make_http_request,
                    host_endpoint,
                    url,
                    username,
                    password,
                    self.entity_config,
                ): url
                for url in url_list
            }

            for future in concurrent.futures.as_completed(future_to_result):
                result = future_to_result[future]
                try:
                    data = future.result()
                    call_results.append(data)
                except Exception as e:
                    error_msg = f"Error in concurrent request to {result}: {str(e)}"
                    self.logger.error(error_msg)
                    call_results.append({"error": error_msg})

        self.logger.info(f"Completed concurrent requests - {len(call_results)} results")
        return call_results
