from abc import ABC, abstractmethod
from typing import Dict
from pyspark.sql import DataFrame


class IDataReader(ABC):
    """
    This is an interface for a data reader that reads data from a source system and puts it into a DataFrame.
    """

    @abstractmethod
    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the IDataReader interface

        Inputs:
            - entity_config:  Pydantic model, A dictionary of config for the entity to be read
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
            - logger: Logger object for logging operations (required)
        Outputs:
            - None.
        """
        raise NotImplementedError

    @abstractmethod
    def read_source_to_dataframe(self, entity_config, spark, dbutils) -> DataFrame:
        """
        Definition for a method that takes the entity_config from the constructor
        and reads the entity from the source and puts the data into a DataFrame

        Inputs:
            - entity_config:  Pydantic model, A dictionary of config for the entity to be read
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
        Outputs:
            - DataFrame: A Spark DataFrame containing the data read from the source.
        """
        raise NotImplementedError
