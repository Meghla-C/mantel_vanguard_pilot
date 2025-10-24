from abc import ABC, abstractmethod
from typing import Dict
from pyspark.sql import DataFrame


class IDataTransform(ABC):
    """
    Abstract Data Transform Interface class for concrete class implementation
    """

    @abstractmethod
    def __init__(self, source_df: DataFrame, entity_config, spark, dbutils, logger):
        """
        constructor for the IDataTransform interface,
        meaning that the concrete class implementation requires this method

        Inputs:
            - source_df : DataFrame, The Dataframe to be transformed
            - entity_config : Pydantic model, A dictionary of config for the entity to be transformed
            - spark SparkSession, instance of a spark session
            - dbutils Object, Databricks Utilities works with secrets
            - logger Logger object for logging operations (required)
        Outputs:
            - None.
        """
        raise NotImplementedError

    @abstractmethod
    def apply_transform(
        self, source_df: DataFrame, entity_config, spark, dbutils
    ) -> Dict[str, DataFrame]:
        """
        Definition for a method that takes the entity_config from the constructor and the source_df and applies
        the transformation to the data and returns a dictionary of DataFrames that have been transformed

        Inputs:
            - source_df : DataFrame, The Dataframe to be transformed
            - entity_config  Pydantic model, A dictionary of config for the entity to be transformed
            - spark SparkSession, instance of a spark session
            - dbutils Object, Databricks Utilities works with secrets
        Outputs:
            - Dict, A dictionary of DataFrames that have been transformed
        """
        raise NotImplementedError
