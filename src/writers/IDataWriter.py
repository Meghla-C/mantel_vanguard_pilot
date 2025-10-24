from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class IDataWriter(ABC):
    """
    This is an interface for a data writer that writes data to a target system.
    """

    @abstractmethod
    def __init__(self, entity_config, spark, dbutils, logger) -> None:
        """
        Constructor for the IDataWriter interface

        Inputs:
            - entity_config: Pydantic model, A dictionary of config for the entity to be written
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
            - logger: Logger object for logging operations (required)
        Outputs:
            - None
        """
        raise NotImplementedError

    @abstractmethod
    def write_to_target_table(
        self,
        source_dataframe: DataFrame,
        entity_config=None,
        spark=None,
        dbutils=None,
    ) : # -> Any:
        """
        Definition for a method that takes the entity_config and source_dataframe from the constructor
        and persist the dataframe into target place of Delta Lake

        Inputs:
            - source_dataframe: DataFrame, The dataframe to be written to the Delta table
            - entity_config: Pydantic model, A dictionary of config for the entity to be written
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
        Outputs:
            - None
        """
        raise NotImplementedError
