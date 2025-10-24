from typing import Dict
from pyspark.sql import DataFrame
from transformers.IDataTransform import IDataTransform


class DefaultTransform(IDataTransform):
    """
    DefaultTransform class that performs no transformation
    """

    def __init__(self, source_df: DataFrame, entity_config, spark, dbutils, logger):
        """
        constructor for the IDataTransform interface,

        Inputs:
            - source_df : DataFrame, The Dataframe to be transformed
            - entity_config: Pydantic model, A dictionary of config for the entity to be transformed
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities
            - logger: Logger object for logging operations (required)
        Outputs:
            - None.
        """
        self.source_df = source_df
        self.entity_config = entity_config
        self.spark = spark
        self.dbutils = dbutils
        self.logger = logger

    def apply_transform(
        self, source_df: DataFrame = None, entity_config=None, spark=None
    ) -> Dict[str, DataFrame]:
        """
        Returns the source DataFrame without any transformation
        """
        self.logger.info(f"Applying DefaultTransform for entity: {self.entity_config.entity_name}")
        return {self.entity_config.entity_name: self.source_df}
