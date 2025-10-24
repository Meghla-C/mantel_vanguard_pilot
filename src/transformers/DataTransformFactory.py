from typing import Dict
from pyspark.sql import DataFrame
from core_functions.config_models import EntityConfig
from transformers.IDataTransform import IDataTransform
from transformers.JsonTransform import JsonTransform
from transformers.DefaultTransform import DefaultTransform


class DataTransformFactory:
    """
    DataTransformFactory class to create the appropriate DataTransform object
    """

    @staticmethod
    def get_datatransform(
        source_df: DataFrame, entity_config: Dict, spark, dbutils, logger
    ) -> IDataTransform:
        """
        Method to create the appropriate DataTransform object

        Inputs:
            - source_df: DataFrame, The Dataframe to be transformed
            - entity_config: Dict, A dictionary of config for the entity to be read
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
        Outputs:
            - IDataTransform, An instance of the appropriate DataTransform class
        """
        transformers = {
            "default": DefaultTransform,
            "json": JsonTransform,
        }

        # Check if entity_config is already an EntityConfig object
        if isinstance(entity_config, EntityConfig):
            entity_config = entity_config
        else:
            entity_config = EntityConfig(**entity_config)
        transformer_type = entity_config.transform.transform_type

        logger.info(f"Selected transformer type: {transformer_type}")
        
        return transformers[transformer_type](source_df, entity_config, spark, dbutils, logger)
