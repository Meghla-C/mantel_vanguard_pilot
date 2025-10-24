import os
import pandas as pd
from pyspark.sql import DataFrame
from typing import Dict
from transformers.IDataTransform import IDataTransform
import core_functions.utils as utils


class JsonTransform(IDataTransform):
    """
    JsonTransform class implementing the IDataTransform class interface
    for transforming JSON data into a set of DataFrame
    """

    def __init__(self, source_df: DataFrame, entity_config, spark, dbutils, logger):
        """
        constructor for the IDataTransform interface,
        meaning that the concrete class implementation requires this method

        Inputs:
            - source_df : DataFrame, The Dataframe to be transformed
            - entity_config: Pydantic model, A dictionary of config for the entity to be transformed
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
        Outputs:
            - None.
        """
        self.source_df = source_df
        self.entity_config = entity_config
        self.spark = spark
        self.dbutils = dbutils
        self.logger = logger
        
        self.logger.info(f"JsonTransform initialized for entity: {entity_config.entity_name}")

    def _create_entity_transform(
        self, mapping_df: DataFrame, target_entity: str
    ) -> list:
        """
        Helper function to create the entity transformation
        inputs:
            - mapping_df : DataFrame, The DataFrame containing the mapping configuration
            - target_entity : str, The target entity to be transformed
            - entity_config : Pydantic model, A dictionary of config for the entity to be transformed
            - spark SparkSession, instance of a spark session
        outputs:
            - Dict, A dictionary of transformation expressions
        """
        sql_exprs = []

        # select the rows in the mapping file that have the target_entity
        target_mapping = mapping_df.filter(mapping_df["target_entity"] == target_entity)
        # create a dictionary of transformations
        for row in target_mapping.collect():
            source_column = row["source_column"]
            transformation = row["transformation"]
            target_column = row["target_column"]
            target_datatype = row["target_datatype"]
            operation = row["operation"]

            if operation is not None and operation.lower() == "explode_outer":
                sql_exprs.append(
                    f"explode_outer(from_json({source_column}, 'array<string>'))  as {target_column}"
                )
                continue
            if operation is not None and operation.lower() == "explode":
                sql_exprs.append(
                    f"explode(from_json({source_column}, 'array<string>'))  as {target_column}"
                )
                continue
            if transformation:
                sql_exprs.append(f"{transformation} as {target_column}")
            elif target_datatype.lower() != "string":
                sql_exprs.append(
                    f"CAST({source_column} AS {target_datatype}) as {target_column}"
                )
            else:
                sql_exprs.append(f"{source_column} as {target_column}")

        self.logger.info(f"Created {len(sql_exprs)} transformation expressions for {target_entity} from mapping rules")
        self.logger.debug(f"Transformation expressions: {sql_exprs}")
        return sql_exprs

    def _read_mapping_config(self, entity_config, spark) -> DataFrame:
        """
        Reads the mapping configuration file into a DataFrame.

        Inputs:
            - entity_config: Pydantic model, A dictionary of config for the entity to be transformed
            - spark: SparkSession, instance of a spark session

        Outputs:
            - DataFrame, The DataFrame containing the mapping configuration
        """
        mapping_config_file = "file:" + os.path.join(
            utils.project_path(), self.entity_config.transform.mapping_config
        )

        mapping_config_file = os.path.join(utils.project_path(), self.entity_config.transform.mapping_config)
        
        self.logger.info(f"Loading mapping config from: {mapping_config_file}")

        pd_df = pd.read_csv(mapping_config_file)
        mapping_df = spark.createDataFrame(pd_df)
        
        self.logger.info(f"Mapping config loaded with columns: {mapping_df.columns}")

        return mapping_df

    def apply_transform(
        self, source_df: DataFrame = None, entity_config=None, spark=None
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
        self.logger.info(f"Applying JsonTransform for entity: {self.entity_config.entity_name}")
        
        TARGET_ENTITY_COLUMN = "target_entity"
        if not source_df:
            source_df = self.source_df
        if not spark:
            spark = self.spark

        transformations = {}

        mapping_df = self._read_mapping_config(self.entity_config, spark)

        ordered_target_entities = utils.distict_order_col_from_df(
            mapping_df, TARGET_ENTITY_COLUMN
        )
        
        self.logger.debug(f"Processing target entities: {ordered_target_entities}")

        for target_entity in ordered_target_entities:
            transformations[target_entity] = self._create_entity_transform(
                mapping_df, target_entity
            )

        return_dict = {}

        # Apply the transformations to the source_df
        for k, v in transformations.items():
            filtered_df = mapping_df.filter(f"target_entity = '{k}'")
            src_df_name = filtered_df.first()["source_df"]

            if src_df_name is not None:
                src_df = return_dict[src_df_name]
            else:
                src_df = source_df
            return_dict[k] = src_df.selectExpr(v)

        self.logger.info(f"JsonTransform completed. Returning {len(return_dict)} transformed DataFrames")
        return return_dict
