import yaml
import os
import uuid
from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, min


def set_attributes(class_instance, **kwargs):
    """
    Update provided class's attributes.

    Input:
        - class_instance : Class, instance of any class which attributes should be updated
        - **kwards : key value pairs of attribute name and value that should be updated
    """
    for key, value in kwargs.items():
        if value is not None:
            setattr(class_instance, key, value)


def get_current_catalog(spark) -> str:
    """
    Returns the current catalog
    """
    return spark.sql("SELECT current_catalog()").collect()[0][0]


def get_full_volume_path(schema_volume_path: str, spark) -> str:
    """
    Returns the full volume path.
    Will return the full path with either the environment depending on the volume path provided.

    Inputs:
        - schema_volume_path: str, the schema, volume and folder part of a volume
    """  

    env = get_current_catalog(spark)
    
    # Check if schema_volume_path already starts with "/Volumes/"
    if schema_volume_path.lower().startswith("/volumes/"):
        environment_volume_path = schema_volume_path
    else:
        environment_volume_path = "/Volumes/" + env + "/" + schema_volume_path

    if not os.path.exists(os.path.dirname(environment_volume_path)):
        raise FileNotFoundError(
            f"Volume path '{schema_volume_path}' does not exist in the environment '{env}'."
        )
    return environment_volume_path


def get_source_volume_path(entity_config: Dict, spark) -> str:
    """
    Return the full volume path for the source data.

    Inputs:
        - entity_config: Dict, A dictionary of config for the entity to be read
    """

    return get_full_volume_path(entity_config.source.volume_path, spark)


def get_schema_name(entity_config: Dict) -> str:
    """
    Get the target schema name from the entity_config dictionary
    """

    return entity_config.target.table_name.split(".", 1)[0]


def get_schema_location(entity_config: Dict, spark):
    """
    Return the schema location. If not provided schema location,
    will use default schema location

    Inputs:
        - entity_config: Dict, A dictionary of config for the entity to be read.
    """
    schema_location = entity_config.source.schema_location
    schema_name = get_schema_name(entity_config)

    if not schema_location:
        schema_location = get_full_volume_path(
            schema_name + "/schema/" + entity_config.entity_name, spark
        )
    return schema_location


def get_checkpoint_location(entity_config: Dict, spark):
    """
    Return the checkpoint location.

    Inputs:
        - entity_config: Dict, A dictionary of config for the entity to be read
    """

    schema_name = get_schema_name(entity_config)

    checkpoint_location = get_full_volume_path(
        schema_name + "/checkpoint/" + entity_config.entity_name, spark
    )

    return checkpoint_location


def project_path() -> str:
    """
    Return the path of the current project
    """

    path = os.path.abspath("")

    if not path or "src" not in path:
        raise AttributeError(f"Initialized in an incorrect directory, no src in {path}")
    else:
        parsed_path = path.split("src")[0]

    return parsed_path


def read_yaml(file_path: str) -> Dict:
    """
    Function that reads a yaml file given a file path and returns a Dictionary
    of yaml file contents.

    Inputs:
        - file_path : str, path to yaml file
    Outputs:
        - data : Dict, contents of the yaml file
    """

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"The configuration file '{file_path}' was not found.")

    with open(file_path, "r") as file:
        try:
            data = yaml.safe_load(file)
            return data
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file {file_path}") from e


def distict_order_col_from_df(df: DataFrame, order_column: str) -> list:
    """
    Helper function to order a column by the order they appear in the data frame
    inputs:
        - df : DataFrame, The DataFrame containing the mapping configuration
        - order_column : str, The column to order the entities
    outputs:
        - DataFrame, The ordered DataFrame for the column
    """
    ordered_df = (
        df.withColumn("unique_id", monotonically_increasing_id())
        .groupBy(order_column)
        .agg(min("unique_id").alias("min_unique_id"))
        .orderBy("min_unique_id")
        .select(df[order_column])
    )

    ordered_list = [row[order_column] for row in ordered_df.collect()]

    return ordered_list


def add_generic_metadata(
    df: DataFrame,
    pk_name: str,
    md_source: str,
    source_name: str,
    source_metadata_columns_to_keep: List[str] = [],
    additional_metadata: List[str] = [],
) -> DataFrame:
    """
    Function that adds generic metadata to the dataframe
    inputs:
        - df : DataFrame, The DataFrame to add the metadata columns too
        - pk_name : str, The primary key name
        - md_source : str, The source of the metadata
        - source_name : str, The name of the source
    outputs:
        - DataFrame, The DataFrame with the metadata columns added
    """

    md_exprs = [
        "*",
        "CAST(null AS string) AS md_validation_status",
        f"uuid() AS md_{pk_name}_pk",
        f"to_json({md_source}) AS md_metadata",
        f"{source_name} AS md_source_name",
        "now() AS md_created_datetime",
        "now() AS md_updated_datetime",
        "CAST(null AS timestamp) AS md_deleted_datetime",
        "'I' AS md_change_status",
    ] + additional_metadata

    md_exprs_to_apply = md_exprs.copy()

    for expr in md_exprs:
        for col in source_metadata_columns_to_keep:
            if col in expr:
                md_exprs_to_apply.remove(expr)

    md_df = df.selectExpr(md_exprs_to_apply)

    return md_df


def rename_source_metadata_columns(
    df: DataFrame, source_metadata_columns_to_keep: List[str] = []
) -> DataFrame:
    """
    Function that renames the metadata columns in the dataframe
    inputs:
        - df : DataFrame, The DataFrame to rename the metadata columns
    outputs:
        - DataFrame, The DataFrame with the metadata columns renamed
    """

    columns = df.columns
    for col in columns:
        if col.startswith("md_") and (col not in source_metadata_columns_to_keep):
            df = df.withColumnRenamed(col, col + "_origin")

    return df


def drop_metadata_columns(df: DataFrame) -> DataFrame:
    """
    Function that drops the metadata columns in the dataframe
    inputs:
        - df : DataFrame, The DataFrame to rename the metadata columns
    outputs:
        - DataFrame, The DataFrame with the metadata columns removed
    """

    columns = df.columns
    cols_to_drop = []
    for col in columns:
        if col.startswith("md_"):
            cols_to_drop.append(col)

    df = df.drop(*cols_to_drop)
    return df


def get_uuid_temp_table_name() -> str:
    """
    Generate a unique temporary table name using UUID.
    Outputs:
        - str: A unique temporary table name.
    """
    return f"temp_table_{str(uuid.uuid4()).replace('-', '_')}"
