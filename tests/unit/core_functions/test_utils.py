import os
import pytest
import uuid
import shutil
import re
from unittest.mock import MagicMock, patch, Mock
import core_functions.utils as utils
from typing import Dict
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import col, lit, to_json, current_timestamp, from_json


# test class attributes are set correctly
class DummyClass:
    def __init__(self, attr1=None, attr2=None, attr3=None):
        self.attr1 = attr1
        self.attr2 = attr2
        self.attr3 = attr3


def test_set_attributes():
    """
    Test that set_attributes correctly updates class attributes.
    Includes a test case for updating with None values.
    """
    test_class = DummyClass(1, 2, 3)
    utils.set_attributes(test_class, attr1="a", attr2="b", attr3="c")
    assert (
        test_class.attr1 == "a" and test_class.attr2 == "b" and test_class.attr3 == "c"
    )

    # Test with None values - should not update
    utils.set_attributes(test_class, attr1=None, attr4="d")
    assert (
        test_class.attr1 == "a" and test_class.attr2 == "b" and test_class.attr3 == "c"
    )


catalog_name = "test_catalog"
schema_volume_path = "test_path"


def test_get_current_catalog(spark_mock: MagicMock):
    """
    Test that get_current_catalog returns the current catalog name from Spark.
    """
    data = [(catalog_name,)]

    mock_sql_result = MagicMock()
    mock_sql_result.collect.return_value = data
    spark_mock.sql.return_value = mock_sql_result

    assert utils.get_current_catalog(spark_mock) == catalog_name


@patch("os.path.exists", return_value=True)
@patch("core_functions.utils.get_current_catalog", return_value=catalog_name)
def test_get_full_volume_path_path_exists(
    mock_get_current_catalog, mock_os_path_exists, spark_mock: MagicMock
):
    """
    Test that get_full_volume_path returns the correct path when the directory exists.
    """
    assert (
        utils.get_full_volume_path(schema_volume_path, spark_mock)
        == f"/Volumes/{catalog_name}/{schema_volume_path}"
    )

    assert mock_os_path_exists.call_count == 1
    call_args = mock_os_path_exists.call_args_list[0][0]
    assert call_args[0] == f"/Volumes/{catalog_name}"


@patch("os.path.exists", return_value=False)
@patch("core_functions.utils.get_current_catalog", return_value=catalog_name)
def test_get_full_volume_path_path_not_exists(
    mock_get_current_catalog, mock_os_path_exists, spark_mock: MagicMock
):
    """
    Test that get_full_volume_path raises FileNotFoundError when the directory does not exist.
    """
    non_existent_path = "not_exists_path"
    with pytest.raises(
        FileNotFoundError,
        match=f"Volume path '{non_existent_path}' does not exist in the environment '{catalog_name}'.",
    ):
        utils.get_full_volume_path(non_existent_path, spark_mock)

    assert mock_os_path_exists.call_count == 1
    call_args = mock_os_path_exists.call_args_list[0][0]
    assert call_args[0] == f"/Volumes/{catalog_name}"


@patch("core_functions.utils.get_full_volume_path", return_value=schema_volume_path)
def test_get_source_volume_path(mock_get_full_volume_path, spark_mock: MagicMock):
    """
    Test that get_source_volume_path calls get_full_volume_path with the correct arguments.
    """

    class MockEntityConfig:
        def __init__(self, volume_path):
            class Source:
                def __init__(self, volume_path):
                    self.volume_path = volume_path

            self.source = Source(volume_path)

    entity_config = MockEntityConfig(schema_volume_path)

    assert utils.get_source_volume_path(entity_config, spark_mock) == schema_volume_path
    mock_get_full_volume_path.assert_called_once_with(schema_volume_path, spark_mock)


@patch("os.path.exists", return_value=True)
@patch("core_functions.utils.get_current_catalog", return_value=catalog_name)
def test_get_checkpoint_location(
    mock_get_current_catalog, mock_os_path_exists, spark_mock: MagicMock
):
    """
    Test that get_checkpoint_location returns the correct checkpoint path.
    """

    class MockEntityConfig:
        def __init__(self, entity_name, table_name):
            self.entity_name = entity_name

            class Target:

                def __init__(self, table_name):
                    self.table_name = table_name

            self.target = Target(table_name)

    entity_config = MockEntityConfig(
        entity_name="test_entity", table_name="test_schema.test"
    )

    assert (
        utils.get_checkpoint_location(entity_config, spark_mock)
        == f"/Volumes/{catalog_name}/test_schema/checkpoint/test_entity"
    )

    assert mock_os_path_exists.call_count == 1
    call_args = mock_os_path_exists.call_args_list[0][0]
    assert call_args[0] == f"/Volumes/{catalog_name}/test_schema/checkpoint"


@patch("os.path.exists", return_value=True)
@patch("core_functions.utils.get_current_catalog", return_value=catalog_name)
def test_get_schema_location_no_schema_location(
    mock_get_current_catalog, mock_os_path_exists, spark_mock: MagicMock
):
    """
    Test that get_schema_location returns the default path when no schema_location is provided.
    """

    class MockEntityConfig:
        def __init__(self, entity_name, table_name, schema_location=None):

            self.entity_name = entity_name

            class Source:

                def __init__(self, schema_location):
                    self.schema_location = schema_location

            class Target:

                def __init__(self, table_name):
                    self.table_name = table_name

            self.source = Source(schema_location)
            self.target = Target(table_name)

    entity_config = MockEntityConfig(
        entity_name="test_entity", table_name="test_schema.test", schema_location=""
    )

    assert (
        utils.get_schema_location(entity_config, spark_mock)
        == f"/Volumes/{catalog_name}/test_schema/schema/test_entity"
    )

    assert mock_os_path_exists.call_count == 1
    call_args = mock_os_path_exists.call_args_list[0][0]
    assert call_args[0] == f"/Volumes/{catalog_name}/test_schema/schema"


@patch("os.path.exists", return_value=True)
@patch("core_functions.utils.get_current_catalog", return_value=catalog_name)
def test_get_schema_location_with_schema_location(
    mock_get_current_catalog, mock_os_path_exists, spark_mock: MagicMock
):
    """
    Test that get_schema_location returns the provided schema_location when it exists.
    """

    class MockEntityConfig:
        def __init__(self, entity_name, table_name, schema_location=None):
            self.entity_name = entity_name

            class Source:
                def __init__(self, schema_location):
                    self.schema_location = schema_location

            class Target:
                def __init__(self, table_name):
                    self.table_name = table_name

            self.source = Source(schema_location)
            self.target = Target(table_name)

    schema_location = "/test_schema_location"
    entity_config = MockEntityConfig(
        entity_name="test_entity",
        table_name="test_schema.test",
        schema_location=schema_location,
    )

    assert utils.get_schema_location(entity_config, spark_mock) == schema_location

    mock_os_path_exists.assert_not_called()


def test_read_yaml():
    """
    Test that read_yaml correctly reads a valid YAML file and raises exceptions for invalid or non-existent files.
    """

    valid_yaml_content = """
dummy1:
    arg1: value1
    arg2: value2
dummy2:
    arg1: value1
    arg2: value2
dummy3:
    arg1: "{value}"
    arg2: value2
"""

    invalid_yaml_content_parse_error = """
key: [value1, value2
another_key: value3 # Missing closing bracket above
"""

    test_dir = "tests/resources"
    valid_yaml_file = os.path.join(test_dir, "valid_file.yml")
    invalid_yaml_file_parse_error = os.path.join(test_dir, "invalid_file_parse.yml")
    nonexistent_yaml_file = "/path/to/nonexistent.yaml"

    os.makedirs(test_dir, exist_ok=True)

    try:
        with open(valid_yaml_file, "w") as f:
            f.write(valid_yaml_content)
        with open(invalid_yaml_file_parse_error, "w") as f:
            f.write(invalid_yaml_content_parse_error)

        # Test case 1: Valid YAML file
        expected_output = {
            "dummy1": {"arg1": "value1", "arg2": "value2"},
            "dummy2": {"arg1": "value1", "arg2": "value2"},
            "dummy3": {"arg1": "{value}", "arg2": "value2"},
        }
        assert utils.read_yaml(valid_yaml_file) == expected_output

        # Test case 2: Invalid YAML file (parse error)
        with pytest.raises(
            ValueError,
            match=f"Error parsing YAML file {re.escape(invalid_yaml_file_parse_error)}",
        ):
            utils.read_yaml(invalid_yaml_file_parse_error)

        # Test case 3: Non-existent YAML file
        with pytest.raises(
            FileNotFoundError,
            match=f"The configuration file '{re.escape(nonexistent_yaml_file)}' was not found.",
        ):
            utils.read_yaml(nonexistent_yaml_file)

    finally:
        # Clean up the dummy files and directory
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)


@patch("core_functions.utils.os")
def test_project_path(os_path_mock):
    """
    Test that project_path returns the correct project path and raises AttributeError for invalid paths.
    """
    # valid path with "src"
    os_path_mock.path.abspath.return_value = "/Workspace/files/src/some/internal/path"
    assert utils.project_path() == "/Workspace/files/"

    # valid path ending with "src"
    os_path_mock.path.abspath.return_value = "/src/"
    assert utils.project_path() == "/"

    # invalid path without "src"
    os_path_mock.path.abspath.return_value = "root/Workspace/test/notebook/location"
    with pytest.raises(
        AttributeError, match="Initialized in an incorrect directory, no src in"
    ):
        utils.project_path()

    # None path
    os_path_mock.path.abspath.return_value = None
    with pytest.raises(
        AttributeError, match="Initialized in an incorrect directory, no src in"
    ):
        utils.project_path()


def test_distict_order_col_from_df_return_value(databricks_spark):
    """
    Test that distict_order_col_from_df returns distinct column values in their order of appearance.
    """
    spark = databricks_spark
    source_df = spark.createDataFrame(
        [("a",), ("a",), ("b",), ("b",), ("b",), ("c",), ("c",)], "col string"
    )
    expected_output = ["a", "b", "c"]

    assert utils.distict_order_col_from_df(source_df, "col") == expected_output


@pytest.mark.parametrize(
    "source_metadata_columns_to_keep, additional_metadata, expected_additional_cols",
    [
        ([], [], []),  # No columns to keep, no additional metadata
        (["md_metadata"], [], []),  # Keep md_metadata, no additional metadata
        (
            [],
            ["'default_value' AS extra_col"],
            ["extra_col"],
        ),  # No columns to keep, one additional literal column
        (
            ["md_metadata", "md_source_name"],
            ["'val1' AS extra_col1", "'val2' AS extra_col2"],
            ["extra_col1", "extra_col2"],
        ),  # Keep some, add some literals
        (
            [],
            [
                "to_json(map('key', 'val')) AS json_col",
                "current_timestamp() AS process_ts",
            ],
            ["json_col", "process_ts"],
        ),  # Add expression-based columns
    ],
)
def test_add_generic_metadata(
    databricks_spark,
    source_metadata_columns_to_keep,
    additional_metadata,
    expected_additional_cols,
):
    """
    Test that add_generic_metadata adds the correct metadata columns, handles column keeping,
    and adds additional columns from expressions.
    """

    schema = StructType([StructField("value", StringType(), True)])
    mock_data = [("value1",)]
    mock_df = databricks_spark.createDataFrame(mock_data, schema)

    pk_name = "test_entity"
    md_source_expr = "map('api_key', 'api_value')"
    source_name_expr = "'my_source_name'"

    result_df = utils.add_generic_metadata(
        mock_df,
        pk_name,
        md_source_expr,
        source_name_expr,
        source_metadata_columns_to_keep,
        additional_metadata,
    )

    # Define expected base metadata columns and their generated names
    expected_base_metadata_cols = [
        "md_validation_status",
        f"md_{pk_name}_pk",
        "md_metadata",
        "md_source_name",
        "md_created_datetime",
        "md_updated_datetime",
        "md_deleted_datetime",
        "md_change_status",
    ]

    expected_final_cols = [
        column
        for column in expected_base_metadata_cols
        if column not in source_metadata_columns_to_keep
    ] + expected_additional_cols
    expected_final_cols.append("value")

    actual_cols = set(result_df.columns)
    expected_cols_set = set(expected_final_cols)

    assert expected_cols_set.issubset(actual_cols)

    for col_name in source_metadata_columns_to_keep:
        if (
            col_name in expected_base_metadata_cols
            and col_name not in expected_additional_cols
        ):
            assert col_name not in actual_cols

    result_row = result_df.collect()[0]
    assert result_row["value"] == "value1"

    if "md_validation_status" in actual_cols:
        assert result_row["md_validation_status"] is None
    if "md_metadata" in actual_cols:
        assert result_row["md_metadata"] == '{"api_key":"api_value"}'
    if "md_source_name" in actual_cols:
        assert result_row["md_source_name"] == "my_source_name"
    if f"md_{pk_name}_pk" in actual_cols:
        assert (
            isinstance(result_row[f"md_{pk_name}_pk"], str)
            and len(result_row[f"md_{pk_name}_pk"]) > 0
        )
    if "md_deleted_datetime" in actual_cols:
        assert result_row["md_deleted_datetime"] is None
    if "md_change_status" in actual_cols:
        assert result_row["md_change_status"] == "I"
    if "extra_col" in actual_cols:
        assert result_row["extra_col"] == "default_value"
    if "extra_col1" in actual_cols:
        assert result_row["extra_col1"] == "val1"
    if "extra_col2" in actual_cols:
        assert result_row["extra_col2"] == "val2"
    if "json_col" in actual_cols:
        assert result_row["json_col"] == '{"key":"val"}'
    if "process_ts" in actual_cols:
        assert isinstance(
            result_row["process_ts"],
            type(databricks_spark.sql("SELECT current_timestamp()").collect()[0][0]),
        )


@pytest.mark.parametrize(
    "source_metadata_columns_to_keep",
    [
        ([]),  # No columns to keep
        (["md_metadata"]),  # Keep md_metadata
        (["md_validation_status", "md_source_name"]),  # Keep multiple columns
        (
            ["non_existent_col"]
        ),  # Keep a column that doesn't exist (should not cause error)
    ],
)
def test_rename_source_metadata_columns(
    databricks_spark, source_metadata_columns_to_keep
):
    """
    Test that rename_source_metadata_columns renames md_ columns correctly,
    respecting the source_metadata_columns_to_keep list.
    """
    # Create a DataFrame with a mix of md_ columns and other columns
    schema = StructType(
        [
            StructField("value", StringType(), True),
            StructField("md_validation_status", StringType(), True),
            StructField("md_test_entity_pk", StringType(), True),
            StructField("md_metadata", StringType(), True),
            StructField("md_source_name", StringType(), True),
            StructField("other_col", StringType(), True),
            StructField(
                "md_created_datetime", StringType(), True
            ),  # Add another md_ col
        ]
    )
    mock_data = [("data1", "valid", "pk1", "{}", "sourceA", "extra", "ts1")]
    mock_df = databricks_spark.createDataFrame(mock_data, schema)

    actual_md_cols = [column for column in mock_df.columns if column.startswith("md_")]

    result_df = utils.rename_source_metadata_columns(
        mock_df, source_metadata_columns_to_keep
    )

    # Check the renaming/keeping logic for each actual md_ column
    for col_name in actual_md_cols:
        if col_name in source_metadata_columns_to_keep:
            # If the column is in the keep list, it should NOT be renamed
            assert col_name in result_df.columns
            assert f"{col_name}_origin" not in result_df.columns
        else:
            # If the column is NOT in the keep list, it SHOULD be renamed
            assert f"{col_name}_origin" in result_df.columns
            assert col_name not in result_df.columns  # Original column should be gone

    # Check that non-md_ columns are not renamed
    assert "other_col" in result_df.columns
    assert "value" in result_df.columns


def test_drop_metadata_columns(databricks_spark):
    """
    Test that drop_metadata_columns correctly drops all columns starting with 'md_'.
    """
    # Create a DataFrame with a mix of md_ columns and other columns
    schema = StructType(
        [
            StructField("value", StringType(), True),
            StructField("md_validation_status", StringType(), True),
            StructField("md_test_entity_pk", StringType(), True),
            StructField("other_col1", StringType(), True),
            StructField("md_metadata", StringType(), True),
            StructField("other_col2", StringType(), True),
        ]
    )
    mock_data = [("data1", "valid", "pk1", "extra1", "{}", "extra2")]
    mock_df = databricks_spark.createDataFrame(mock_data, schema)

    result_df = utils.drop_metadata_columns(mock_df)

    # Check that all md_ columns are dropped
    for col_name in mock_df.columns:
        if col_name.startswith("md_"):
            assert col_name not in result_df.columns

    # Check that non-md_ columns are kept
    assert "value" in result_df.columns
    assert "other_col1" in result_df.columns
    assert "other_col2" in result_df.columns


@patch("core_functions.utils.uuid")
def test_get_uuid_temp_table_name(mock_uuid):
    """
    Test that get_uuid_temp_table_name generates a unique temporary table name with underscores.
    """
    # Mock uuid.uuid4() to return a consistent value for testing structure
    mock_uuid_value = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")
    mock_uuid.uuid4.return_value = mock_uuid_value

    temp_table_name = utils.get_uuid_temp_table_name()

    # Assert the format is correct
    assert temp_table_name.startswith("temp_table_")
    # Check that hyphens are replaced by underscores
    assert temp_table_name == f"temp_table_{str(mock_uuid_value).replace('-', '_')}"

    # Test that uuid.uuid4 is called
    mock_uuid.uuid4.assert_called_once()

    # Test uniqueness across calls by changing the return value of the mock
    mock_uuid_value_2 = uuid.UUID("00000000-0000-0000-0000-000000000001")
    mock_uuid.uuid4.return_value = mock_uuid_value_2
    temp_table_name_2 = utils.get_uuid_temp_table_name()
    assert temp_table_name_2 != temp_table_name
    assert temp_table_name_2 == f"temp_table_{str(mock_uuid_value_2).replace('-', '_')}"
    assert mock_uuid.uuid4.call_count == 2  # Ensure it was called again
