import pytest
from unittest.mock import MagicMock, patch, Mock
import transformers.JsonTransform as JsonTransform
from decimal import Decimal
from helpers import assert_dataframes_are_equal
import pandas as pd
from io import StringIO
from core_functions.config_models import EntityConfig


# generate unit tests using pytest for /transformers/JsonTransform for apply_transform, mocking _create_entity_transform mocking spark.read mocking os.path.join mocking utils.distict_order_col_from_df mocking utils.project_path
@patch("transformers.JsonTransform.os.path.join", return_value="file_path")
@patch("transformers.JsonTransform.utils.project_path", return_value="project_path")
@patch(
    "transformers.JsonTransform.utils.distict_order_col_from_df",
    return_value=["order_col"],
)
@patch("transformers.JsonTransform.JsonTransform._create_entity_transform")
@patch("transformers.JsonTransform.JsonTransform._read_mapping_config")
def test_apply_transform(
    mock_read_mapping_config,
    mock_create_entity_transform,
    mock_distict_order_col_from_df,
    mock_project_path,
    mock_os_path_join,
    databricks_spark,
    dbutils_mock,
):
    spark = databricks_spark

    source_df = spark.createDataFrame(
        [
            (
                "2024-01-31T23:59:28Z",
                294,
                0.6,
                19906432310,
                -37.81596,
                144.84055,
                "2024-01-31T23:59:28Z",
                19,
                12,
                "POSITION",
            )
        ],
        "dateTime string, direction int, hdop double, number long, latitude string, longitude string, receiptDateTime string, satelliteCount int, speed int, type string",
    )
    expected_list = [
        "dateTime as dateTime",
        "CAST(direction AS int) as direction",
        "CAST(hdop AS double) as hdop",
        "CAST(number AS long) as number",
        "CAST(latitude AS decimal(10,6)) as latitude",
        "CAST(longitude AS decimal(10,6)) as longitude",
        "receiptDateTime as receiptDateTime",
        "CAST(satelliteCount AS int) as satelliteCount",
        "CAST(speed AS int) as speed",
        "type as type",
    ]

    expected_df = spark.createDataFrame(
        [
            (
                "2024-01-31T23:59:28Z",
                294,
                0.6,
                19906432310,
                Decimal("-37.81596"),
                Decimal("144.84055"),
                "2024-01-31T23:59:28Z",
                19,
                12,
                "POSITION",
            )
        ],
        "dateTime string, direction int, hdop double, number long, latitude decimal(10,6), longitude decimal(10,6), receiptDateTime string, satelliteCount int, speed int, type string",
    )

    entity_config = {
        "transform": {"transform_type": "json", "mapping_config": "mapping_config"}
    }
    mock_create_entity_transform.return_value = expected_list
    mock_read_mapping_config.return_value = spark.createDataFrame(
        [(None, "order_col")], "source_df string, target_entity string"
    )

    json_transform = JsonTransform.JsonTransform(
        source_df, EntityConfig(**entity_config), spark, dbutils_mock
    )
    output = json_transform.apply_transform()
    assert list(output.keys())[0] == "order_col"
    assert_dataframes_are_equal(output["order_col"], expected_df)


@pytest.mark.parametrize(
    "entity, expected_result",
    [
        (
            "device_record",
            [
                "deviceRecords:device.id as device_id",
                "SPLIT(file_name,  '_')[1] as provider_id",
                "UUID() as record_id",
                "explode_outer(from_json(deviceRecords:records, 'array<string>'))  as records",
            ],
        ),
        (
            "records",
            [
                "device_id as device_id",
                "provider_id as provider_id",
                "record_id as record_id",
                "CAST(records:dateTime AS timestamp) as dateTime",
                "CAST(records:position.latitude AS decimal(10,6)) as latitude",
                "CAST(records:position.longitude AS decimal(10,6)) as longitude",
                "records:connectedDeviceRecord.msuRecords as msuRecords_unexploded",
            ],
        ),
        (
            "msuRecords_exploded",
            [
                "record_id as record_id",
                "explode(from_json(msuRecords_unexploded, 'array<string>'))  as msuRecords",
            ],
        ),
        (
            "msuRecords",
            [
                "record_id as record_id",
                "msuRecords:liftAxle as liftAxle",
                "CAST(msuRecords:mass AS int) as mass",
                "msuRecords:msuId as msuId",
                "CAST(msuRecords:msuSequence AS int) as msuSequence",
            ],
        ),
    ],
)
def test_create_entity_transform(
    entity, expected_result, databricks_spark, dbutils_mock
):
    spark = databricks_spark

    source_df = MagicMock()
    entity_config_mock = {
        "transform": {"transform_type": "json", "mapping_config": "mapping_config"}
    }
    mapping_csv = """source_df,source_column,transformation,target_entity,target_column,target_datatype,operation,comments
,deviceRecords:device.id,,device_record,device_id,string,,
,,"SPLIT(file_name,  '_')[1]",device_record,provider_id,string,,
,UUID(),,device_record,record_id,string,SK,
,deviceRecords:records,,device_record,records,string,Explode_outer,
device_record,device_id,,records,device_id,string,,
device_record,provider_id,,records,provider_id,string,,
device_record,record_id,,records,record_id,string,PK,
device_record,records:dateTime,,records,dateTime,timestamp,,
device_record,records:position.latitude,,records,latitude,"decimal(10,6)",,
device_record,records:position.longitude,,records,longitude,"decimal(10,6)",,
device_record,records:connectedDeviceRecord.msuRecords,,records,msuRecords_unexploded,string,,
records,record_id,,msuRecords_exploded,record_id,string,FK,
records,msuRecords_unexploded,,msuRecords_exploded,msuRecords,string,Explode,
msuRecords_exploded,record_id,,msuRecords,record_id,string,FK,
msuRecords_exploded,msuRecords:liftAxle,,msuRecords,liftAxle,string,,
msuRecords_exploded,msuRecords:mass,,msuRecords,mass,int,,
msuRecords_exploded,msuRecords:msuId,,msuRecords,msuId,string,,
msuRecords_exploded,msuRecords:msuSequence,,msuRecords,msuSequence,int,,"""

    # Convert CSV string to file-like object
    csv_file_like = StringIO(mapping_csv)
    pandas_df = pd.read_csv(csv_file_like)

    mapping_df = spark.createDataFrame(pandas_df)

    json_transform = JsonTransform.JsonTransform(
        source_df, EntityConfig(**entity_config_mock), spark, dbutils_mock
    )
    output = json_transform._create_entity_transform(
        mapping_df=mapping_df, target_entity=entity
    )

    assert output == expected_result
