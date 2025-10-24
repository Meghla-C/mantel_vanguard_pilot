import pytest
from pyspark.sql import DataFrame
from unittest.mock import patch, MagicMock
from writers.MergeBatchWriter import MergeBatchWriter
from pyspark.sql import Row
from freezegun import freeze_time
from helpers import assert_dataframes_are_equal
from core_functions.config_models import EntityConfig


def test_invalid_table_name(spark_mock, dbutils_mock):
    entity_config = {
        "entity_name": "test_entity",
        "target": {"writer_type": "merge_batch", "table_name": "test_schema"},
    }
    with pytest.raises(
        ValueError,
        match="target_table_name with its schema name is required",
    ):
        abw = MergeBatchWriter(EntityConfig(**entity_config), spark_mock, dbutils_mock)
        abw.write_to_target_table(None)


@freeze_time("2025-10-01T00:00:00Z")
@pytest.mark.parametrize(
    "source_df, updates_df, result_table, entity_config, failure",
    [
        (  # ingle col join and update
            # source df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=30,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Bob",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # updates df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # result table
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            {
                "entity_name": "test_entity1",
                "target": {
                    "writer_type": "merge_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "join_columns": ["id"],
                    "exclude_columns": ["md_updated_datetime", "md_change_status"],
                },
            },
            False,
        ),
        (  # entity name constains non alpha numeric symbols
            # source df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=30,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Bob",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # updates df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # result table
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            {
                "entity_name": "test.entity_name2",
                "target": {
                    "writer_type": "merge_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "join_columns": ["id"],
                    "exclude_columns": ["md_updated_datetime", "md_change_status"],
                },
            },
            False,
        ),
        (  # multi col join
            # source df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=30,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Bob",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # updates df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # result table
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Bob",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            {
                "entity_name": "test_entity3",
                "target": {
                    "writer_type": "merge_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "join_columns": ["id", "name"],
                    "exclude_columns": ["md_updated_datetime", "md_change_status"],
                },
            },
            False,
        ),
        (  # no join columns
            # source df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=30,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Bob",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # updates df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # result table
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Bob",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            {
                "entity_name": "test_entity4",
                "target": {
                    "writer_type": "merge_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "join_columns": [],
                    "exclude_columns": ["md_updated_datetime", "md_change_status"],
                },
            },
            True,
        ),
        (  # no update, insert only
            # source df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=30,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Bob",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # updates df
            [
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                )
            ],
            # result table
            [
                Row(
                    id=1,
                    name="Alice",
                    age=30,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Bob",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            {
                "entity_name": "test_entity5",
                "target": {
                    "writer_type": "merge_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "join_columns": ["id"],
                    "exclude_columns": ["md_updated_datetime", "md_change_status"],
                },
            },
            False,
        ),
        (  # update only, no insert
            # source df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=30,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Bob",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # updates df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # result table
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            {
                "entity_name": "test_entity6",
                "target": {
                    "writer_type": "merge_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "join_columns": ["id"],
                    "exclude_columns": ["md_updated_datetime", "md_change_status"],
                },
            },
            False,
        ),
        (  # update only even if insert exists
            # source df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=30,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Bob",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # updates df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # result table
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            {
                "entity_name": "test_entity7",
                "target": {
                    "writer_type": "merge_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "join_columns": ["id"],
                    "exclude_columns": ["md_updated_datetime", "md_change_status"],
                    "update_only": True,
                },
            },
            False,
        ),
        (  # merge into table with custom condition
            # source df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=30,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Bob",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # updates df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # result table
            [
                Row(
                    id=1,
                    name="Alice",
                    age=30,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            {
                "entity_name": "test_entity8",
                "target": {
                    "writer_type": "merge_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "join_columns": ["id"],
                    "exclude_columns": ["md_updated_datetime", "md_change_status"],
                    "custom_update_condition": "target.name != 'Alice'",
                },
            },
            False,
        ),
        (  # entity name constains non alpha numeric symbols
            # source df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=30,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Bob",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # updates df
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            # result table
            [
                Row(
                    id=1,
                    name="Alice",
                    age=27,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=2,
                    name="Robert",
                    age=25,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="U",
                    md_validation_status="None",
                ),
                Row(
                    id=3,
                    name="Charlie",
                    age=35,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
                Row(
                    id=4,
                    name="Daniel",
                    age=40,
                    md_updated_datetime="2025-10-01T00:00:00Z",
                    md_change_status="I",
                    md_validation_status="None",
                ),
            ],
            {
                "entity_name": "test.entity_name9",
                "target": {
                    "writer_type": "merge_batch",
                    "table_name": "meta.dummy_table_for_unit_test",
                    "join_columns": ["id"],
                    "exclude_columns": ["md_updated_datetime", "md_change_status"],
                },
            },
            False,
        ),
    ],
)
def test_merge_batch_writer(
    source_df,
    updates_df,
    result_table,
    entity_config,
    failure,
    databricks_spark,
    dbutils_mock,
):

    spark = databricks_spark

    source_df = spark.createDataFrame(source_df)
    updates_df = spark.createDataFrame(updates_df)
    result_table = spark.createDataFrame(result_table)

    spark.sql("DROP TABLE IF EXISTS meta.dummy_table_for_unit_test")
    source_df.write.mode("overwrite").saveAsTable("meta.dummy_table_for_unit_test")

    if failure:
        with pytest.raises(
            ValueError,
            match="join_columns with list of column names to join on is required in entity_config",
        ):
            abw = MergeBatchWriter(EntityConfig(**entity_config), spark, dbutils_mock)
        return

    abw = MergeBatchWriter(EntityConfig(**entity_config), spark, dbutils_mock)
    abw.write_to_target_table(updates_df)

    result = spark.sql("SELECT * FROM meta.dummy_table_for_unit_test ORDER BY id")
    result_no_update = result.drop("md_updated_datetime")
    result_table_no_update = result_table.drop("md_updated_datetime")

    assert_dataframes_are_equal(result_no_update, result_table_no_update)

    spark.sql("DROP TABLE IF EXISTS meta.dummy_table_for_unit_test")
