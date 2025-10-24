import pytest
from unittest.mock import MagicMock
from transformers.IDataTransform import IDataTransform
from transformers.JsonTransform import JsonTransform
from transformers.DataTransformFactory import DataTransformFactory
from transformers.DefaultTransform import DefaultTransform


@pytest.mark.parametrize(
    "entity_config, expected_result",
    [
        (
            {
                "entity_name": "test_entity",
                "transform": {
                    "transform_type": "json",
                    "mapping_config": "mapping_config",
                },
            },
            JsonTransform,
        ),
        (
            {
                "entity_name": "test_entity",
            },
            DefaultTransform,
        ),
    ],
)
def test_DataTransformFactory(entity_config, expected_result, spark_mock, dbutils_mock):

    mock_source_df = MagicMock()

    # get the data transformer
    data_transformer = DataTransformFactory.get_datatransform(
        mock_source_df, entity_config, spark_mock, dbutils_mock
    )
    # assert that the data transformer is an instance of IDataTransform
    assert isinstance(data_transformer, IDataTransform)
    # assert that the data transformer is an isstance of the expected result
    assert isinstance(data_transformer, expected_result)
