import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame
from core_functions.config_models import EntityConfig
from transformers.DefaultTransform import DefaultTransform


@pytest.mark.parametrize(
    "entity_config",
    [
        (
            {
                "entity_name": "test_entity",
                "transform": {
                    "transform_type": "default",
                },
            }
        ),
        (
            {
                "entity_name": "test_entity",
            }
        ),
    ],
)
def test_default_transform_init(entity_config, spark_mock, dbutils_mock):
    """
    Test that DefaultTransform constructor correctly initializes attributes.
    """

    source_df_mock = MagicMock()
    transformer = DefaultTransform(
        source_df_mock, entity_config, spark_mock, dbutils_mock
    )

    assert transformer.source_df == source_df_mock
    assert transformer.spark == spark_mock
    assert transformer.dbutils == dbutils_mock


@pytest.mark.parametrize(
    "entity_config",
    [
        (
            {
                "entity_name": "test_entity",
                "transform": {
                    "transform_type": "default",
                },
            }
        ),
        (
            {
                "entity_name": "test_entity",
            }
        ),
    ],
)
def test_default_transform_apply_transform(entity_config, spark_mock, dbutils_mock):
    """
    Test that apply_transform returns the initialized source_df when no arguments are given.
    """
    source_df_mock = MagicMock()
    
    entity_config = EntityConfig(**entity_config)

    transformer = DefaultTransform(
        source_df_mock, entity_config, spark_mock, dbutils_mock
    )

    result = transformer.apply_transform()

    assert isinstance(result, dict)
    assert entity_config.entity_name in result
    assert result[entity_config.entity_name] == source_df_mock
