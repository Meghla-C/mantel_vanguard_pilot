import pytest
import json
import re
import requests
from requests.models import Response
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from unittest.mock import patch, MagicMock
from readers.APIReader import APIReader
from core_functions.utils import get_current_catalog
from core_functions.config_models import EntityConfig


@patch("core_functions.utils.set_attributes")
@patch("readers.APIReader.APIReader._add_api_metadata")
@patch("readers.APIReader.APIReader.get_api_credentials")
@patch("readers.APIReader.APIReader.make_http_request")
def test_read_source_to_dataframe(
    mock_make_http_request,
    mock_get_api_credentials,
    mock__add_api_metadata,
    mock_set_attributes,
    spark_mock,
    dbutils_mock,
):

    # Define variable for testing
    entity_config = {
        "entity_name": "test_entity",
        "source": {
            "reader_type": "api",
            "host_name_secret_key": "entity-api-hostname",
            "path": "/rest/test",
            "username_secret_key": "entity-test-username",
            "password_secret_key": "entity-test-password",
        },
    }
    entity_config = EntityConfig(**entity_config)
    mock_get_api_credentials.return_value = {
        "host_endpoint": "http://mockhost",
        "api_url": "http://mockhost/rest/test",
        "username": "mock_user",
        "password": "mock_pass",
    }

    mock_make_http_request.return_value = ['{"key1": "value1"}', '{"key2": "value2"}']

    api_reader = APIReader(entity_config, spark_mock, dbutils_mock)
    api_reader.read_source_to_dataframe(entity_config, spark_mock, dbutils_mock)

    mock_set_attributes.assert_called_once()
    mock_get_api_credentials.assert_called_once()
    mock_make_http_request.assert_called_once_with(
        host_endpoint="http://mockhost",
        api_url="http://mockhost/rest/test",
        username="mock_user",
        password="mock_pass",
        entity_config=entity_config,
    )
    spark_mock.createDataFrame.assert_called_once_with(
        ['{"key1": "value1"}', '{"key2": "value2"}'], StringType()
    )
    mock__add_api_metadata.assert_called_once()
