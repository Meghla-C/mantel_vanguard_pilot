import pytest
import json
import re
import requests
import datetime
from typing import Dict, List, Optional
from requests.models import Response
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from unittest.mock import patch, MagicMock
from freezegun import freeze_time
from readers.AbstractAPIReader import AbstractAPIReader
from core_functions.utils import get_current_catalog
from core_functions.config_models import EntityConfig


class TestAbstractAPIReader(AbstractAPIReader):

    def read_source_to_dataframe(self, entity_config, spark, dbutils) -> DataFrame:
        return super().read_source_to_dataframe(entity_config, spark, dbutils)


def test__add_api_metadata_with_none(spark_mock, dbutils_mock):
    # Define variable for testing
    entity_config = {"entity_name": "test_entity"}

    api_reader = TestAbstractAPIReader(
        EntityConfig(**entity_config), spark_mock, dbutils_mock
    )

    # Mock the dataframe and its methods
    mock_dataframe = MagicMock()

    with pytest.raises(AttributeError):
        api_reader._add_api_metadata(mock_dataframe)


def test_get_api_credentials_failure(spark_mock, dbutils_mock):
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

    # Setup the dbutils_mock
    dbutils_mock.secrets.get.side_effect = lambda scope, key: {
        "wrong-api-hostname": "http://mockhost",
        "wrong-test-username": "mock_user",
        "wrong-test-password": "mock_pass",
    }[key]

    api_reader = TestAbstractAPIReader(entity_config, spark_mock, dbutils_mock)

    with pytest.raises(KeyError):
        api_reader.get_api_credentials(entity_config, spark_mock)


def test_get_api_credentials_success(spark_mock, dbutils_mock):

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

    # Setup the dbutils_mock
    dbutils_mock.secrets.get.side_effect = lambda scope, key: {
        "entity-api-hostname": "http://mockhost",
        "entity-test-username": "mock_user",
        "entity-test-password": "mock_pass",
    }[key]

    api_reader = TestAbstractAPIReader(entity_config, spark_mock, dbutils_mock)

    response = api_reader.get_api_credentials(entity_config, spark_mock)

    assert response == {
        "host_endpoint": "http://mockhost",
        "api_url": "http://mockhost/rest/test",
        "username": "mock_user",
        "password": "mock_pass",
    }


@patch("requests.get")
def test_make_http_request_failure(mock_requests_get, spark_mock, dbutils_mock):

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

    api_metadata = {
        "host_endpoint": "http://mockhost",
        "api_url": "http://mockhost/rest/test",
        "username": "mock_user",
        "password": "mock_pass",
    }

    # Mock the API response
    mock_response = MagicMock(spec=Response)
    mock_response.status_code = 500
    mock_requests_get.return_value = mock_response

    api_reader = TestAbstractAPIReader(entity_config, spark_mock, dbutils_mock)

    with pytest.raises(
        Exception, match="Failed to fetch data from API. Status code: 500"
    ):
        api_reader.make_http_request(
            host_endpoint=api_metadata["host_endpoint"],
            api_url=api_metadata["api_url"],
            username=api_metadata["username"],
            password=api_metadata["password"],
            entity_config=entity_config,
        )


@patch("requests.get")
@freeze_time("2024-09-04T02:15:15Z")
@pytest.mark.parametrize(
    "mock_json, return_json, mock_data_volume",
    [
        (
            [{"key1": "value1"}, {"key2": "value2"}],
            ['{"key1": "value1"}', '{"key2": "value2"}'],
            "2",
        ),
        ({"key1": "value1"}, '{"key1": "value1"}', "1"),
        ([], [], "0"),
    ],
)
def test_make_http_request_success(
    mock_requests_get,
    mock_json,
    return_json,
    mock_data_volume,
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

    api_metadata = {
        "host_endpoint": "http://mockhost",
        "api_url": "http://mockhost/rest/test",
        "username": "mock_user",
        "password": "mock_pass",
    }

    # Mock the API response
    mock_response = MagicMock(spec=Response)
    mock_response.status_code = 200
    mock_response.headers = requests.structures.CaseInsensitiveDict(
        {"Content-Type": "application/json", "Date": "Sat, 31 Aug 2024 00:00:00 GMT"}
    )

    mock_response.json.return_value = mock_json
    mock_requests_get.return_value = mock_response

    api_reader = TestAbstractAPIReader(entity_config, spark_mock, dbutils_mock)

    response = api_reader.make_http_request(
        host_endpoint=api_metadata["host_endpoint"],
        api_url=api_metadata["api_url"],
        username=api_metadata["username"],
        password=api_metadata["password"],
        entity_config=entity_config,
    )

    mock_requests_get.assert_called_once_with(
        "http://mockhost/rest/test",
        auth=("mock_user", "mock_pass"),
        headers={"Accept": "application/json"},
        timeout=(15, 180),
    )

    assert api_reader._header_metadata == {
        "Content-Type": "application/json",
        "Date": "Sat, 31 Aug 2024 00:00:00 GMT",
        "api_response_time": "2024-09-04T02:15:15Z",
        "api_endpoint": "http://mockhost",
        "api_name": "/rest/test",
        "api_response_data_volume": mock_data_volume,
    }

    assert response == return_json


@pytest.mark.parametrize(
    "mock_url_list, http_return, test_return",
    [
        (
            [
                "http://mockhost/rest/test/id1",
                "http://mockhost/rest/test/id2",
                "http://mockhost/rest/test/id3",
            ],
            ['{"key1": "value1"}', '{"key2": "value2"}', '{"key3": "value3"}'],
            ['{"key1": "value1"}', '{"key2": "value2"}', '{"key3": "value3"}'],
        ),
        (
            ["http://mockhost/rest/test/id1"],
            ['{"key1": "value1"}'],
            ['{"key1": "value1"}'],
        ),
        ([], [], []),
        (
            ["http://mockhost/rest/test/id1", "http://mockhost/rest/test/id2"],
            [
                Exception("Failed to fetch data from API. Status code: 500"),
                '{"key1": "value1"}',
            ],
            [
                {
                    "error": "'http://mockhost/rest/test/id1' generated an exception: Failed to fetch data from API. Status code: 500"
                },
                '{"key1": "value1"}',
            ],
        ),
    ],
)
def test_concurrent_http_request(
    mock_url_list, http_return, test_return, spark_mock, dbutils_mock
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

    api_reader = TestAbstractAPIReader(entity_config, spark_mock, dbutils_mock)
    api_reader.make_http_request = MagicMock(side_effect=http_return)

    call_requests = api_reader.make_concurrent_http_requests(
        url_list=mock_url_list,
        host_endpoint="dummy",
        username="dummy",
        password="dummy",
        entity_config=entity_config,
    )

    # completion can be at diff times
    for val in test_return:
        if val not in call_requests:
            raise Exception(f"Missing value {val}")
