import pytest
from readers.IDataReader import IDataReader
from readers.DataReaderFactory import DataReaderFactory
from readers.FileStreamReader import FileStreamReader
from readers.APIReader import APIReader
from readers.APIReaderByID import APIReaderByID
from readers.TableStreamReader import TableStreamReader
from readers.TableBatchReader import TableBatchReader


@pytest.mark.parametrize(
    "entity_config, expected_result",
    [
        (
            {
                "entity_name": "test_entity",
                "source": {
                    "reader_type": "file_stream",
                    "format": "parquet",
                    "read_options": {"header": "true"},
                    "volume_path": "test_volume_path",
                },
            },
            FileStreamReader,
        ),
        (
            {
                "entity_name": "test_entity",
                "source": {
                    "reader_type": "api",
                    "url": "https://api.example.com/data",
                    "headers": {"Authorization": "Bearer token"},
                },
            },
            APIReader,
        ),
        (
            {
                "entity_name": "test_entity",
                "source": {
                    "reader_type": "api_by_id",
                    "url": "https://api.example.com/data",
                    "headers": {"Authorization": "Bearer token"},
                    "id_column": "id",
                },
            },
            APIReaderByID,
        ),
        (
            {
                "entity_name": "test_entity",
                "source": {
                    "reader_type": "table_stream",
                    "table_name": "schema.test_table",
                    "read_options": {"header": "true"},
                },
            },
            TableStreamReader,
        ),
        (
            {
                "entity_name": "test_entity",
                "source": {
                    "reader_type": "table_batch",
                    "table_name": "schema.test_table",
                    "read_options": {"header": "true"},
                },
            },
            TableBatchReader,
        ),
    ],
)
def test_DataReaderFactory(entity_config, expected_result, spark_mock, dbutils_mock):
    # get the data reader
    data_reader = DataReaderFactory.get_datareader(
        entity_config, spark_mock, dbutils_mock
    )
    # assert that the data reader is an instance of IDataReader
    assert isinstance(data_reader, IDataReader)
    # assert that the data reader is an instance of FileStreamReader
    assert isinstance(data_reader, expected_result)
