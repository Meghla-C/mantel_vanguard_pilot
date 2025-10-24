import pytest
from writers.IDataWriter import IDataWriter
from writers.AppendStreamWriter import AppendStreamWriter
from writers.BatchWriter import BatchWriter
from writers.MergeBatchWriter import MergeBatchWriter
from writers.DataWriterFactory import DataWriterFactory


@pytest.mark.parametrize(
    "entity_config, expected_result",
    [
        (
            {
                "entity_name": "test",
                "target": {
                    "table_name": "test_schema.test_table",
                    "writer_type": "append_stream",
                },
            },
            AppendStreamWriter,
        ),
        (
            {
                "entity_name": "test",
                "target": {
                    "table_name": "test_schema.test_table",
                    "writer_type": "batch",
                    "writer_mode": "append",
                },
            },
            BatchWriter,
        ),
        (
            {
                "entity_name": "test",
                "target": {
                    "table_name": "test_schema.test_table",
                    "writer_type": "merge_batch",
                    "join_columns": ["test_column"],
                },
            },
            MergeBatchWriter,
        ),
    ],
)
def test_DataWriterFactory(entity_config, expected_result, spark_mock, dbutils_mock):

    # get the data writer
    data_writer = DataWriterFactory.get_datawriter(
        entity_config, spark_mock, dbutils_mock
    )
    # assert that the data writer is an instance of IDataWriter
    assert isinstance(data_writer, IDataWriter)
    # assert that the data writer is an instance of AppendStreamWriter
    assert isinstance(data_writer, expected_result)
