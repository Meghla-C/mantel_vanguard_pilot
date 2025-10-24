from readers.IDataReader import IDataReader
from core_functions.config_models import EntityConfig
from readers.FileStreamReader import FileStreamReader
from readers.APIReader import APIReader
from readers.TableStreamReader import TableStreamReader
from readers.APIReaderByID import APIReaderByID
from readers.TableBatchReader import TableBatchReader


class DataReaderFactory:
    """
    This is a factory class that creates the appropriate DataReader object based on the reader type specified in the entity config.
    """

    @staticmethod
    def get_datareader(entity_config, spark, dbutils, logger) -> IDataReader:
        """
        Method to create the appropriate DataReader object

        Inputs:
            - entity_config: Dict, A dictionary of config for the entity to be read
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
            - logger: Logger object for logging operations (required)
        Outputs:
            - IDataReader, The appropriate DataReader object
        """
        readers = {
            "file_stream": FileStreamReader,
            "api": APIReader,
            "api_by_id": APIReaderByID,
            "table_stream": TableStreamReader,
            "table_batch": TableBatchReader,
        }

        # Check if entity_config is already an EntityConfig object
        if isinstance(entity_config, EntityConfig):
            entity_config = entity_config
        else:
            entity_config = EntityConfig(**entity_config)
        reader_type = entity_config.source.reader_type
        return readers[reader_type](entity_config, spark, dbutils, logger)
