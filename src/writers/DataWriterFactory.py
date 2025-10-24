from typing import Dict
from core_functions.config_models import EntityConfig
from writers.IDataWriter import IDataWriter
from writers.AppendStreamWriter import AppendStreamWriter
from writers.BatchWriter import BatchWriter
from writers.MergeBatchWriter import MergeBatchWriter
from writers.DLTAppendFlowWriter import DLTAppendFlowWriter
from writers.DLTAutoCDCWriter import DLTAutoCDCWriter
from writers.DLTAutoCDCSnapshotWriter import DLTAutoCDCSnapshotWriter
from writers.DLTSinkWriter import DLTSinkWriter
from writers.DLTTableWriter import DLTTableWriter
from writers.DLTViewWriter import DLTViewWriter


class DataWriterFactory:
    """
    This is a factory class that creates the appropriate DataWriter object based on the writer type specified in the entity config.
    """

    @staticmethod
    def get_datawriter(entity_config: Dict, spark, dbutils, logger) -> IDataWriter:
        """
        Method to create the appropriate DataWriter object

        Inputs:
            - entity_config: Dict, A dictionary of config for the entity to be written
            - spark: SparkSession, instance of a spark session
            - dbutils: Object, Databricks Utilities works with secrets
        Outputs:
            - IDataWriter, The appropriate DataWriter object
        """
        writers = {
            "append_stream": AppendStreamWriter,
            "batch": BatchWriter,
            "merge_batch": MergeBatchWriter,
            "dlt_append_flow": DLTAppendFlowWriter,
            "dlt_auto_cdc": DLTAutoCDCWriter,
            "dlt_auto_cdc_snapshot": DLTAutoCDCSnapshotWriter,
            "dlt_sink": DLTSinkWriter,
            "dlt_table": DLTTableWriter,
            "dlt_view": DLTViewWriter,
        }

        # Check if entity_config is already an EntityConfig object
        if isinstance(entity_config, EntityConfig):
            entity_config = entity_config
        else:
            entity_config = EntityConfig(**entity_config)
        writer_type = entity_config.target.writer_type
        logger.info(f"Selected writer type: {writer_type}")
        return writers[writer_type](entity_config, spark, dbutils, logger)
