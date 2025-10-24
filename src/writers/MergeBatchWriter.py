from writers.IDataWriter import IDataWriter
import core_functions.utils as utils
from pyspark.sql import DataFrame


class MergeBatchWriter(IDataWriter):
    """
    MergeBatchWriter class implementing the IDataWriter class.
    It writes data to a Delta table in batch mode using the MERGE statement.
    It performs an upsert operation, which means it updates existing records and inserts new records.
    """

    def __init__(self, entity_config, spark, dbutils, logger):
        """
        Constructor for the MergeBatchWriter class

        Inputs:
            - spark SparkSession, instance of a spark session
            - entity_config : Pydantic model, A dictionary of config for the entity to be written
            - dbutils Object, Databricks Utilities is not used in this class but required for the interface
        Outputs:
            - None.
        """
        self.spark = spark
        self.dbutils = dbutils
        self.entity_config = entity_config
        self.logger = logger

    def write_to_target_table(
        self,
        source_dataframe: DataFrame,
        entity_config=None,
        spark=None,
        dbutils=None,
    ) -> None:
        """
        This method upserts records from source_dataframe to target table specified in entity config.
        If the record is already present in the target table with no changes, no update is performed.
        If the record is already present in the target table but has changed, it updates the record, and
        sets its "md_change_status" to "U" and "md_updated_datetime" to current timestamp.
        If the record is not present in the target table, it inserts the record with all columns
        (including metadata columns) from source_dataframe.

        Inputs:
            - source_dataframe : DataFrame, Data from the entity to be written into target place of Delta Lake
            - spark SparkSession, instance of a spark session, Optional
            - entity_config : Pydantic model, A dictionary of config for the entity to be written
                {
                    "entity_name": "entity_name",
                    "target": {
                        "table_name": "target_table_name",
                        "join_columns": ["col1", "col2"]
                    }
                }
                Here "source_table_name" is the name of the table from which data is read.
                "target_table_name" is the name of the table to which data is written.
                "join_columns" is a list of column names used to join on between source and target tables,
                joining on these columns should identify if a record is already present in the target table.
            - dbutils Object, Databricks Utilities is not used in this class but required for the interface
        """

        self.logger.info(f"Writing to target table: {self.entity_config.target.table_name}")

        if not entity_config:   
            entity_config = self.entity_config
        if not spark:
            spark = self.spark
        if not dbutils:
            dbutils = self.dbutils

        temp_update_table_name = utils.get_uuid_temp_table_name()

        self.logger.info(f"Creating temp update table: {temp_update_table_name}")
        
        # remove change status as we will override it
        if self.entity_config.target.change_status_column in source_dataframe.columns:
            source_dataframe = source_dataframe.drop(
                self.entity_config.target.change_status_column
            )
            self.logger.debug(f"Dropped {self.entity_config.target.change_status_column} column from source dataframe")

        source_dataframe.createTempView(temp_update_table_name)

        all_cols = source_dataframe.columns

        # get every col except the md_change_status column as we need to update default "I" with "U"
        update_cols = [
            col
            for col in all_cols
            if col not in self.entity_config.target.exclude_columns
            and col not in self.entity_config.target.join_columns
        ]

        self.logger.debug(f"Update columns: {update_cols}")

        # build merge statement
        merge_condition = " AND ".join(
            [
                f"COALESCE(CAST(target.{col_name} AS STRING), 'NULL') == COALESCE(CAST(updates.{col_name} AS STRING), 'NULL')"
                for col_name in self.entity_config.target.join_columns
            ]
        )

        self.logger.debug(f"Merge condition: {merge_condition}")

        update_condition = " OR ".join(
            [
                f"COALESCE(CAST(target.{col_name} AS STRING), 'NULL') != COALESCE(CAST(updates.{col_name} AS STRING), 'NULL')"
                for col_name in update_cols
            ]
        )

        self.logger.debug(f"Update condition: {update_condition}")

        if self.entity_config.target.custom_update_condition:
            update_condition = f"({update_condition}) AND ({self.entity_config.target.custom_update_condition})"
            self.logger.debug(f"Update condition with custom update condition: {update_condition}")

        update_config = [
            f"target.{col_name} = updates.{col_name}" for col_name in update_cols
        ]

        update_config.append(
            f"target.{self.entity_config.target.change_status_column} = 'U', target.{self.entity_config.target.updated_datetime_column} = current_timestamp()"
        )
        update_set = ", ".join(update_config)

        insert_cols = ", ".join(all_cols)
        values_cols = ", ".join([f"updates.{col_name}" for col_name in all_cols])

        if update_condition and self.entity_config.target.update_only:
            merge_definition = f"""MERGE INTO {self.entity_config.target.table_name} AS target USING {temp_update_table_name} AS updates
                                    ON {merge_condition}
                                    WHEN MATCHED AND ({update_condition}) THEN
                                        UPDATE SET {update_set}
                                """
        elif update_condition and not self.entity_config.target.update_only:
            merge_definition = f"""MERGE INTO {self.entity_config.target.table_name} AS target USING {temp_update_table_name} AS updates
                                    ON {merge_condition}
                                    WHEN MATCHED AND ({update_condition}) THEN
                                        UPDATE SET {update_set}
                                    WHEN NOT MATCHED THEN
                                        INSERT ({insert_cols}, {self.entity_config.target.change_status_column})
                                        VALUES ({values_cols}, 'I')
                                """
        else:
            merge_definition = f"""MERGE INTO {self.entity_config.target.table_name} AS target USING {temp_update_table_name} AS updates
                                    ON {merge_condition}
                                    WHEN NOT MATCHED THEN
                                        INSERT ({insert_cols}, {self.entity_config.target.change_status_column})
                                        VALUES ({values_cols}, 'I')
                                """

        self.logger.debug(f"Merge definition: {merge_definition}")

        self.spark.sql(merge_definition)

        self.logger.info(f"Completed merge to target table: {self.entity_config.target.table_name}")
