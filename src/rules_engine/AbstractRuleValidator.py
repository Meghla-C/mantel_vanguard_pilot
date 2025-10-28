from typing import Dict
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, lit, to_timestamp, col
import core_functions.utils as utils
from writers.DataWriterFactory import DataWriterFactory
import json
from datetime import datetime
from pyspark.sql.types import StringType, MapType
from functools import reduce
from core_functions.config_models import EntityConfig


class AbstractRuleValidator(ABC):
    """
    Abstract Rule Validator is a part of Rule Engine framework. It implements some of key
    functions shared by child rule validation classes.
    """

    def __init__(self, rule_config, spark, dbutils, logger):
        """
        Construct for RuleValidator Class. Creates GE context and suite for the
        expectations to be executed.

        Inputs:
            - rule_config: Pydantic model, a dictionary of config for the rule to be validated
            - spark: SparkSession, instance of spark session
            - dbutils Object, Databricks Utitlities
        """

        self.rule_config = rule_config
        self.spark = spark
        self.dbutils = dbutils
        self.logger = logger

        # timestamps
        self.start_timestamp = None
        self.end_timestamp = None

    def subset_data_from_source(
        self,
        start_timestamp: str = None,
        end_timestamp: str = None,
        rule_config=None,
        spark=None,
        dbutils=None,
    ) -> DataFrame:
        """
        Method to read the data from the source, subset by start_timestamp timestamp and return the DataFrame

        Inputs:
            - start_timestamp: str, the timestamp that will be applied to `update_datetime` column to subset data
            - end_timestmap: str, the timestamp that will be applied to `update_datetime` column to subset data
            - rule_config: Pydantic model, a dictionary of config for the rule to be validated
            - spark: SparkSession, instance of spark session
            - dbutils Object, Databricks Utitlities
        Outputs:
            - df: DataFrame, subset of the source data that will be validated
        """
        utils.set_attributes(
            self, rule_config=rule_config, spark=spark, dbutils=dbutils
        )

        # if start_timestamp is not provided, get the latest start_timestamp value
        if not start_timestamp:
            start_timestamp = self.get_start_timestamp()

        if not end_timestamp:
            end_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp

        source_table_name = self.rule_config.source_table_name
        subset_method = self.rule_config.subset_method

        if not self.spark.catalog.tableExists(source_table_name):
            raise ValueError(f"Could not find source table {source_table_name}")

        # Subsetting based on the specified method
        if subset_method == "watermark_results_table":
            df = self.spark.table(source_table_name).filter(
                (col("md_created_datetime") >= to_timestamp(lit(start_timestamp)))
                & (col("md_created_datetime") < to_timestamp(lit(end_timestamp)))
            )

        elif subset_method == "custom_columns_source_data":
            if not self.rule_config.subset_column_list:
                raise ValueError(
                    "subset_column_list must be provided in rule_config for custom_columns_source_data method."
                )

            subset_column_list = self.rule_config.subset_column_list

            last_watermark_df = self.spark.table(source_table_name).filter(
                (col("md_created_datetime") >= to_timestamp(lit(start_timestamp)))
                & (col("md_created_datetime") < to_timestamp(lit(end_timestamp)))
            )

            min_aggregations = {c: "min" for c in subset_column_list}
            max_aggregations = {c: "max" for c in subset_column_list}

            min_values = last_watermark_df.agg(min_aggregations).collect()[0]
            max_values = last_watermark_df.agg(max_aggregations).collect()[0]

            min_max_values = {**min_values.asDict(), **max_values.asDict()}

            filter_conditions = [
                (col(c) >= min_max_values[f"min({c})"])
                & (col(c) <= min_max_values[f"max({c})"])
                for c in subset_column_list
            ]

            df = self.spark.table(source_table_name).filter(
                reduce(lambda x, y: x & y, filter_conditions)
            )

        else:
            raise ValueError(f"Unsupported subset_method: {subset_method}")

        return df

    def get_batch_id(self) -> int:

        if not self.spark.catalog.tableExists(self.rule_config.results_table_name):
            return 1

        max_batch_id = (
            self.spark.table(self.rule_config.results_table_name)
            .agg({"batch_id": "max"})
            .collect()[0][0]
        )

        if not max_batch_id:
            return 1

        return max_batch_id + 1

    def get_start_timestamp(self, rule_config=None) -> str:
        """
        Method to get the start_timetamp from the results table. The start_timestamp value is the timestamp of the last run when the source table was validated.
        If table hasnt been tested before, it will return 1900-01-01 00:00:00 and test whole table
        If results table doesnt exist, then it will return 1900-01-01 00:00:00 and test whole table

        Inputs:
            - rule_config: Pydantic model, a dictionary of config for the rule to be validated
        Outputs:
            - start_timestamp: str, the timestamp of the last run when the source table was validated
        """
        utils.set_attributes(self, rule_config=rule_config)
        DEFAULT_START_TIMESTAMP = "1900-01-01 00:00:00"

        if not self.spark.catalog.tableExists(self.rule_config.results_table_name):
            return DEFAULT_START_TIMESTAMP

        start_timestamp = (
            self.spark.table(self.rule_config.results_table_name)
            .filter(
                (col("source_table_name") == self.rule_config.source_table_name)
                & (col("rule_name") == self.rule_config.rule_name)
            )
            .agg({"md_created_datetime": "max"})
            .collect()[0][0]
        )

        if start_timestamp:
            return str(start_timestamp)
        else:
            return DEFAULT_START_TIMESTAMP

    def update_results_table(self, test_results: DataFrame) -> DataFrame:
        """
        Method to append to the results table the outcome of the rule validation.

        Intputs:
            - test_results: DataFrame, the DataFrame that contains the results of the rule validations
        Outputs:
            - test_metadata_df: DataFrame, the DataFrame that will be appended to the results table
        """

        test_results = test_results.selectExpr(
            "*",
            f"'{self.rule_config.source_table_name}' as source_table_name",
        )

        json_metadata = json.dumps(
            {
                "start_timestamp": self.start_timestamp,
                "end_timestamp": self.end_timestamp,
            }
        )
        metadata_schema = MapType(StringType(), StringType())

        # add metadata
        test_results = test_results.select(
            "*", from_json(lit(json_metadata), metadata_schema).alias("metadata_map")
        )
        test_metadata_df = utils.add_generic_metadata(
            test_results, "validation_result_id", "metadata_map", "source_table_name"
        )

        test_metadata_df = test_metadata_df.withColumnRenamed(
            "md_validation_result_id_pk", "validation_result_id"
        ).drop("md_validation_result_id_pk")

        test_metadata_df = (
            test_metadata_df.withColumn(
                "md_validation_status", col("validation_status")
            )
            .drop("validation_status")
            .drop("metadata_map")
        )

        results_table_config = {
            "target": {
                "table_name": self.rule_config.results_table_name,
                "writer_type": "batch",
                "writer_mode": "append",
            }
        }

        data_writer = DataWriterFactory.get_datawriter(
            EntityConfig(**results_table_config), self.spark, self.dbutils, self.logger
        )
        data_writer.write_to_target_table(test_metadata_df)
        return test_metadata_df

    @abstractmethod
    def execute_rule(subset_df: DataFrame) -> Dict:
        """
        Method is implemented in child class to execute custom rule. Method must always return
        Dictionary of Results created by GE rule.

        Inputs:
            - subset_df: DataFrame, the DataFrame that will be tested
        Outputs:
            - results: Dict, a GE dictionary of results for each expectation that was validated
        """
        raise NotImplementedError

    @abstractmethod
    def execute_engine(self, start_timestamp=None, end_timestamp=None):
        """
        This method imeplements a custom rule engine execution against a subset of data
        from the source table based on the timestamps provided.
        This method should:
            - subset the data from the source,
            - execute the rule,
            - parse the results,
            - update the results table and source records.

        Inputs:
            - start_timestamp: str, the start timestamp that will be applied to `update_datetime` column to subset data
            - end_timestamp: str, the end timestamp that will be applied to `update_datetime` column to subset data
        """
        raise NotImplementedError
