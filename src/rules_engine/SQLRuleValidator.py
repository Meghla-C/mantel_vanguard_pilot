from rules_engine.AbstractRuleValidator import AbstractRuleValidator
import core_functions.utils as utils
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Dict
from pyspark.sql.utils import AnalysisException


class SQLRuleValidator(AbstractRuleValidator):
    """
    This class inherits from the AbstractRuleValidator class to create a generic SQL rule validator.
    It takes in any SQL code as a string and executes it against the subset of data from the source table.
    If quey_params are provided in the rule configuration, the placeholders in the SQL query will be replaced with the values from the query_params.

    The rule configuration should be formatted as follows:
        example_rule :
            rule_name: rule_name
            validator: sql
            source_table_name: <schema>.<table>
            unique_key: unique_key_column
            select_query: SELECT unique_key_column FROM subset_df WHERE <condition>
    """

    def replace_query_params(self, query: str, params: Dict[str, str]) -> str:
        """
        Replace placeholders in SQL query with values from query_params in config.
        Inputs:
            - query: str, the SQL query to be executed
            - params: Dict[str, str], the dictionary of parameters to replace in the query
        Outputs:
            - query: str, the query with placeholders replaced
        """
        for key, value in params.items():
            placeholder = f"{{{key}}}"
            query = query.replace(placeholder, str(value))
        return query

    def execute_rule(self, subset_df: DataFrame) -> Dict:
        """
        This method executes the rule against the subset of data from the source table.
        It takes a SQL string from the rule configuration and executes it against the subset of data.
        The SQL query should always:
            1. Return a DataFrame only containing records that should be labelled as invalid
            2. Return only the surrogate key (uuid key) column.
                - <primary key column>: str, the primary key of the record
            3. Query the subset_df as it is created as a temp view within this metnhod.

        Inputs:
            - subset_df: DataFrame, the subset of data to be validated
        Outputs:
            - results_df: DataFrame, the results of the rule execution
        """
        unique_key = self.rule_config.unique_key
        select_query = self.rule_config.select_query
        rule_name = self.rule_config.rule_name
        validation_status = self.rule_config.validation_status
        validation_status_flag = self.rule_config.validation_status_flag
        query_params = self.rule_config.query_params

        temp_table_name = utils.get_uuid_temp_table_name()

        count = subset_df.count()

        batch_id = self.get_batch_id()

        subset_df.createTempView(temp_table_name)

        select_query = select_query.replace("subset_df", temp_table_name)

        # replace placeholders in query with values from query_params
        if query_params:
            select_query = self.replace_query_params(select_query, query_params)

        # try select
        try:
            selected_df = self.spark.sql(select_query)
        except AnalysisException as E:
            raise AnalysisException(f"Failed to execute SQL query: {E}")

        # if there are any failed records
        if selected_df.count() > 0:
            results_df = (
                selected_df.withColumn("unique_key", F.col(unique_key))
                .withColumn("unique_key_column", F.lit(unique_key))
                .withColumn("rule_name", F.lit(rule_name))
                .withColumn("batch_id", F.lit(batch_id))
                .withColumn("batch_record_count", F.lit(count).cast("bigint"))
                .withColumn(
                    "validation_status",
                    (
                        F.col("validation_status")
                        if validation_status_flag
                        else F.lit(validation_status)
                    ),
                )
                .drop(unique_key)
            )
        # if no failed records
        else:
            results_dict = [
                {
                    "unique_key": "ALL",
                    "unique_key_column": unique_key,
                    "rule_name": rule_name,
                    "batch_id": batch_id,
                    "batch_record_count": count,
                    "validation_status": "SUCCESS",
                }
            ]
            results_df = self.spark.createDataFrame(
                results_dict,
                "unique_key string, unique_key_column string, rule_name string, batch_id int, batch_record_count bigint, validation_status string",
            )

        return results_df

    def execute_engine(self, start_timestamp=None, end_timestamp=None):
        """
        This method imeplements a custom rule engine execution against a subset of data
        from the source table based on the timestamps provided.
        This method completes following steps:
            - subset the data from the source,
            - execute the rule,
            - parse the results,
            - update the results table and source records.

        Inputs:
            - start_timestamp: str, the start timestamp that will be applied to `update_datetime` column to subset data
            - end_timestamp: str, the end timestamp that will be applied to `update_datetime` column to subset data
        """
        subset_df = self.subset_data_from_source(start_timestamp, end_timestamp)

        if subset_df.count() == 0:
            return

        # get results from abstract rule
        results_df = self.execute_rule(subset_df)

        self.update_results_table(results_df)
