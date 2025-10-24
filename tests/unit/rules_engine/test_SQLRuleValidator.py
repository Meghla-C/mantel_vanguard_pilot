from unittest.mock import MagicMock
import pytest
from rules_engine.SQLRuleValidator import SQLRuleValidator
from pyspark.sql import Row
from helpers import assert_dataframes_are_equal
from pyspark.sql.utils import AnalysisException
from core_functions.config_models import RuleConfig


@pytest.mark.parametrize(
    "rule_config, expected_result_dict, execution_failure",
    [
        # failed query
        (
            {
                "rule_name": "test",
                "validator": "sql",
                "unique_key": "unique_key",
                "select_query": "SELECT ",
                "validation_status": "INVALID",
            },
            [],
            True,
        ),
        # successfull query no failed records
        (
            {
                "rule_name": "test_age",
                "validator": "sql",
                "unique_key": "id",
                "select_query": "SELECT id FROM subset_df where age < 18",
                "validation_status": "INVALID",
            },
            [
                {
                    "unique_key": "ALL",
                    "unique_key_column": "id",
                    "rule_name": "test_age",
                    "batch_record_count": 5,
                    "validation_status": "SUCCESS",
                }
            ],
            False,
        ),
        # successfull query with failed records
        (
            {
                "rule_name": "test_age",
                "validator": "sql",
                "unique_key": "id",
                "select_query": "SELECT id FROM subset_df where age < 30",
                "validation_status": "INVALID",
            },
            [
                {
                    "unique_key": "2",
                    "unique_key_column": "id",
                    "rule_name": "test_age",
                    "batch_record_count": 5,
                    "validation_status": "INVALID",
                },
                {
                    "unique_key": "5",
                    "unique_key_column": "id",
                    "rule_name": "test_age",
                    "batch_record_count": 5,
                    "validation_status": "INVALID",
                },
            ],
            False,
        ),
    ],
)
def test_sql_validator_execute_rule(
    rule_config, expected_result_dict, execution_failure, databricks_spark, dbutils_mock
):

    base_config = {
        "results_table_name": "schema.results_table",
        "source_table_name": "schema.source_df",
        "subset_method": "generic",
    }

    rule_config = {
        **base_config,
        **rule_config,
    }
    rule_validator = SQLRuleValidator(
        RuleConfig(**rule_config), databricks_spark, dbutils_mock
    )
    rule_validator.get_batch_id = MagicMock(return_value=1)

    expected_result_schema = "unique_key string, unique_key_column string, rule_name string, batch_record_count bigint, validation_status string"
    expected_result_df = databricks_spark.createDataFrame(
        expected_result_dict, expected_result_schema
    )

    # Create dummy data
    data = [
        Row(id="1", name="Alice", age=30, salary=70000.0),
        Row(id="2", name="Bob", age=25, salary=50000.0),
        Row(id="3", name="Charlie", age=35, salary=80000.0),
        Row(id="4", name="David", age=40, salary=60000.0),
        Row(id="5", name="Eva", age=29, salary=72000.0),
    ]

    # Create DataFrame
    dummy_df = databricks_spark.createDataFrame(data)

    if execution_failure:
        with pytest.raises(AnalysisException):
            rule_validator.execute_rule(subset_df=dummy_df)

    else:
        result_df = rule_validator.execute_rule(subset_df=dummy_df)
        result_df = result_df.drop("batch_id")
        expected_result_df = expected_result_df.drop("batch_id")

        assert_dataframes_are_equal(result_df, expected_result_df)


@pytest.mark.parametrize(
    "rule_config, expected_sql_query",
    [
        # test with one query param
        (
            {
                "rule_name": "test",
                "validator": "sql",
                "unique_key": "unique_key",
                "query_params": {"age": 30},
                "select_query": "SELECT * FROM subset_df WHERE age < {age}",
            },
            "SELECT * FROM subset_df WHERE age < 30",
        ),
        # test with multiple query params
        (
            {
                "rule_name": "test",
                "validator": "sql",
                "unique_key": "unique_key",
                "query_params": {"age": 30, "salary": 50000},
                "select_query": "SELECT * FROM subset_df WHERE age < {age} AND salary > {salary}",
            },
            "SELECT * FROM subset_df WHERE age < 30 AND salary > 50000",
        ),
    ],
)
def test_sql_validator_replace_query_params(
    rule_config, expected_sql_query, databricks_spark, dbutils_mock
):

    base_config = {
        "results_table_name": "schema.results_table",
        "source_table_name": "schema.source_df",
        "subset_method": "generic",
    }

    rule_config = {
        **base_config,
        **rule_config,
    }
    rule_validator = SQLRuleValidator(
        RuleConfig(**rule_config), databricks_spark, dbutils_mock
    )

    sql_query = rule_validator.replace_query_params(
        rule_config["select_query"], rule_config["query_params"]
    )
    assert sql_query == expected_sql_query
