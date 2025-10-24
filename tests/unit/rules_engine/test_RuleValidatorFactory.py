import pytest
from rules_engine.AbstractRuleValidator import AbstractRuleValidator
from rules_engine.RuleValidatorFactory import RuleValidatorFactory
from rules_engine.SQLRuleValidator import SQLRuleValidator


@pytest.mark.parametrize(
    "rule_config, expected_result",
    [
        (
            {
                "validation_rule": "test",
                "validator": "sql",
                "unique_key": "pk",
                "results_table_name": "schema.results_table",
                "source_table_name": "schema.source_df",
                "subset_method": "generic",
            },
            SQLRuleValidator,
        ),
    ],
)
def test_rule_validator_factory(rule_config, expected_result, spark_mock, dbutils_mock):

    # get the rule validator
    rule_validator = RuleValidatorFactory.get_validator(
        rule_config, spark_mock, dbutils_mock
    )

    # assert that the rule validator is an instance of AbstractRuleValidator
    assert isinstance(rule_validator, AbstractRuleValidator)
    # assert that the rule validator is an instance of given validator
    assert isinstance(rule_validator, expected_result)
