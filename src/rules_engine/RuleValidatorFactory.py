from typing import Dict
from rules_engine.AbstractRuleValidator import AbstractRuleValidator
from rules_engine.SQLRuleValidator import SQLRuleValidator
from core_functions.config_models import RuleConfig


class RuleValidatorFactory:
    """
    RuleValidatorFactory class to create the appropriate RuleValidator object
    """

    @staticmethod
    def get_validator(rule_config: Dict, spark, dbutils, logger) -> AbstractRuleValidator:
        """
        Method to create the appropriate validator object
        Inputs:
            - rule_config: Dict, A dictionary of config for the rule to be read
            - spark SparkSession, instance of a spark session
            - dbutils Object, Databricks Utilities
        Outputs:
            - An instance of a validator object
        """
        validators = {
            "sql": SQLRuleValidator,
        }

        rule_config = RuleConfig(**rule_config)
        rule_type = rule_config.validator
        return validators[rule_type](rule_config, spark, dbutils, logger)
