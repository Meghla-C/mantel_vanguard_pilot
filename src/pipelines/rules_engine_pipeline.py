# Databricks notebook source
# MAGIC %md
# MAGIC # Rules Engine Pipeline
# MAGIC ## Set Up Paths

# COMMAND ----------

import os
import sys
import subprocess

# COMMAND ----------

base_path = os.path.abspath("").split("src")[0]

# Append the source directory to the system path
sys.path.append(os.path.join(base_path, "src"))

# Set configuration path
config_path = os.path.join(base_path, "config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Up Widgets and Configuration

# COMMAND ----------

# Set widgets for entity and YAML config file
dbutils.widgets.text("rule_name", "")  # noqa: F821
dbutils.widgets.text("yaml_config_file", "")  # noqa: F821
dbutils.widgets.text("requirements_file", "")  # noqa: F821
dbutils.widgets.text("logging_mode", "INFO")  # noqa: F821
dbutils.widgets.text("logs_directory", "")  # noqa: F821
# start timestamp allows to set watermark from which data will be processed (sets min updated_datetime that will be tested)
dbutils.widgets.text("start_timestamp", "")  # noqa: F821
dbutils.widgets.text("end_timestamp", "")  # noqa: F821

# COMMAND ----------

# Retrieve widget values
rule_name = dbutils.widgets.get("rule_name")  # noqa: F821
yaml_config_file = os.path.join(
    config_path, dbutils.widgets.get("yaml_config_file")  # noqa: F821
)
requirements_file = os.path.join(
    base_path, dbutils.widgets.get("requirements_file")  # noqa: F821
)
logging_mode = dbutils.widgets.get("logging_mode")  # noqa: F821
logs_directory = dbutils.widgets.get("logs_directory")  # noqa: F821
start_timestamp = dbutils.widgets.get("start_timestamp")  # noqa: F821
end_timestamp = dbutils.widgets.get("end_timestamp")  # noqa: F821

if start_timestamp == "":
    start_timestamp = None
if end_timestamp == "":
    end_timestamp = None

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Install Libraries

# COMMAND ----------

# Install pip modules
subprocess.run(["pip", "install", "-r", requirements_file])

# COMMAND ----------

# Rules Engine Modules
from core_functions.Config import Config
from rules_engine.RuleValidatorFactory import RuleValidatorFactory
from loggers.LoggerBuilder import LoggerBuilder
from core_functions.utils import get_current_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Logger

# COMMAND ----------

# Initialize logger for the Rules Engine pipeline
logger = LoggerBuilder.get_logger(
    spark, 
    log_dir=f"/Volumes/{get_current_catalog(spark)}/{logs_directory}", 
    name=f"Rules_Engine_Pipeline_{rule_name}", 
    level=logging_mode
)
logger.info(f"Rules Engine Pipeline configuration - Rule: {rule_name}, Config file: {yaml_config_file}, Start: {start_timestamp}, End: {end_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Entity Configuration

# COMMAND ----------

# Load the rule configuration from a YAML file
config = Config(yaml_config=yaml_config_file, yaml_key=rule_name, key_name="rule_name", logger=logger)  # noqa: F821
rule_config = config.get_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get RuleValidator

# COMMAND ----------

# Get the RuleValidator object from the factory
rule_validator = RuleValidatorFactory.get_validator(
    rule_config, spark, dbutils, logger  # noqa: F821
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Validation

# COMMAND ----------

rule_validator.execute_engine(
    start_timestamp=start_timestamp, end_timestamp=end_timestamp
)
