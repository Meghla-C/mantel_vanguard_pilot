# Databricks notebook source
# MAGIC %md
# MAGIC # Data ETL Pipeline
# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Libraries
# COMMAND ----------

dbutils.widgets.text("requirements_file", "")  # noqa: F821
requirements_file = dbutils.widgets.get("requirements_file")  # noqa: F821

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "-r", requirements_file])
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
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
# MAGIC ## Set Up Widgets

# COMMAND ----------

# Set widgets for entity and YAML config file
dbutils.widgets.text("entity", "")  # noqa: F821
dbutils.widgets.text("optimize", "True")  # noqa: F821
dbutils.widgets.text("requirements_file", "")  # noqa: F821
dbutils.widgets.text("logging_mode", "INFO")  # noqa: F821
dbutils.widgets.text("logs_directory", "")  # noqa: F821
# COMMAND ----------

# Retrieve widget values
entity = dbutils.widgets.get("entity")  # noqa: F821
yaml_config_file = os.path.join(
    config_path, dbutils.widgets.get("yaml_config_file")  # noqa: F821
)
optimize = dbutils.widgets.get("optimize").lower()  # noqa: F821
logging_mode = dbutils.widgets.get("logging_mode")  # noqa: F821
logs_directory = dbutils.widgets.get("logs_directory")  # noqa: F821

# COMMAND ----------

# Import Ingestion Framework modules
from core_functions.Config import Config
from readers.DataReaderFactory import DataReaderFactory
from transformers.DataTransformFactory import DataTransformFactory
from writers.DataWriterFactory import DataWriterFactory
from loggers.LoggerBuilder import LoggerBuilder
from core_functions.utils import get_current_catalog

# COMMAND ----------
# Initialize logger for the ETL pipeline
logger = LoggerBuilder.get_logger(spark, log_dir=f"/Volumes/{get_current_catalog(spark)}/{logs_directory}", name=f"ETL_Pipeline_{entity}", level=logging_mode)
logger.info(f"Pipeline configuration - Entity: {entity}, Config file: {yaml_config_file}, Optimize: {optimize}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Entity Configuration

# COMMAND ----------

# Load the entity configuration from a YAML file
config = Config(yaml_config=yaml_config_file, yaml_key=entity, key_name="entity_name", logger=logger)  # noqa: F821
entity_config = config.get_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source Data

# COMMAND ----------

# Get the IDataReader object from the factory
data_reader = DataReaderFactory.get_datareader(
    entity_config, spark, dbutils, logger  # noqa: F821
)

# Read source data into a DataFrame
source_df = data_reader.read_source_to_dataframe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform the data

# COMMAND ----------

# Get the IDataTransform object from the factory
transformers = DataTransformFactory.get_datatransform(
    source_df, entity_config, spark, dbutils, logger  # noqa: F821
)

# Read source data into transformed DataFrame(s)
transform_dfs = transformers.apply_transform()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Transformed Data to Delta Table

# COMMAND ----------

# Initialize DataWriterFactory and write data to a Delta table
data_writer = DataWriterFactory.get_datawriter(entity_config, spark, dbutils, logger)  # noqa: F821
data_writer.write_to_target_table(transform_dfs[entity])


# COMMAND ----------

# Optimize and vacuum the Delta table
if optimize == 'true':
    target_table = entity_config["target"].get("table_name")
    spark.sql(f"OPTIMIZE {target_table}")  # noqa: F821
    spark.sql(f"VACUUM {target_table}")  # noqa: F821
