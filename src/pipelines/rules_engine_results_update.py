# Databricks notebook source
# MAGIC %md
# MAGIC # Rules Engine Results Update Pipeline
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
dbutils.widgets.text(  # noqa: F821
    "results_table_name", "meta.clean_validation_results"
)
dbutils.widgets.text("requirements_file", "")  # noqa: F821
dbutils.widgets.text("logging_mode", "INFO")  # noqa: F821
dbutils.widgets.text("logs_directory", "")  # noqa: F821

# COMMAND ----------

# Retrieve widget values
results_table_name = dbutils.widgets.get("results_table_name")  # noqa: F821
requirements_file = os.path.join(
    base_path, dbutils.widgets.get("requirements_file")  # noqa: F821
)
logging_mode = dbutils.widgets.get("logging_mode")  # noqa: F821
logs_directory = dbutils.widgets.get("logs_directory")  # noqa: F821

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Install Libraries

# COMMAND ----------

# Install pip modules
subprocess.run(["pip", "install", "-r", requirements_file])

# COMMAND ----------

# Rules Engine Modules
from core_functions.Config import Config
from core_functions.parallel_run_utils import parallel_run, get_writer_and_write_wrapper
from loggers.LoggerBuilder import LoggerBuilder
from core_functions.utils import get_current_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Logger

# COMMAND ----------

# Initialize logger for the Rules Engine Results Update pipeline
logger = LoggerBuilder.get_logger(
    spark, 
    log_dir=f"/Volumes/{get_current_catalog(spark)}/{logs_directory}", 
    name="Rules_Engine_Results_Update", 
    level=logging_mode
)
logger.info(f"Rules Engine Results Update Pipeline configuration - Results table: {results_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Tables to Update

# COMMAND ----------

logger.info(f"Querying tables to update from results table: {results_table_name}")
tables_to_update = spark.sql(  # noqa: F821
    f"select distinct source_table_name, unique_key_column from {results_table_name}"
).collect()

logger.info(f"Found {len(tables_to_update)} tables to update")
logger.debug(f"Tables to update: {tables_to_update}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Each Table

# COMMAND ----------

target_configs = {}
result_dfs = {}

for target in tables_to_update:
    query = f"\
        SELECT unique_key as {target['unique_key_column']}, \
            CASE \
              WHEN MAX(CASE WHEN md_validation_status = 'CRITICAL' THEN 1 ELSE 0 END) = 1 \
              THEN 'CRITICAL' \
              ELSE 'ANOMALOUS' \
          END AS md_validation_status     \
        FROM meta.clean_validation_results \
        WHERE source_table_name = '{target['source_table_name']}' \
            AND unique_key_column = '{target['unique_key_column']}' \
            AND md_validation_status = 'CRITICAL' or md_validation_status = 'ANOMALOUS' \
            AND md_created_datetime > ( \
                SELECT MAX(md_updated_datetime) \
                FROM {target['source_table_name']}) \
        GROUP BY unique_key"

    target_configs[target["source_table_name"]] = {
        "entity_name": target["source_table_name"].split(".")[1],
        "target": {
            "table_name": target["source_table_name"],
            "writer_type": "merge_batch",
            "join_columns": [target["unique_key_column"]],
            "custom_update_condition": "target.md_validation_status IS NULL OR target.md_validation_status = 'ANOMALOUS'",  # needed to guarantee we dont update critical records
            "update_only": True,
            "exclude_columns": [
                "md_metadata",
                "md_source_name",
                "md_created_datettime",
                "md_updated_datetime",
                "md_deleted_datetime",
                "md_change_status",
            ],
        },
    }

    re_results_df = spark.sql(query)  # noqa: F821

    result_dfs[target["source_table_name"]] = re_results_df

# COMMAND ----------

items = [
    (
        target_configs[target_entity_name],
        result_dfs[target_entity_name],
        spark,  # noqa: F821
        dbutils,  # noqa: F821
    )
    for target_entity_name in result_dfs.keys()
]

logger.debug(f"Prepared items for parallel processing: {items}")

# COMMAND ----------

result = parallel_run(get_writer_and_write_wrapper, items)
