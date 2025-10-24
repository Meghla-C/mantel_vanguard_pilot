# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline Orchestrator
# MAGIC This notebook orchestrates DLT pipeline execution by reading a configuration file and iterating through entities, calling the data_etl_pipeline.py for each entity.

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
# Import Ingestion Framework modules
from core_functions.Config import Config
from writers.DataWriterFactory import DataWriterFactory
from loggers.LoggerBuilder import LoggerBuilder
# Import framework modules
from core_functions.utils import read_yaml, get_current_catalog

# COMMAND ----------

# Retrieve widget values
yaml_config_file = spark.conf.get("yaml_config_file")  # noqa: F821
requirements_file = spark.conf.get("requirements_file")  # noqa: F821
logging_mode = spark.conf.get("logging_mode", "INFO")  # noqa: F821
logs_directory = spark.conf.get("logs_directory", "")  # noqa: F821

# Construct full paths
full_config_path = os.path.normpath(os.path.join(base_path, "config", yaml_config_file))
full_requirements_path = os.path.normpath(os.path.join(base_path, requirements_file))

# Initialize logger for the DLT Pipeline Orchestrator
logger = LoggerBuilder.get_logger(spark, log_dir=f"/Volumes/{get_current_catalog(spark)}/{logs_directory}", name="DLT_Pipeline_Orchestrator", level=logging_mode)
logger.info(f"DLT Pipeline Orchestrator started - Config file: {yaml_config_file}, Requirements: {requirements_file}")

# COMMAND ----------

# Install pip modules
subprocess.run(["pip", "install", "-r", full_requirements_path])

# COMMAND ----------

# Load the configuration file to get all entities
logger.info(f"Loading configuration from: {full_config_path}")
config_data = read_yaml(full_config_path)
logger.info(f"Found {len(config_data)} entities to process: {list(config_data.keys())}")

# Store the table functions to prevent them from being garbage collected
entity_tables = {}

# Process each entity in the configuration
for entity_name, entity_config in config_data.items():  
    logger.info(f"Processing entity: {entity_name}")
    try:
        # Create dummy dataframe to meet interface
        dummy_df = spark.createDataFrame([("dummy_value",)], ["dummy_column"])

        # Load the entity configuration from a YAML file
        config = Config(yaml_config=full_config_path, yaml_key=entity_name, key_name="entity_name", logger=logger)
        entity_config = config.get_config()
        
        # Write Transformed Data to Delta Table
        logger.info(f"Writing transformed data to target for entity: {entity_name}")
        
        # Initialize DataWriterFactory and write data to a Delta table
        data_writer = DataWriterFactory.get_datawriter(entity_config, spark, dbutils, logger)  # noqa: F821
        result = data_writer.write_to_target_table(dummy_df)
        entity_tables[entity_name] = result
        logger.info(f"Successfully processed entity: {entity_name}")
    except Exception as e:
        logger.error(f"Failed to process entity {entity_name}: {str(e)}")
        raise

logger.info(f"DLT Pipeline Orchestrator completed successfully. Processed {len(entity_tables)} entities.")
logger.info(f"Registered tables: {list(entity_tables.keys())}")
