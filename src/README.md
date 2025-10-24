# Source Code Documentation

This document provides an overview of the `src` folder, its submodules, and their functionalities.

## Overview

The `src` folder contains the core implementation of the ETL framework. It is organized into several submodules, each responsible for a specific aspect of the ETL process. The key submodules include:

- **core_functions**: Contains utility functions and configuration models.
- **pipelines**: Implements the ETL pipelines, including data extraction, transformation, and loading processes.
- **readers**: Provides various data readers to fetch data from different sources, such as APIs, files, and tables.
- **rules_engine**: Implements the rules engine for validating and processing data based on predefined rules.
- **transformers**: Contains data transformation logic, including default and custom transformations.
- **writers**: Handles writing data to different destinations, supporting both batch and streaming modes.

## Configuration Validation

Configurations for readers, writers, and transformers are defined in YAML files and validated using the models in `core_functions/config_models.py`. The validation leverages **Pydantic**, a library for data validation and settings management, ensuring that all configurations adhere to the expected schema and reducing runtime errors.

## Example Usage

Below is an example of how to use the core components of the framework:

```python
from src.readers.DataReaderFactory import DataReaderFactory
from src.transformers.DataTransformFactory import DataTransformFactory
from src.writers.DataWriterFactory import DataWriterFactory

# Initialize reader, transformer, and writer
reader = DataReaderFactory.get_datareader(config)
transformer = DataTransformFactory.get_datatransform(source_df,config)
writer = DataWriterFactory.get_datawriter(config)

# Perform ETL
data = reader.read_source_to_dataframe()
transformed_data = transformers.apply_transform()
writer.write_to_target_table(transformed_data)
```

## Submodule Details

### core_functions
- **Purpose**: Provides utility functions and configuration models.
- **Key Components**:
  - `config_models.py`: Validates configurations for readers, writers, and transformers.
  - `Config.py`: Handles general configuration management.
  - `utils.py`: Contains utility functions for common operations.

### pipelines
- **Purpose**: Implements ETL pipelines.
- **Key Components**:
  - `data_etl_pipeline.py`: Main pipeline for data extraction, transformation, and loading.
  - `rules_engine_pipeline.py`: Pipeline for applying rules to data.
  - `rules_engine_results_update.py`: Updates results based on rules engine output.

### readers
- **Purpose**: Provides data readers for various sources.
- **Key Components**: Refer to `src/readers/README.md` for detailed documentation.

### rules_engine
- **Purpose**: Implements the rules engine for data validation and processing.
- **Key Components**: Refer to `src/rules_engine/README.md` for detailed documentation.

### transformers
- **Purpose**: Contains data transformation logic.
- **Key Components**: Refer to `src/transformers/README.md` for detailed documentation.

### writers
- **Purpose**: Handles data writing to various destinations.
- **Key Components**: Refer to `src/writers/README.md` for detailed documentation.