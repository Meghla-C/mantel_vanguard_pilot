# Databricks ETL Framework

A comprehensive, configurable ETL framework for Databricks that supports both traditional batch/streaming operations and Databricks Declarative Pipelines (DLT).

## Overview

This framework provides a flexible, configuration-driven approach to building ETL pipelines on Databricks. It supports multiple data sources, transformation patterns, and output destinations with built-in data quality validation and automatic metadata tracking.

## Key Features

- **Multiple Data Sources**: Support for files, tables, APIs, and streaming data
- **Flexible Transformations**: Built-in and custom transformation capabilities
- **Multiple Output Formats**: Batch, streaming, and DLT pipeline support
- **Data Quality**: Unified expectations format for data validation
- **Configuration-Driven**: YAML-based configuration for easy pipeline management
- **Automatic Metadata Tracking**: Built-in metadata columns for data lineage and processing history
- **Watermarking Support**: File and table-based watermarking for incremental processing
- **Rules Engine**: Configurable business rules validation

## Architecture

The framework is organized into several key modules:

- **Readers**: Extract data from various sources (files, tables, APIs)
- **Transformers**: Apply data transformations and business logic
- **Writers**: Load data to target destinations with quality checks
- **Pipelines**: Orchestrate end-to-end ETL processes
- **Rules Engine**: Validate data against business rules

## Supported Data Sources

### Readers
- **FileStreamReader**: Databricks Auto Loader for file ingestion
- **TableStreamReader**: Streaming table reads
- **TableBatchReader**: Batch table reads with watermarking
- **APIReader**: REST API data extraction
- **APIReaderByID**: Concurrent API calls by ID

### Writers
- **BatchWriter**: Standard Delta table writes with optional watermarking
- **MergeBatchWriter**: Upsert operations with MERGE
- **AppendStreamWriter**: Streaming data writes
- **DLT Writers**: Databricks Declarative Pipeline integration
  - DLTTableWriter
  - DLTAppendFlowWriter
  - DLTAutoCDCWriter
  - DLTAutoCDCSnapshotWriter
  - DLTSinkWriter

## Metadata Management

The framework automatically adds metadata columns to track data lineage and processing:

- **md_[entity]_pk**: Unique identifier for each record
- **md_metadata**: JSON metadata about the source
- **md_source_name**: Name of the data source
- **md_created_datetime**: Record creation timestamp
- **md_updated_datetime**: Last update timestamp
- **md_deleted_datetime**: Deletion timestamp (for soft deletes)
- **md_change_status**: Change type (I=Insert, U=Update, D=Delete)
- **md_validation_status**: Data quality validation status

### Watermarking Support
- **File Watermarking**: Track processed files to avoid reprocessing
- **Table Watermarking**: Use timestamp columns for incremental loads
- **Configurable Watermark Tables**: Custom tables for tracking processing state

## Configuration Structure

Pipelines are configured using YAML files with the following structure:

```yaml
entity_name:
  source:
    reader_type: "table_batch"
    table_name: "source.table"
    # reader-specific configuration
  
  transform:
    transformer_type: "default"
    # transformation configuration
  
  target:
    writer_type: "dlt_table"
    table_name: "target.table"
    expectations:
      - name: "data_quality_check"
        constraint: "value > 0"
        action: "fail"
    # writer-specific configuration
```

## Quick Start

1. **Set up development environment** (see Development Environment section below)

2. **Create a configuration file**:
   ```yaml
   my_pipeline:
     source:
       reader_type: "file_stream"
       path: "/path/to/source/files"
       format: "json"
     
     target:
       writer_type: "batch"
       table_name: "my_schema.my_table"
       writer_mode: "append"
   ```

3. **Run the pipeline**:
   ```python
   from src.pipelines.data_etl_pipeline import run_etl_pipeline
   
   run_etl_pipeline("config/my_pipeline.yml")
   ```

## Sample Configurations

The framework includes comprehensive sample configurations in the `config/` directory:

- **Basic ETL**: `config/sample_etl_config.yml`
- **DLT Pipelines**: `config/dlt/` directory
- **Rules Engine**: `config/rules_engine/`
- **Data Enrichment**: `config/enrich/`

## Documentation

Detailed documentation is available for each module:

- [Source Code Overview](src/README.md)
- [Data Readers](src/readers/README.md)
- [Data Writers](src/writers/README.md)
- [Transformers](src/transformers/README.md)
- [Rules Engine](src/rules_engine/README.md)

## Development Environment

### Prerequisites
- Podman Desktop
- VS Code with Dev Containers extension

### Setup Steps
1. Install `podman` desktop for your machine with [podman installer](https://podman-desktop.io/)
2. Once `podman` is installed, ensure it is running
3. Install "Dev Containers" VS Code extension
4. In the VS Code extension settings, go to "Dev Containers" extension and change "Docker Compose Path" to `podman-compose` and change "Docker Path" to `podman`
5. Run `cd .devcontainer`
6. Run `podman compose up`, this will build development environment container and mount it to the repo directory on your machine
7. In VS Code, in "Dev Containers" extension you should now see `devcontainer: python-latest-env`, attach to that container to start working in your development environment

## Testing

The framework includes comprehensive unit tests:

```bash
# Run all tests
python -m pytest tests/

# Run specific test module
python -m pytest tests/unit/readers/
```

## Contributing

1. Follow the established patterns for readers, writers, and transformers
2. Implement the appropriate interfaces (`IDataReader`, `IDataWriter`, `IDataTransform`)
3. Add comprehensive unit tests
4. Update documentation and configuration samples
5. Validate configurations using the Pydantic models in `core_functions/config_models.py`

## Recent Updates

- **Unified Expectations Format**: Simplified data quality validation across all DLT writers
- **Enhanced DLT Support**: Comprehensive support for all Databricks Declarative Pipeline functions
- **Improved Configuration Validation**: Stronger type checking and validation using Pydantic
- **Streamlined Architecture**: Removed deprecated components and improved code organization
