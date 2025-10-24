# Data Writers Documentation

This document provides an overview of the available data writer classes, their configurations, and limitations.

## Overview

The data writers module provides a set of classes that implement the `IDataWriter` interface for writing data to various targets. The `DataWriterFactory` class creates the appropriate writer based on the configuration provided.

## IDataWriter Interface

The `IDataWriter` interface defines the contract that all writer classes must implement. It requires:

1. An `__init__` method that takes:
   - `entity_config`: Dictionary containing the configuration for the entity to be written
   - `spark`: SparkSession instance
   - `dbutils`: Databricks Utilities object for secrets management

2. A `write_to_target_table` method that:
   - Takes a source DataFrame and optional configuration parameters
   - Writes the data to the target system
   - Handles any necessary data transformation and metadata management

When implementing a new writer class, you must:
- Inherit from `IDataWriter`
- Implement both required methods
- Handle configuration validation
- Implement appropriate error handling

## Example Entity Configuration for Writers

Below is an example `entity_config.yml` file showing configurations for different types of writers:

```yaml
# Example entity configuration file
<your_entity_name>: 
    target:
      writer_type: <your writer type>
      writer_parameter1 : writer_value1
      ... 
      writer_parameterN : writer_valueN
```

## Available Writers

### 1. BatchWriter
Writes data to a Delta table in batch mode with optional watermark support.

**Configuration Parameters:**
- `table_name`: Fully qualified target table name (schema.table)
- `writer_mode`: Write mode (e.g., "append", "overwrite")
- `write_options`: Dictionary of additional write options
- `enable_file_watermarking`: Whether to enable file watermarking
- `meta_table_name`: Name of the metadata table for watermarking
- `delete_condition`: Optional SQL condition for deleting records before write
- `exclude_columns`: List of columns to exclude from the write operation

**Sample Config:**
```yaml
target:
  writer_type: "batch"
  table_name: "schema.target_table"
  writer_mode: "append"
  write_options:
    mergeSchema: "true"
  enable_file_watermarking: true
  meta_table_name: "schema.meta_table"
  exclude_columns:
    - "temp_column"
    - "debug_column"
```

**Limitations:**
- Target table must exist in the catalog
- Requires proper schema alignment
- Watermark table must exist if watermarking is enabled

### 2. MergeBatchWriter
Writes data to a Delta table using MERGE operations for upsert functionality.

**Configuration Parameters:**
- `table_name`: Fully qualified target table name
- `join_columns`: List of columns to use for matching records
- `exclude_columns`: List of columns to exclude from updates
- `change_status_column`: Column name for tracking record changes (defaults to "md_change_status")
- `updated_datetime_column`: Column name for tracking update timestamps (defaults to "md_updated_datetime")
- `custom_update_condition`: Optional SQL condition to further filter which records get updated (e.g., "target.status != 'LOCKED'")
- `update_only`: Boolean flag to only perform updates without inserts (defaults to false)

**Sample Config:**
```yaml
target:
  writer_type: "merge_batch"
  table_name: "schema.target_table"
  join_columns:
    - "id"
    - "version"
  exclude_columns:
    - "created_date"
  change_status_column: "md_change_status"
  updated_datetime_column: "md_updated_datetime"
  custom_update_condition: "target.status != 'LOCKED'"
  update_only: false
```

**Behavior:**
- Updates existing records when join columns match and data has changed
- Inserts new records when join columns don't match (unless `update_only` is true)
- Sets `change_status_column` to 'U' for updates and 'I' for inserts
- Updates `updated_datetime_column` to current timestamp for modified records
- Can restrict updates using `custom_update_condition`
- When `update_only` is true, only performs updates (no inserts)

**Limitations:**
- Join columns must uniquely identify records
- Requires proper schema alignment

### 3. AppendStreamWriter
Writes data to a Delta table in streaming mode.

**Configuration Parameters:**
- `table_name`: Fully qualified target table name
- `trigger_type_value`: Dictionary of trigger configuration
- `checkpoint_location`: Location for storing streaming checkpoints

**Sample Config:**
```yaml
target:
  writer_type: "append_stream"
  table_name: "schema.target_table"
  trigger_type_value:
    availableNow: true
  checkpoint_location: "/path/to/checkpoints"
```

**Limitations:**
- Requires streaming-compatible source
- Checkpoint location must be accessible

## DLT (Databricks Declarative Pipeline) Writers

The framework now includes support for Databricks Declarative Pipelines (DLT) through specialized writers that integrate with DLT functions.

### 4. DLTAppendFlowWriter
Uses `dlt.append_flow()` to append data to target tables in DLT pipelines.

**Configuration Parameters:**
- `table_name`: Fully qualified target table name
- `flow_name`: Optional name for the flow (defaults to function name)
- `once`: Boolean flag to run flow only once (defaults to false)
- `spark_conf`: Dictionary of Spark configuration options
- `comment`: Optional comment for the flow
- `expectations`: List of data quality expectations

**Sample Config:**
```yaml
target:
  writer_type: "dlt_append_flow"
  table_name: "schema.target_table"
  flow_name: "sales_append_flow"
  once: false
  spark_conf:
    "spark.sql.adaptive.enabled": "true"
  comment: "Sales transactions flow"
  expectations:
    - name: "valid_amount"
      constraint: "amount > 0"
      action: "fail"
```

### 5. DLTAutoCDCWriter
Uses `dlt.create_auto_cdc_flow()` for change data capture operations.

**Configuration Parameters:**
- `table_name`: Fully qualified target table name
- `keys`: List of key columns for matching records
- `sequence_by`: Column to sequence changes by
- `ignore_null_updates`: Boolean to ignore null updates
- `apply_as_deletes`: SQL condition for delete operations
- `apply_as_truncates`: SQL condition for truncate operations
- `column_list`: List of columns to include
- `except_column_list`: List of columns to exclude
- `stored_as_scd_type`: SCD type ("1" or "2")
- `track_history_column_list`: Columns to track history for
- `track_history_except_column_list`: Columns to exclude from history tracking
- `flow_name`: Optional flow name
- `once`: Boolean flag for single execution

**Sample Config:**
```yaml
target:
  writer_type: "dlt_auto_cdc"
  table_name: "schema.customers"
  keys: ["customer_id"]
  sequence_by: "updated_timestamp"
  stored_as_scd_type: "2"
  track_history_column_list: ["name", "email", "phone"]
```

### 6. DLTAutoCDCSnapshotWriter
Uses `dlt.create_auto_cdc_from_snapshot_flow()` for snapshot-based change data capture.

**Configuration Parameters:**
- `table_name`: Fully qualified target table name
- `keys`: List of key columns for matching records
- `stored_as_scd_type`: SCD type ("1" or "2")
- `track_history_column_list`: Columns to track history for
- `track_history_except_column_list`: Columns to exclude from history tracking

**Sample Config:**
```yaml
target:
  writer_type: "dlt_auto_cdc_snapshot"
  table_name: "schema.products_history"
  keys: ["product_id"]
  stored_as_scd_type: "2"
  track_history_column_list: ["product_name", "price", "category"]
```

### 7. DLTSinkWriter
Uses `dlt.create_sink()` to create data sinks for exporting data.

**Configuration Parameters:**
- `sink_name`: Name of the sink
- `format`: Output format (e.g., "parquet", "delta", "json")
- `options`: Dictionary of sink options (path, mode, compression, etc.)

**Sample Config:**
```yaml
target:
  writer_type: "dlt_sink"
  sink_name: "data_export_sink"
  format: "parquet"
  options:
    "path": "/mnt/exports/processed_data"
    "mode": "overwrite"
    "compression": "snappy"
```

### 8. DLTTableWriter
Uses `dlt.table()` to create materialized tables in DLT pipelines.

**Configuration Parameters:**
- `table_name`: Fully qualified target table name
- `comment`: Optional table comment
- `table_properties`: Dictionary of Delta table properties
- `partition_cols`: List of partition columns
- `expectations`: List of data quality expectations

**Sample Config:**
```yaml
target:
  writer_type: "dlt_table"
  table_name: "schema.processed_data"
  comment: "Processed customer data table"
  table_properties:
    "quality": "gold"
    "owner": "data_team"
  partition_cols: ["date", "region"]
  expectations:
    - name: "valid_customer_id"
      constraint: "customer_id IS NOT NULL"
      action: "fail"
    - name: "positive_amount"
      constraint: "amount > 0"
      action: "drop"
```

### 9. DLTViewWriter
Uses `dlt.view()` to create views in DLT pipelines for intermediate data transformations.

**Configuration Parameters:**
- `view_name`: Fully qualified view name
- `comment`: Optional view comment
- `table_properties`: Dictionary of view properties
- `expectations`: List of data quality expectations

**Sample Config:**
```yaml
target:
  writer_type: "dlt_view"
  view_name: "schema.customer_summary_view"
  comment: "Customer summary view for analytics"
  table_properties:
    "quality": "silver"
    "purpose": "analytics"
  expectations:
    - name: "valid_customer_count"
      constraint: "customer_count > 0"
      action: "fail"
```

## Data Quality Expectations Format

DLT writers support a unified expectations format that provides flexible data quality validation. Expectations can be configured in three ways:

### 1. Simple String Format
For basic constraints, you can use a simple string:
```yaml
expectations:
  - "amount > 0"
  - "customer_id IS NOT NULL"
```

### 2. Dictionary Format with Action
For more control, use a dictionary with constraint and action:
```yaml
expectations:
  - name: "valid_amount"
    constraint: "amount > 0"
    action: "fail"  # Options: "fail", "drop", "warn"
  - name: "non_null_customer"
    constraint: "customer_id IS NOT NULL"
    action: "drop"
```

### 3. Legacy DLT Format
The framework also supports the legacy DLT format for backward compatibility:
```yaml
expect_all:
  "valid_amount": "amount > 0"
expect_all_or_drop:
  "valid_customer": "customer_id IS NOT NULL"
expect_all_or_fail:
  "critical_check": "status IN ('active', 'pending')"
```

### Expectation Actions
- **fail**: Pipeline fails if constraint is violated
- **drop**: Rows violating constraint are dropped
- **warn**: Violations are logged but processing continues

The unified format is automatically converted to the appropriate DLT decorator format during pipeline execution.

## Common Configuration Elements

All writers support the following common configuration elements:

1. **Table Name**: Must be fully qualified (schema.table)
2. **Write Options**: Additional options for the write operation
3. **Metadata Management**: Support for tracking changes and updates
4. **Data Quality Expectations**: Support for unified expectations format (DLT writers)
5. **Column Management**: Ability to exclude specific columns from operations

## Sample Configurations

Complete sample configurations for all DLT writers are available in the `config/dlt/` directory:
- `dlt_append_flow_sample.yml`
- `dlt_auto_cdc_sample.yml`
- `dlt_auto_cdc_snapshot_sample.yml`
- `dlt_streaming_table_sample.yml`
- `dlt_sink_sample.yml`
