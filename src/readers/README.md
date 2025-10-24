# Data Readers Documentation

This document provides an overview of the available data reader classes, their configurations, and limitations.

## Overview

The data readers module provides a set of classes that implement the `IDataReader` interface for reading data from various sources. The `DataReaderFactory` class creates the appropriate reader based on the configuration provided.

## IDataReader Interface

The `IDataReader` interface defines the contract that all reader classes must implement. It requires:

1. An `__init__` method that takes:
   - `entity_config`: Dictionary containing the configuration for the entity to be read
   - `spark`: SparkSession instance
   - `dbutils`: Databricks Utilities object for secrets management

2. A `read_source_to_dataframe` method that:
   - Takes the same parameters as `__init__` (all optional)
   - Returns a Spark DataFrame containing the read data
   - Handles any necessary data transformation and metadata addition

When implementing a new reader class, you must:
- Inherit from `IDataReader`
- Implement both required methods
- Handle configuration validation
- Add appropriate metadata to the output DataFrame
- Implement error handling for common failure scenarios

## Example Entity Configuration for Readers

Below is an example `entity_config.yml` file showing configurations for different types of readers:

```yaml
# Example entity configuration file
<your_entity_name>: 
    source:
      reader_type: <your reader type>
      reader_paramter1 : reader_value1
      ... 
      reader_parameterN : reader_valueN
```

## Available Readers

### 1. FileStreamReader
Reads files from a specified path using Databricks Auto Loader.

**Configuration Parameters:**
- `read_options`: Dictionary of additional read options
- `source_volume_path`: Path to the source files
- `format`: File format (e.g., json, csv, parquet)
- `explode_column`: Optional column to explode if it contains arrays
- `schema_location`: Location to store schema information

**Sample Config:**
```yaml
source:
  reader_type: "file_stream"
  read_options:
    header: "true"
    inferSchema: "true"
  format: "json"
  explode_column: "items"
  path: "/path/to/files"
```

**Limitations:**
- Requires Databricks Auto Loader
- Schema evolution must be handled through schema_location
- File format must be supported by Auto Loader

### 2. TableStreamReader
Reads data from a table in streaming mode.

**Configuration Parameters:**
- `read_options`: Dictionary of additional read options
- `filter_condition`: Optional SQL filter condition
- `table_name`: Fully qualified table name (schema.table)

**Sample Config:**
```yaml
source:
  reader_type: "table_stream"
  read_options:
    maxFilesPerTrigger: 100
  filter_condition: "status = 'active'"
  table_name: "schema.table_name"
```

**Limitations:**
- Table must exist in the catalog
- Requires streaming-compatible table
- Table name must include schema

### 3. TableBatchReader
Reads data from a table in batch mode with watermark support.

**Configuration Parameters:**
- `read_options`: Dictionary of additional read options
- `filter_condition`: Optional SQL filter condition
- `table_name`: Fully qualified table name
- `select_query`: Optional SQL query to subset data
- `source_watermark_column`: Column for tracking updates
- `batch_window`: Time window for batch processing
- `watermark_table_name`: Optional watermark table
- `watermark_table_datetime_column`: Column in watermark table
- `load_type`: "full" or incremental
- `keep_source_metadata`: Whether to keep source metadata
- `generated_metadata_columns_to_remove`: List of metadata columns to remove
- `source_metadata_columns_to_keep`: List of metadata columns to keep

**Sample Config:**
```yaml
source:
  reader_type: "table_batch"
  read_options: {}
  table_name: "schema.table_name"
  source_watermark_column: "md_updated_datetime"
  batch_window: 60
  watermark_table_name: "schema.watermark_table"
  load_type: "incremental"
  keep_source_metadata: true
  generated_metadata_columns_to_remove: []
  source_metadata_columns_to_keep:
    - "md_created_datetime"
    - "md_updated_datetime"
```

**Limitations:**
- Table must exist in the catalog
- Requires datetime column for incremental loads
- Watermark table must exist for incremental loads


### 4. APIReader
Makes a single API call and returns the response as a DataFrame.

**Configuration Parameters:**
- `host_name_secret_key`: Key for host endpoint in secrets
- `username_secret_key`: Key for username in secrets
- `password_secret_key`: Key for password in secrets
- `path`: API endpoint path

**Sample Config:**
```yaml
source:
  reader_type: "api"
  host_name_secret_key: "api_host"
  username_secret_key: "api_user"
  password_secret_key: "api_pass"
  path: "/api/v1/data"
```

**Limitations:**
- Only supports GET requests
- Requires basic authentication

### 5. APIReaderByID
Makes an initial API call and then concurrent calls for each ID in the response.

**Configuration Parameters:**
- Same as APIReader plus:
- `id_key`: Key in response to use for subsequent API calls

**Sample Config:**
```yaml
source:
  reader_type: "api_by_id"
  host_name_secret_key: "api_host"
  username_secret_key: "api_user"
  password_secret_key: "api_pass"
  path: "/api/v1/items"
  id_key: "item_id"
```

**Limitations:**
- Same as APIReader
- Initial response must contain IDs for subsequent calls
- Concurrent calls may be rate-limited by API
