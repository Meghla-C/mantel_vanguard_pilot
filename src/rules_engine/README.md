# Rules Engine Module

## Overview

The Rules Engine is a flexible framework for validating data quality in your Databricks environment. It allows you to define and execute custom validation rules against your data using SQL queries. The engine supports incremental validation by tracking validation results and only processing new or modified data.

Key features:
- SQL-based rule definitions
- Incremental validation using watermarks
- Support for both critical and anomalous data issues
- Results tracking and reporting
- Parallel execution capabilities
- Customizable validation statuses

## Validation Status Types

The Rules Engine supports two types of validation statuses:

1. **Critical Status**
   - Used for data quality issues that must be resolved before data can proceed downstream
   - Stops data from flowing to subsequent processing layers

2. **Anomaly Status**
   - Used to flag potential data quality issues that don't require immediate blocking
   - Data can continue to flow to future processing layers
   - Anomaly testing jobs can be executed in parallel with downstream jobs
   - Useful for monitoring and gradual improvement of data quality

## High Level Execution Sequence

The Rules Engine follows a specific sequence of tasks when executing validation rules:

1. **Configuration Loading**
   - Loads the YAML configuration file
   - Validates the configuration structure
   - Initializes the appropriate validator based on the `validator` field

2. **Data Subsetting**
   - Determines the subset of data to validate based on the `subset_method`:
     - For `watermark_results_table`: Uses `md_created_datetime` as a watermark to get only new/modified records
     - For `custom_columns_source_data`: Uses specified columns to subset the data
   - Applies timestamp filters if provided (`start_timestamp` and `end_timestamp`)

3. **Rule Execution**
   - Creates a temporary view of the subsetted data
   - Injects any `query_params` into the SQL query
   - Executes the validation query against the subsetted data
   - Processes the results to identify invalid records

4. **Results Processing**
   - Generates a new `batch_id` for the validation run
   - Counts the total number of records processed
   - Formats the results with metadata (timestamps, status, etc.)
   - Updates the results table with validation outcomes

5. **Source Table Updates**
   - Updates the source table's validation status for records that failed validation
   - Handles both status updates
   - Preserves existing critical statuses when updating

## Databricks Job Execution

The Rules Engine is designed to run as a Databricks job with parallel task execution. The job structure follows these principles:

1. **Parallel Task Execution**
   - Individual validation rules are executed as separate parallel tasks
   - Each task runs independently, allowing for efficient processing of multiple rules

2. **Results Consolidation**
   - A final task runs after all validation tasks complete
   - This task consolidates and updates the results table with all validation outcomes
   - Ensures consistent and complete reporting of validation results

## Configuration with YAML

The Rules Engine uses YAML configuration files to define validation rules. Each rule is defined as a separate entry in the YAML file with the following structure:

```yaml
rule_name:
  validator: sql
  source_table_name: schema.table_name
  results_table_name: meta.rule_validation_results
  unique_key: table_id_column
  validation_status: CRITICAL | ANOMALY
  subset_method: watermark_results_table | custom_columns_source_data
  query_params:
    param1: value1
    param2: value2
  select_query: >
    SELECT
      id
    FROM subset_df
    WHERE condition
```

### Configuration Parameters

1. **Basic Parameters**:
   - `validator`: Currently only "sql" is supported
   - `source_table_name`: The table to validate (format: schema.table_name)
   - `results_table_name`: Where to store validation results
   - `unique_key`: The primary key column of the source table
   - `validation_status`: Either "CRITICAL" or "ANOMALY"

2. **Subsetting Methods**:
   - `subset_method`: How to subset the data for validation
     - `watermark_results_table`: Uses `md_created_datetime` as watermark
     - `custom_columns_source_data`: Uses custom columns for subsetting
   - `subset_column_list`: Required for `custom_columns_source_data` method

3. **Query Configuration**:
   - `query_params`: Parameters to inject into the SQL query
   - `select_query`: The SQL query to execute
     - Must select from `subset_df` (created by the engine)
     - Should return only invalid records
     - Must include the unique key column

### Example Configurations

1. **Null Check Rule**:
```yaml
null_check_rule:
  validator: sql
  source_table_name: schema.table_name
  results_table_name: meta.rule_validation_results
  unique_key: table_id
  validation_status: CRITICAL
  subset_method: watermark_results_table
  select_query: >
    SELECT
      id
    FROM subset_df
    WHERE column_name IS NULL
```

2. **Duplicate Check Rule**:
```yaml
duplicate_check_rule:
  validator: sql
  source_table_name: schema.table_name
  results_table_name: meta.rule_validation_results
  unique_key: table_id
  validation_status: ANOMALY
  subset_method: custom_columns_source_data
  subset_column_list: ["date_column"]
  select_query: >
    WITH cte AS (
      SELECT 
        id, 
        COUNT(*) AS count 
      FROM subset_df 
      GROUP BY column_name
      HAVING COUNT(*) > 1
    )
    SELECT 
      id
    FROM subset_df
    INNER JOIN cte ON subset_df.id = cte.id
```

### Results Table Structure

The validation results are stored in a table with the following key columns:
- `validation_result_id`: Unique identifier for the validation result
- `source_table_name`: The table that was validated
- `unique_key`: The primary key of the invalid record
- `rule_name`: The name of the rule that failed
- `batch_id`: The batch number of the validation run
- `batch_record_count`: Total number of records processed
- `validation_status`: The status of the validation (CRITICAL/ANOMALY)
- `md_created_datetime`: When the result was created
- `md_updated_datetime`: When the result was last updated
