# Logger System with Chain of Responsibility Pattern

This logger system implements a Chain of Responsibility pattern that allows different loggers to handle different types of logging operations.

## Architecture

The system consists of several specialized loggers that work together in a chain:

```
STOutLogger → STErrorLogger → VolumeLogger
```

### Logger Chain Flow

1. **STOutLogger**: Handles DEBUG, INFO, and WARNING levels
   - Outputs to stdout for DEBUG and INFO
   - Outputs to stderr for WARNING
   - Passes ERROR and CRITICAL to next handler

2. **STErrorLogger**: Handles ERROR and CRITICAL levels
   - Outputs to stderr for ERROR and CRITICAL
   - Passes all levels to next handler

3. **VolumeLogger**: Handles persistent storage to JSON files
   - Writes Warning, Error and Critical levels to JSON files
   - Creates one file per job run
   - End of the chain

## VolumeLogger Features

The VolumeLogger provides persistent storage of logs in JSON files with the following capabilities:

### Configuration

```python
from loggers.VolumeLogger import VolumeLogger

# Use default configuration
volume_logger = VolumeLogger()

# Custom configuration
volume_logger = VolumeLogger(
    name="my_app",
    level="DEBUG",
    log_dir="custom_logs",
    batch_size=100
)
```

### File Structure

Each job run creates a unique JSON file with the following structure:

```json
{
  "job_info": {
    "logger_name": "my_app",
    "start_time": "2024-01-15T10:30:45.123456",
    "batch_size": 50,
    "log_directory": "logs",
    "last_update": "2024-01-15T10:35:20.987654",
    "total_logs": 150,
    "end_time": "2024-01-15T10:35:30.123456",
    "status": "completed"
  },
  "logs": [
    {
      "timestamp": "2024-01-15T10:30:45.123456",
      "level": "INFO",
      "message": "Job started",
      "logger": "my_app",
      "line": 25,
      "function": "main",
      "module": "__main__",
      "args": null,
      "kwargs": null,
      "created_at": "2024-01-15T10:30:45.123456"
    }
  ]
}
```

### File Naming Convention

Files are named using the pattern: `{logger_name}_{timestamp}.json`

Example: `my_app_20240115_103045.json`

### Batch Processing

- **Batch Size**: Configurable batch size for writing logs (default: 50)
- **Automatic Flushing**: Flushes when buffer is full
- **Manual Flushing**: Call `flush()` to force write remaining logs

### Performance Features

- **Buffered Writing**: Reduces I/O operations by batching log writes
- **File Management**: Automatic directory creation and file handling
- **Job Completion**: Proper job finalization with `close_job()` method

## Usage Examples

### Basic Usage

```python
from loggers.LoggerBuilder import LoggerBuilder

# Get logger with full chain
logger = LoggerBuilder.get_logger()

# Log messages (automatically handled by appropriate loggers)
logger.info("Application started")
logger.warning("Resource usage high")
logger.error("Database connection failed")
logger.critical("System shutdown required")
```

### Custom VolumeLogger Configuration

```python
from loggers.VolumeLogger import VolumeLogger

# Create custom VolumeLogger
volume_logger = VolumeLogger(
    name="custom_app",
    level="DEBUG",
    log_dir="custom_logs",
    batch_size=100
)

# Use in your application
volume_logger.info("Custom logging message")
volume_logger.flush()  # Force flush remaining logs
volume_logger.close_job()  # Finalize the job
```

### Job Lifecycle Management

```python
# Start of job
logger.info("Job started")

# During job execution
logger.info("Processing data...")
logger.warning("High memory usage detected")

# End of job
logger.info("Job completed")

# Get VolumeLogger instance and finalize
volume_logger = get_volume_logger_from_chain(logger)
volume_logger.close_job()
```

## File Management

### Directory Structure

```
logs/
├── ETL_Pipeline_{entity}_20240115_103045.json
├── ETL_Pipeline_{entity}_app_20240115_143022.json
├── ETL_Pipeline_{entity}_app_20240116_091234.json
└── ...
```

### File Information

```python
# Get information about the log file
file_info = volume_logger.get_file_info()
print(f"File: {file_info['filepath']}")
print(f"Size: {file_info['file_size']} bytes")
print(f"Buffer: {file_info['buffer_size']} records")
```

### Job Finalization

```python
# Properly close the job
volume_logger.close_job()

# This will:
# 1. Flush any remaining logs in buffer
# 2. Update job_info with end_time and status
# 3. Finalize the JSON file
```

## Integration Patterns

### With ETL Jobs

```python
def run_etl_job():
    logger = LoggerBuilder.get_logger()
    
    try:
        logger.info("ETL job started")
        # ... ETL processing ...
        logger.info("ETL job completed successfully")
    except Exception as e:
        logger.error(f"ETL job failed: {e}")
        raise
    finally:
        # Ensure job is properly finalized
        volume_logger = get_volume_logger_from_chain(logger)
        volume_logger.close_job()
```

### With Web Applications

```python
def handle_request():
    logger = LoggerBuilder.get_logger()
    
    logger.info("Request received")
    # ... process request ...
    logger.info("Request completed")
    
    # Request logs are automatically written to file
```

## Monitoring and Maintenance

### File Monitoring

```python
# Check log file status
file_info = volume_logger.get_file_info()
if file_info['file_size'] > 1000000:  # 1MB
    print("Log file is getting large")
```

### Log Rotation

```python
# Implement log rotation based on file size or age
import os
from datetime import datetime, timedelta

def cleanup_old_logs(log_dir, max_age_days=30):
    cutoff_date = datetime.now() - timedelta(days=max_age_days)
    
    for filename in os.listdir(log_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(log_dir, filename)
            file_time = datetime.fromtimestamp(os.path.getctime(filepath))
            
            if file_time < cutoff_date:
                os.remove(filepath)
                print(f"Removed old log file: {filename}")
```

## Error Handling

The VolumeLogger includes robust error handling:

- **Graceful Degradation**: If file operations fail, logs are kept in buffer
- **Directory Creation**: Automatically creates log directories if they don't exist
- **File Recovery**: Recreates files if they're accidentally deleted
- **Non-blocking**: Logger failures don't prevent application operation

## Future Enhancements

- **Compression**: Automatic log file compression for old files
- **Log Aggregation**: Centralized log collection from multiple instances
- **Metrics Dashboard**: Real-time logging metrics and file statistics
- **Cloud Storage**: Integration with S3, Azure Blob, or GCS
- **Search Indexing**: Full-text search across log files
