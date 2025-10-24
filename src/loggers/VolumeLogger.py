import logging
import json
import os
import inspect
from datetime import datetime
from typing import Optional, Any, Dict, List
from loggers.ILogger import ILogger


class VolumeLogger(ILogger):
    """
    Logger implementation for writing logs to JSON files
    Creates one file per job run for volume-based logging
    Uses Chain of Responsibility pattern to handle log persistence
    """

    def __init__(self, name: str = "default", level: str = "INFO", 
                 log_dir: str = "logs", batch_size: int = 50, **kwargs):
        
        self.name = name
        self.level = level.upper()
        
        # File logging configuration
        self.log_dir = log_dir
        self.batch_size = batch_size
        self.log_buffer = []
        self.last_flush_time = datetime.now()
        
        # Create log directory if it doesn't exist
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Generate unique filename for this job run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.filename = f"{name}_{timestamp}.json"
        self.filepath = os.path.join(self.log_dir, self.filename)
        
        # Create logger instance for internal logging
        self.logger = logging.getLogger(f"{name}_volume")
        self.logger.setLevel(self._get_logging_level(level))
        
        # Prevent propagation to root logger
        self.logger.propagate = False
        
        # Initialize the log file
        self._initialize_log_file()
    
    def _initialize_log_file(self):
        """Initialize the JSON log file with header"""
        try:
            # Create file with initial structure
            initial_data = {
                "job_info": {
                    "logger_name": self.name,
                    "start_time": datetime.now().isoformat(),
                    "batch_size": self.batch_size,
                    "log_directory": self.log_dir
                },
                "logs": []
            }
            
            with open(self.filepath, 'w', encoding='utf-8') as f:
                json.dump(initial_data, f, indent=2, ensure_ascii=False)
                
            print(f"VolumeLogger initialized: {self.filepath}")
            
        except Exception as e:
            print(f"Warning: Could not initialize log file: {e}")
    
    def _get_caller_info(self) -> Dict[str, Any]:
        """Get information about the calling function and module"""
        try:
            # Get the caller's frame (skip this method and the logging method)
            frame = inspect.currentframe().f_back.f_back
            if frame:
                return {
                    'function': frame.f_code.co_name,
                    'module': frame.f_globals.get('__name__', 'unknown'),
                    'line': frame.f_lineno
                }
        except Exception:
            pass
        
        return {
            'function': 'unknown',
            'module': 'unknown',
            'line': 0
        }
    
    def _add_to_buffer(self, level: str, message: str, *args, **kwargs):
        """Add log record to buffer for batch writing"""
        caller_info = self._get_caller_info()
        current_time = datetime.now()
        
        log_record = {
            'timestamp': current_time.isoformat(),
            'level': level,
            'message': message,
            'logger': self.name,
            'line': caller_info['line'],
            'function': caller_info['function'],
            'module': caller_info['module'],
            'args': args if args else None,
            'kwargs': kwargs if kwargs else None,
            'created_at': current_time.isoformat()
        }
        
        self.log_buffer.append(log_record)
        
        # Write to file if buffer is full
        if len(self.log_buffer) >= self.batch_size:
            self._flush_buffer()
    
    def _flush_buffer(self):
        """Flush the log buffer to the JSON file"""
        if not self.log_buffer:
            return
            
        try:
            self._write_to_json_file(self.log_buffer)
            self.log_buffer.clear()
            self.last_flush_time = datetime.now()
        except Exception as e:
            # If writing fails, log the error and keep the buffer
            print(f"Error writing logs to JSON file: {e}")
    
    def _write_to_json_file(self, log_records: List[Dict[str, Any]]):
        """Write log records to JSON file"""
        try:
            # Read existing file content
            if os.path.exists(self.filepath):
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    file_data = json.load(f)
            else:
                # Recreate file if it was deleted
                self._initialize_log_file()
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    file_data = json.load(f)
            
            # Append new log records
            file_data['logs'].extend(log_records)
            
            # Update job info
            file_data['job_info']['last_update'] = datetime.now().isoformat()
            file_data['job_info']['total_logs'] = len(file_data['logs'])
            
            # Write back to file
            with open(self.filepath, 'w', encoding='utf-8') as f:
                json.dump(file_data, f, indent=2, ensure_ascii=False)
            
            print(f"Wrote {len(log_records)} log records to {self.filepath}")
            
        except Exception as e:
            raise Exception(f"Failed to write to JSON file {self.filepath}: {e}")
    
    def debug(self, message: str, *args, **kwargs) -> None:
        """Log debug message - pass to next handler only"""
        """Log debug message to JSON file"""
        self._add_to_buffer("DEBUG", message, *args, **kwargs)
        # Pass to next handler in chain
        self.handle(message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs) -> None:
        """Log info message - pass to next handler only"""
        """Log info message to JSON file"""
        self._add_to_buffer("INFO", message, *args, **kwargs)
        # Pass to next handler in chain
        self.handle(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs) -> None:
        """Log warning message to JSON file"""
        self._add_to_buffer("WARNING", message, *args, **kwargs)
        # Pass to next handler in chain
        self.handle(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs) -> None:
        """Log error message to JSON file"""
        self._add_to_buffer("ERROR", message, *args, **kwargs)
        # Pass to next handler in chain
        self.handle(message, *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs) -> None:
        """Log critical message to JSON file"""
        self._add_to_buffer("CRITICAL", message, *args, **kwargs)
        # Pass to next handler in chain
        self.handle(message, *args, **kwargs)
    
    def set_level(self, level: str) -> None:
        """Set the logging level"""
        logging_level = self._get_logging_level(level)
        self.logger.setLevel(logging_level)
        self.level = level.upper()
    
    def get_level(self) -> str:
        """Get the current logging level"""
        return self.level
    
    def handle(self, request: Any, *args, **kwargs):
        """Handle the request and pass to next handler in chain"""
        if hasattr(self, '_next_handler') and self._next_handler:
            return self._next_handler.handle(request, *args, **kwargs)
        return None
    
    def flush(self):
        """Force flush of any remaining log records in buffer"""
        self._flush_buffer()
    
    def get_file_info(self) -> Dict[str, Any]:
        """Get information about the log file"""
        return {
            'filepath': self.filepath,
            'filename': self.filename,
            'log_dir': self.log_dir,
            'batch_size': self.batch_size,
            'buffer_size': len(self.log_buffer),
            'last_flush': self.last_flush_time.isoformat(),
            'file_size': os.path.getsize(self.filepath) if os.path.exists(self.filepath) else 0
        }
    
    def close_job(self):
        """Close the job and finalize the log file"""
        try:
            # Flush any remaining logs
            self.flush()
            
            # Update job info with completion
            if os.path.exists(self.filepath):
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    file_data = json.load(f)
                
                file_data['job_info']['end_time'] = datetime.now().isoformat()
                file_data['job_info']['status'] = 'completed'
                file_data['job_info']['total_logs'] = len(file_data['logs'])
                
                with open(self.filepath, 'w', encoding='utf-8') as f:
                    json.dump(file_data, f, indent=2, ensure_ascii=False)
                
                print(f"Job completed and log file finalized: {self.filepath}")
            
        except Exception as e:
            print(f"Error finalizing log file: {e}")
    
    def __del__(self):
        """Destructor to ensure logs are flushed"""
        try:
            self.flush()
        except Exception:
            pass  # Ignore errors during cleanup
