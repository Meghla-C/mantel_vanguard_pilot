import os
from pyspark.sql import SparkSession
from typing import Dict
from loggers.ILogger import ILogger
from loggers.STOutLogger import STOutLogger
from loggers.STErrorLogger import STErrorLogger
from loggers.VolumeLogger import VolumeLogger
from loggers.SparkStreamingLogger import SparkStreamingLogger

class LoggerBuilder:
    """
    LoggerBuilder class to create the appropriate Logger object
    Uses Chain of Responsibility pattern for different log levels
    """

    @staticmethod
    def get_logger(spark: SparkSession, **kwargs) -> ILogger:
        """
        Method to create the logger object with Chain of Responsibility pattern

        Outputs:
            - ILogger, An instance of the Logger class with error handling chain
        """
        log_dir = kwargs.get('log_dir')
        log_batch_size = kwargs.get('log_batch_size', 50)
        name = kwargs.get('name', 'default')
        level = kwargs.get('level', 'INFO')

        # Check if log_dir is provided and directory exists
        use_volume_logger = False

        # make sure log dir has at least 4 levels of depth (Volumes/catalog/schema/volume_name)
        if os.path.exists(log_dir) and len(log_dir.strip("/").split("/")) > 3:
            use_volume_logger = True
            print(f"Directory Exists, VolumeLogger enabled: {log_dir}")
        else:
            print(f"log_dir '{log_dir}' does not exist or is not a valid volume path. Not using VolumeLogger")

        # handle debug, info, and warning logging
        stoutLogger = STOutLogger(name=name, level=level)
        # handle error and critical logging
        stErrorLogger = STErrorLogger(name=name, level=level)

        if use_volume_logger:
            # handle volume logging to JSON files
            volumeLogger = VolumeLogger(name=name, level=level, log_dir=log_dir, batch_size=log_batch_size)

        # Set up the chain: STOutLogger -> STErrorLogger -> VolumeLogger (if enabled)
        stoutLogger.set_next(stErrorLogger)

        if use_volume_logger:
            stErrorLogger.set_next(volumeLogger)

        spark.streams.addListener(SparkStreamingLogger(stoutLogger))

        return stoutLogger
