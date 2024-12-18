from typing import List
from pyspark.sql import SparkSession
from .check_log_entry import CheckLogEntry
from ..logging.log_service import LogService
from ..logging.log_writer import LogWriter
from ..logging.log_delta_table_config import LogDeltaTableConfig
from ..logging.log_writer_delta_table import DeltaTableLogWriter
from ..model.table import Table


class CheckLogger:
    """
    Handles logging of check entries to Delta tables.

    This class provides a simplified interface for logging check entries to Delta tables,
    encapsulating all the configuration and service management internally. It handles
    the setup of the logging infrastructure and provides methods to log both single
    and multiple check entries.

    Attributes:
        spark (SparkSession): The SparkSession used for Delta table operations
        table_name (str): The full name of the target Delta table (schema.table format)
        _log_service (LogService): Internal LogService instance for handling log writes

    Example:
        >>> logger = CheckLogger(spark)
        >>> entry = CheckLogEntry(job_name="test_job", check_id="check1",
                                check_name="test_check", status="SUCCESS")
        >>> logger.log_entry(entry)
    """

    def __init__(self, spark: SparkSession, writer: LogWriter = None):
        """
        Initialise the check logger.

        Args:
            spark: SparkSession to use for logging
            table_name: Full name of the Delta table (schema.table)
        """
        self.spark = spark
        self.writer = writer
        self._log_service = None
        self._initialise_logging(self.writer)

    def _initialise_logging(self, writer: LogWriter = None) -> None:
        """
        Set up the logging infrastructure.

        Initialises the LogDeltaTableConfig, DeltaTableLogWriter, and LogService
        with default partition columns ["JobName", "Date", "CheckId"].

        This is called automatically by the constructor and should not be called directly.
        """
        if not writer:
            config = LogDeltaTableConfig(
                Table("lg.dtb_checks"), partition_columns=["JobName", "Date", "CheckId"]
            )
            writer = DeltaTableLogWriter(
                spark=self.spark, config=config, schema=CheckLogEntry._target_schema
            )

        self._log_service = LogService()
        self._log_service.add_writer(writer)

    def log_entries(self, entries: List[CheckLogEntry]) -> None:
        """
        Log multiple check entries and flush to the Delta table.
        
        Args:
            entries: List of CheckLogEntry objects to log
        
        Raises:
            ValueError: If entries is None or empty
            RuntimeError: If logging fails due to Delta table write issues
        """
        if not entries:
            raise ValueError("entries cannot be None or empty")
            
        try:
            for entry in entries:
                self._log_service.add_log_entry(entry)
            self._log_service.flush()
        except Exception as e:
            raise RuntimeError(f"Failed to log entries: {str(e)}") from e

    def log_entry(self, entry: CheckLogEntry) -> None:
        """
        Log a single check entry and flush to the Delta table.
        
        This is a convenience method that wraps log_entries for single entries.
        
        Args:
            entry: CheckLogEntry object to log
        
        Raises:
            ValueError: If entry is None
            RuntimeError: If logging fails due to Delta table write issues
        """
        if entry is None:
            raise ValueError("entry cannot be None")
            
        self.log_entries([entry])
