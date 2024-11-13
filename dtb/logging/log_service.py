from typing import Dict, List, Optional
from pyspark.sql import SparkSession
from .log_entry import LogEntry
from .log_writer import LogWriter


class LogService:

    """Main logging system that coordinates log entries and writers"""
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.writers: List[LogWriter] = []
        self.buffer: List[LogEntry] = []
       
    def add_writer(self, writer: LogWriter):
        """Add a new log writer"""
        self.writers.append(writer)
       
    def add_log_entry(self, entry: LogEntry):
        """Log a new entry"""      
        self.buffer.append(entry)
   
    def flush(self, log_type: Optional[str] = None):
        """Flush logs to all writers"""
        self.buffer = []
