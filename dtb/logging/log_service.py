from typing import List
from .log_entry import LogEntry
from .log_writer import LogWriter


class LogService:

    """
    Main logging system that coordinates log entries and writers.

    This service manages the buffering and writing of log entries to various targets
    through configured writers. It supports batching of log entries for improved
    performance.

    Attributes:
        writers: List of configured log writers
        buffer: Buffer of log entries waiting to be written
    """

    def __init__(self):
        self.writers: List[LogWriter] = []
        self.buffer: List[LogEntry] = []

    def add_writer(self, writer: LogWriter) -> None:
        """
        Add a new log writer to the service.

        Args:
            writer: LogWriter instance to add
        """
        self.writers.append(writer)

    def add_log_entry(self, entry: LogEntry) -> None:
        """
        Add a new log entry to the buffer.

        Args:
            entry: LogEntry instance to add

        Raises:
            ValueError: If entry is not a LogEntry instance
        """
        if not isinstance(entry, LogEntry):
            raise ValueError("Entry must be an instance of LogEntry")

        self.buffer.append(entry)

    def flush(self) -> None:
        """
        Flush all buffered log entries to configured writers.

        Raises:
            RuntimeError: If no writers are configured
        """
        if not self.writers:
            raise RuntimeError("No log writers configured")

        if not self.buffer:
            return

        for writer in self.writers:
            writer.write(self.buffer)

        self.buffer = []
