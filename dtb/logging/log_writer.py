from abc import ABC, abstractmethod
from typing import List
from .log_entry import LogEntry


class LogWriter(ABC):
    """
    Abstract base class for log writers.

    Log writers are responsible for writing log entries to specific targets
    (e.g., Delta tables, files, etc.). All concrete writer implementations
    should inherit from this class.
    """

    @abstractmethod
    def write(self, log_entries: List[LogEntry]) -> None:
        """
        Write log entries to the target.

        Args:
            log_entries: List of LogEntry instances to write

        Raises:
            ValueError: If log_entries is empty or contains invalid entries
        """
        pass
