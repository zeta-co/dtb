from abc import ABC, abstractmethod
from typing import List
from .log_entry import LogEntry


class LogWriter(ABC):

    @abstractmethod
    def write(self, log_entries: List[LogEntry]) -> None:
        pass
