from abc import ABC, abstractmethod
from typing import Dict, Any, List
from .log_entry import LogEntry


class OutputHandler(ABC):
    @abstractmethod
    def write(self, entry: LogEntry) -> None:
        pass

    @abstractmethod
    def read(self, filters: Dict[str, Any] = None) -> List[LogEntry]:
        pass
