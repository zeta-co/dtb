from abc import ABC, abstractmethod
from dtb.logging.log_entry import LogEntry


class Tracker(ABC):
    @abstractmethod
    def start(self, entry: LogEntry) -> None:
        pass

    @abstractmethod
    def end(self, entry: LogEntry) -> None:
        pass
