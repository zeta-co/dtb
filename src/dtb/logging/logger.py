from typing import Dict, Any, List
from .log_entry import LogEntry
from .output_handler import OutputHandler


class Logger:
    def __init__(self, output_handler: OutputHandler):
        self.output_handler = output_handler

    def write_log_entry(self, log_entry: LogEntry) -> None:
        self.output_handler.write(log_entry)

    def read_log_entries(self, filters: Dict[str, Any] = None) -> List[LogEntry]:
        return self.output_handler.read(filters)
