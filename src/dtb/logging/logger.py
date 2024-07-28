from typing import Dict, Any, List

from dtb.logging.log_entry import LogEntry


class Logger:
    def __init__(self, output_handler: OutputHandler):
        self.output_handler = output_handler

    def log(self, log_entry: LogEntry) -> None:
        self.output_handler.write(log_entry)

    def get_logs(self, filters: Dict[str, Any] = None) -> List[LogEntry]:
        return self.output_handler.read(filters)