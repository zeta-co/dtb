from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class LogContext:
    """
    Holds global context for the logs.
    """
    job_id: str
    job_name: str
    run_id: str
    table_name: str
    table_path: str
    extra: Dict[str, str] = field(default_factory=dict)

    def enrich_log_entry_dict(self, log_entry: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a log entry with context information.
        """
        context_data = {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "run_id": self.run_id,
            "table_name": self.table_name,
            "table_path": self.table_path,
            **self.extra
        }
        return {**context_data, **log_entry}
