import datetime
from dataclasses import dataclass, field
from typing import List, Optional, Union
from ...validation.check_log_entry import CheckLogEntry


@dataclass
class ProcessingResult:
    """Represents the result of processing a single batch."""

    dataset: str
    filter: str
    datetime: datetime.datetime
    success: bool
    total_count: int = 0
    failed_count: int = 0
    threshold: Union[int, float] = 0.0
    check_log_entries: List[CheckLogEntry] = field(default_factory=list)
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None

    def __post_init__(self):
        if not isinstance(self.datetime, datetime.datetime):
            raise TypeError(
                f"datetime must be datetime.datetime, not {type(self.datetime)}"
            )
