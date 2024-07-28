
import datetime
from dataclasses import dataclass
from .log_entry import LogEntry


@dataclass(frozen=False)
class DeltaVersionLogEntry(LogEntry):
    JobID: str = None
    RunID: str = None
    Operation: str = None
    RunDatetime: datetime.datetime = None
    TableID: str = None
    TableName: str = None
    TablePath: str = None
    VersionFrom: int = None
    VersionTo: int = None
    VersionDatetime: datetime.datetime = None
