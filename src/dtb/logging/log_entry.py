import datetime
from abc import ABC
from dataclasses import dataclass


@dataclass(frozen=False)
class LogEntry(ABC):
    pass


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
