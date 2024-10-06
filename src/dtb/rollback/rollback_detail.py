import datetime
from dataclasses import dataclass


@dataclass(frozen=False)
class RollbackDetail:
    catalog: str
    schema: str
    table: str
    action: str
    from_version: str
    to_version: str
    rollback_timestamp: datetime.datetime
    success: bool
    error_message: str = None
