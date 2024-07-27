from datetime import datetime


class RollbackDetail:
    def __init__(
        self,
        catalog: str,
        schema: str,
        table: str,
        action: str,
        from_version: str,
        to_version: str,
        rollback_timestamp: datetime,
        success: bool,
        error_message: str = None,
    ):
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.action = action
        self.from_version = from_version
        self.to_version = to_version
        self.rollback_timestamp = rollback_timestamp
        self.success = success
        self.error_message = error_message
