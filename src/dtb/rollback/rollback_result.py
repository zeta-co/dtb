from datetime import datetime
from typing import List
from dtb.rollback.rollback_detail import RollbackDetail


class RollbackResult:
    def __init__(self, details: List[RollbackDetail], user: str):
        self.details = details
        self.user = user
        self.timestamp = datetime.now()
        self.success_count = sum(1 for detail in details if detail.success)
        self.failure_count = len(details) - self.success_count

    def __str__(self):
        return (
            f"Rollback Operation:\n"
            f"User: {self.user}\n"
            f"Timestamp: {self.timestamp}\n"
            f"Successful Rollbacks: {self.success_count}\n"
            f"Failed Rollbacks: {self.failure_count}\n"
            f"Affected Tables:\n"
            + "\n".join(
                [
                    f"  - {detail.catalog}.{detail.schema}.{detail.table}: "
                    f"{'SUCCESS' if detail.success else 'FAILED'} "
                    f"{'(from ' + detail.from_version + ' to ' + detail.to_version + ')' if detail.success else ''}"
                    f"{'Error: ' + detail.error_message if not detail.success else ''}"
                    for detail in self.details
                ]
            )
        )
