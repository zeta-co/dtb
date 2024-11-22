import datetime
from typing import Dict, Optional
from pyspark.sql import DataFrame
from .validation_status import ValidationStatus


class ValidationResult:
    def __init__(
        self,
        status: ValidationStatus,
        validator_name: str,
        error_df: Optional[DataFrame] = None,
        error_count: int = 0,
        total_count: int = 0,
        metadata: Dict = None,
        validation_time: datetime.datetime = None,
    ):
        self.status = status
        self.validator_name = validator_name
        self.error_df = error_df
        self.error_count = error_count
        self.total_count = total_count
        self.metadata = metadata or {}
        self.validation_time = validation_time or datetime.datetime.now()

    @property
    def error_percentage(self) -> float:
        return (
            (self.error_count / self.total_count * 100) if self.total_count > 0 else 0
        )
