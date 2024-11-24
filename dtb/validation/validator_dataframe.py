from pyspark.sql import DataFrame, SparkSession
from .validation_logger import ValidationLogger
from .validation_status import ValidationStatus
from .validator_base import BaseValidator


class DataFrameValidator(BaseValidator):

    def __init__(self, spark: SparkSession, logger: ValidationLogger, df: DataFrame):
        super().__init__(spark, logger)
        self._df = df

    def _determine_status(self, error_count: int, total_count: int) -> ValidationStatus:
        """Determine validation status based on error count and threshold"""
        if error_count == 0:
            return ValidationStatus.SUCCESS

        error_percentage = (error_count / total_count * 100) if total_count > 0 else 100

        if self.threshold:
            if self.threshold.type == "percentage":
                if error_percentage > self.threshold.value:
                    return ValidationStatus.FAILURE
                return ValidationStatus.WARNING
            else:  # absolute threshold
                if error_count > self.threshold.value:
                    return ValidationStatus.FAILURE
                return ValidationStatus.WARNING
        else:
            # If no threshold is set, any errors result in FAILURE
            return ValidationStatus.FAILURE
