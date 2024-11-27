from dataclasses import dataclass
from typing import Optional
from .validation_result import ValidationResult


@dataclass
class DataframeValidationResult(ValidationResult):

    flag_column: str
    value_column: Optional[str] = None
    message: Optional[str] = None
