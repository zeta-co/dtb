from dataclasses import dataclass, field
from typing import List
from .validation_result import ValidationResult


@dataclass
class DataframeSchemaValidationResult(ValidationResult):

    passed: bool
    source_columns: List[str]
    expected_columns: List[str]
    missing_columns: List[str] = field(default_factory=list)
    extra_columns: List[str] = field(default_factory=list)
