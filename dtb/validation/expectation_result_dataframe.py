from dataclasses import dataclass
from typing import Optional
from .expectation_result import ExpectationResult


@dataclass
class DataframeExpectationResult(ExpectationResult):

    flag_column: str
    value_column: Optional[str] = None
    message: Optional[str] = None
