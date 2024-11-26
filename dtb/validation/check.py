from typing import List, Tuple
from pyspark.sql import DataFrame
from .expectation import Expectation
from .validation_result import ValidationResult
from .check_log_entry_builder import result_to_entry_builder_mapping, CheckLogEntry
from .registry import CheckLogEntryBuilderRegistry
from ..logging.log_context import LogContext


class CheckOutput:
    pass


class Check:
    """Wrapper for expectation and calculate stats"""

    def __init__(self, expectation: Expectation, description: str):
        self.expectation = expectation
        self.description = description
        self.log_entry_builder_registry = CheckLogEntryBuilderRegistry()
        self.register_handlers()

    def register_handlers(self) -> None:
        for result_type, builder in result_to_entry_builder_mapping.items():
            self.log_entry_builder_registry.register(result_type, builder())

    def _validate(self, df: DataFrame) -> ValidationResult:
        return self.expectation.validate(df)

    def process_result(
        self, df: DataFrame, log_context: LogContext
    ) -> Tuple[DataFrame, List[CheckLogEntry]]:
        validation_result = self._validate(df)
        log_entry_builder = self.log_entry_builder_registry.get(validation_result.type)
        log_entries = log_entry_builder.build(
            self.description, log_context, validation_result
        )
        return validation_result.df, log_entries
