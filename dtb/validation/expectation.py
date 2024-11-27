from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from ..utils.name import generate_random_alphanumeric
from .validation_result import ValidationResult


class Expectation(ABC):

    id: str

    """Base class for all expectations"""

    def __init__(self):
        self.id = generate_random_alphanumeric(12)

    @property
    def flag_column(self) -> str:
        """Name of the column showing if record passed the Expectation"""
        return f"_dtb_check_{self.id}"

    @property
    def type(self) -> str:
        """Return the type of expectation."""
        return self.__class__.__name__

    @abstractmethod
    def value_column(self) -> str:
        pass

    @abstractmethod
    def validate(self, df: DataFrame) -> ValidationResult:
        pass


class TrueExpectation(Expectation):
    """An expectation that always passes."""

    def evaluate(self, df):
        return True


class FalseExpectation(Expectation):
    """An expectation that always fails."""

    def evaluate(self, df):
        return False


class AllExpectation(Expectation):
    def __init__(self, *expectations):
        self.expectations = expectations

    def evaluate(self, df):
        results = [exp.evaluate(df) for exp in self.expectations]
        if all(results):
            return ValidationResult(True)
        else:
            failed_results = [r for r in results if not r.passed]
            failed_count = sum(r.failed_count for r in failed_results)
            failed_records = [
                record for r in failed_results for record in r.failed_records
            ]
            reasons = [r.failure_reason for r in failed_results]
            return ValidationResult(
                False, failed_count, failed_records, "; ".join(reasons)
            )


class AnyExpectation(Expectation):
    """An expectation that passes when any of the sub-expectations pass."""

    def __init__(self, *expectations):
        self.expectations = expectations

    def evaluate(self, df):
        return any(e.evaluate(df) for e in self.expectations)


class NegateExpectation(Expectation):
    """An expectation that passes when the sub-expectation fails."""

    def __init__(self, expectation):
        self.expectation = expectation

    def evaluate(self, df):
        return not self.expectation.evaluate(df)


class ColumnExpectation(Expectation):
    """Base class for column-based expectations."""

    def __init__(self, column_name):
        self.column_name = column_name

    def evaluate(self, df):
        raise NotImplementedError()


class ColumnEqualsExpectation(ColumnExpectation):
    """An expectation that checks if a column value equals another column value."""

    def __init__(self, column_name, other_column_name):
        super().__init__(column_name)
        self.other_column_name = other_column_name

    def evaluate(self, df):
        return df[self.column_name] == df[self.other_column_name]


class ColumnGreaterThanExpectation(ColumnExpectation):
    def __init__(self, column_name, value):
        super().__init__(column_name)
        self.value = value

    def evaluate(self, df):
        condition = df[self.column_name] > self.value
        passed = condition.all()
        if passed:
            return ValidationResult(True)
        else:
            failed_count, failed_records = self.collect_failed_records(df, condition)
            reason = f"Column '{self.column_name}' has values <= {self.value}"
            return ValidationResult(False, failed_count, failed_records, reason)


class GroupByExpectation(Expectation):
    """An expectation that applies sub-expectations to grouped data."""

    def __init__(self, *group_by_columns, sub_expectation):
        self.group_by_columns = group_by_columns
        self.sub_expectation = sub_expectation

    def evaluate(self, df):
        grouped = df.groupBy(*self.group_by_columns)
        return all(
            self.sub_expectation.evaluate(group_df)
            for group_df in grouped.toLocalIterator()
        )


class SchemaExpectation(Expectation):
    """An expectation that checks the schema of the dataframe."""

    def __init__(self, expected_schema):
        self.expected_schema = expected_schema

    def evaluate(self, df):
        return df.schema == self.expected_schema


class WhenExpectation(Expectation):
    """An expectation that applies different sub-expectations based on a condition."""

    def __init__(self, condition, when_true, when_false=None):
        self.condition = condition
        self.when_true = when_true
        self.when_false = when_false or FalseExpectation()

    def evaluate(self, df):
        if self.condition.evaluate(df):
            return self.when_true.evaluate(df)
        else:
            return self.when_false.evaluate(df)
