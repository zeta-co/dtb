from typing import Any, List, Union
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from .base import Expectation
from ..validation_result import ValidationResult


class ColumnExpectation(Expectation):
    """Base class for column-based expectations."""

    def __init__(self, column_name: str):
        super().__init__()
        self.column_name = column_name

    @property
    def value_column(self) -> str:
        return self.column_name

    def validate(self, df: DataFrame) -> ValidationResult:
        """Implement abstract method - this base class shouldn't be used directly"""
        raise NotImplementedError("ColumnExpectation is an abstract base class. Use a specific column expectation instead.")

    def gt(self, value: Any) -> 'ColumnGreaterThanExpectation':
        """Greater than comparison"""
        return ColumnGreaterThanExpectation(self.column_name, value)
    
    def lt(self, value: Any) -> 'ColumnLessThanExpectation':
        """Less than comparison"""
        return ColumnLessThanExpectation(self.column_name, value)
    
    def eq(self, value: Any) -> 'ColumnEqualsValueExpectation':
        """Equals comparison"""
        return ColumnEqualsValueExpectation(self.column_name, value)
    
    def is_in(self, values: Union[List, set]) -> 'ColumnInExpectation':
        """Check if value is in a set of values"""
        return ColumnInExpectation(self.column_name, values)
    
    def is_null(self) -> 'ColumnIsNullExpectation':
        """Check if value is null"""
        return ColumnIsNullExpectation(self.column_name)
    
    def is_not_null(self) -> 'ColumnIsNotNullExpectation':
        """Check if value is not null"""
        return ColumnIsNotNullExpectation(self.column_name)

class ColumnGreaterThanExpectation(ColumnExpectation):
    def __init__(self, column_name: str, value: Any):
        super().__init__(column_name)
        self.value = value
    
    @property
    def value_column(self) -> str:
        return self.column_name
    
    def validate(self, df: DataFrame) -> ValidationResult:
        return ValidationResult(
            expectation_id=self.id,
            expectation_type=self.type,
            df=df.withColumn(
                self.flag_column,
                F.col(self.column_name) > self.value
            )
        )

class ColumnLessThanExpectation(ColumnExpectation):
    def __init__(self, column_name: str, value: Any):
        super().__init__(column_name)
        self.value = value
    
    def validate(self, df: DataFrame) -> ValidationResult:
        return ValidationResult(
            expectation_id=self.id,
            expectation_type=self.type,
            df=df.withColumn(
                self.flag_column,
                F.col(self.column_name) < self.value
            )
        )

class ColumnEqualsValueExpectation(ColumnExpectation):
    def __init__(self, column_name: str, value: Any):
        super().__init__(column_name)
        self.value = value
    
    def validate(self, df: DataFrame) -> ValidationResult:
        return ValidationResult(
            expectation_id=self.id,
            expectation_type=self.type,
            df=df.withColumn(
                self.flag_column,
                F.col(self.column_name) == self.value
            )
        )

class ColumnInExpectation(ColumnExpectation):
    def __init__(self, column_name: str, values: Union[List, set]):
        super().__init__(column_name)
        self.values = values if isinstance(values, set) else set(values)
    
    def validate(self, df: DataFrame) -> ValidationResult:
        return ValidationResult(
            expectation_id=self.id,
            expectation_type=self.type,
            df=df.withColumn(
                self.flag_column,
                F.col(self.column_name).isin(list(self.values))
            )
        )

class ColumnIsNullExpectation(ColumnExpectation):
    def validate(self, df: DataFrame) -> ValidationResult:
        return ValidationResult(
            expectation_id=self.id,
            expectation_type=self.type,
            df=df.withColumn(
                self.flag_column,
                F.col(self.column_name).isNull()
            )
        )

class ColumnIsNotNullExpectation(ColumnExpectation):
    def validate(self, df: DataFrame) -> ValidationResult:
        return ValidationResult(
            expectation_id=self.id,
            expectation_type=self.type,
            df=df.withColumn(
                self.flag_column,
                F.col(self.column_name).isNotNull()
            )
        )
