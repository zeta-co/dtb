from typing import Sequence
from pyspark.sql import DataFrame, functions as F
from ..expectation import Expectation
from ..validation_result import ValidationResult

class AllExpectation(Expectation):
    def __init__(self, *expectations: Sequence[Expectation]):
        super().__init__()
        self.expectations = expectations

    @property
    def value_column(self) -> str:
        return self.expectations[0].value_column if self.expectations else None

    def validate(self, df: DataFrame) -> ValidationResult:
        df_with_flags = df
        combined_flags = F.lit(True)
        
        for exp in self.expectations:
            result = exp.validate(df_with_flags)
            df_with_flags = result.df
            combined_flags = combined_flags & F.col(exp.flag_column)
        
        return ValidationResult(
            expectation_id=self.id,
            expectation_type=self.type,
            df=df_with_flags.withColumn(self.flag_column, combined_flags)
        )

class AnyExpectation(Expectation):
    def __init__(self, *expectations: Sequence[Expectation]):
        super().__init__()
        self.expectations = expectations

    @property
    def value_column(self) -> str:
        return self.expectations[0].value_column if self.expectations else None

    def validate(self, df: DataFrame) -> ValidationResult:
        df_with_flags = df
        combined_flags = F.lit(False)
        
        for exp in self.expectations:
            result = exp.validate(df_with_flags)
            df_with_flags = result.df
            combined_flags = combined_flags | F.col(exp.flag_column)
        
        return ValidationResult(
            expectation_id=self.id,
            expectation_type=self.type,
            df=df_with_flags.withColumn(self.flag_column, combined_flags)
        )

class NegateExpectation(Expectation):
    def __init__(self, expectation: Expectation):
        super().__init__()
        self.expectation = expectation

    @property
    def value_column(self) -> str:
        return self.expectation.value_column

    def validate(self, df: DataFrame) -> ValidationResult:
        result = self.expectation.validate(df)
        return ValidationResult(
            expectation_id=self.id,
            expectation_type=self.type,
            df=result.df.withColumn(self.flag_column, ~F.col(self.expectation.flag_column))
        )
