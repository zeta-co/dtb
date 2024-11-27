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
    """A wrapper class that combines an Expectation with logging and result processing capabilities.

    The Check class serves as a higher-level abstraction over Expectations, adding the ability to:
    1. Process and validate data against defined expectations
    2. Generate appropriate log entries based on validation results
    3. Track and describe the validation being performed

    Parameters
    ----------
    expectation : Expectation
        The expectation object that defines the actual validation logic to be performed.
    description : str
        A human-readable description of what this check validates.

    Attributes
    ----------
    expectation : Expectation
        The underlying expectation object that performs the actual validation.
    description : str
        Human-readable description of the check's purpose.
    log_entry_builder_registry : CheckLogEntryBuilderRegistry
        Registry containing builders for different types of validation results.

    Properties
    ----------
    id : str
        The unique identifier of the underlying expectation.

    Examples
    --------
    >>> from pyspark.sql import DataFrame
    >>> from .expectation import ColumnExistsExpectation
    >>> 
    >>> # Create a check to validate column existence
    >>> expectation = ColumnExistsExpectation("user_id")
    >>> check = Check(expectation, "Validate user_id column exists")
    >>> 
    >>> # Process a dataframe
    >>> df = spark.createDataFrame([{"user_id": 1}])
    >>> log_context = LogContext(job_id="123")
    >>> result_df, log_entries = check.process_result(df, log_context)

    Notes
    -----
    - The Check class automatically registers handlers for all result types defined
      in result_to_entry_builder_mapping during initialization.
    - Validation results are processed into log entries using the appropriate builder
      from the registry based on the result type.
    - The processed dataframe is always returned along with the log entries, allowing
      for chaining of multiple checks.

    See Also
    --------
    Expectation : Base class for all expectations
    ValidationResult : Container for validation results
    CheckLogEntry : Structured log entry for validation results
    CheckLogEntryBuilderRegistry : Registry for result processors
    """

    def __init__(self, expectation: Expectation, description: str):
        """Initialize a new Check instance.

        Parameters
        ----------
        expectation : Expectation
            The expectation object that defines the validation logic.
        description : str
            Human-readable description of what this check validates.
        """
        self.expectation = expectation
        self.description = description
        self.log_entry_builder_registry = CheckLogEntryBuilderRegistry()
        self.register_handlers()

    @property
    def id(self):
        return self.expectation.id

    def register_handlers(self) -> None:
        """Register all available result handlers in the registry.
        
        This method sets up handlers for each result type defined in
        result_to_entry_builder_mapping. Each handler is responsible for
        converting a specific type of validation result into appropriate
        log entries.
        """
        for result_type, builder in result_to_entry_builder_mapping.items():
            self.log_entry_builder_registry.register(result_type, builder())

    def _validate(self, df: DataFrame) -> ValidationResult:
        """Perform validation using the underlying expectation.

        Parameters
        ----------
        df : DataFrame
            The PySpark DataFrame to validate.

        Returns
        -------
        ValidationResult
            The result of the validation operation.
        """
        return self.expectation.validate(df)

    def process_result(
        self, df: DataFrame, log_context: LogContext
    ) -> Tuple[DataFrame, List[CheckLogEntry]]:
        """Process a DataFrame and generate appropriate log entries.

        This method:
        1. Validates the DataFrame using the underlying expectation
        2. Processes the validation result using the appropriate builder
        3. Returns the processed DataFrame and generated log entries

        Parameters
        ----------
        df : DataFrame
            The PySpark DataFrame to process and validate.
        log_context : LogContext
            Context information for logging.

        Returns
        -------
        Tuple[DataFrame, List[CheckLogEntry]]
            A tuple containing:
            - The processed DataFrame (potentially modified by the validation)
            - A list of generated log entries

        Notes
        -----
        The returned DataFrame may be modified by the validation process,
        allowing for data cleaning or transformation as part of the check.
        """
        validation_result = self._validate(df)
        log_entry_builder = self.log_entry_builder_registry.get(validation_result.type)
        log_entries = log_entry_builder.build(
            self.description, log_context, validation_result
        )
        return validation_result.df, log_entries
