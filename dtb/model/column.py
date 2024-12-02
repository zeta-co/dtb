from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


@dataclass
class Column:
    """A dataclass representing metadata and validation rules for a data column.

    This class provides a structured way to define column specifications including data types,
    constraints, and validation rules. It supports conversion to Spark data types and includes
    comprehensive validation of type-specific attributes.

    Attributes:
        name (str): The name of the column.
        data_type (str): The data type of the column. Must be one of:
            'boolean', 'date', 'timestamp', 'decimal', 'double', 'integer', 'long', 'string'.
        nullable (Optional[bool]): Whether the column can contain null values. Defaults to True.
        description (Optional[str]): Human-readable description of the column.
        datetime_format (Optional[str]): Format string for date/timestamp types (e.g., "yyyy-MM-dd").
        min_value (Optional[Any]): Minimum allowed value for numeric types.
        max_value (Optional[Any]): Maximum allowed value for numeric types.
        valid_values (Optional[List[Any]]): List of allowed values for the column.
        precision (Optional[int]): Total number of digits for decimal type.
        scale (Optional[int]): Number of digits after decimal point for decimal type.
        regex_pattern (Optional[str]): Regular expression pattern for string validation.
        is_primary_key (Optional[bool]): Whether the column is a primary key. Defaults to False.
        is_unique (Optional[bool]): Whether values must be unique. Defaults to False.
        metadata (Optional[Dict[str, Any]]): Additional metadata as key-value pairs.

    Methods:
        to_spark_type(): Converts the column definition to a PySpark data type.
        to_dict(): Converts the column metadata to a dictionary format.

    Raises:
        ValueError: If data_type is invalid or type-specific attributes are incorrectly specified.

    Examples:
        >>> # Create a simple string column
        >>> string_col = Column(
        ...     name="user_id",
        ...     data_type="string",
        ...     nullable=False,
        ...     is_primary_key=True
        ... )

        >>> # Create a decimal column with precision and scale
        >>> amount_col = Column(
        ...     name="amount",
        ...     data_type="decimal",
        ...     precision=10,
        ...     scale=2,
        ...     min_value=0,
        ...     description="Transaction amount in dollars"
        ... )

        >>> # Create a date column with format
        >>> date_col = Column(
        ...     name="transaction_date",
        ...     data_type="date",
        ...     datetime_format="yyyy-MM-dd",
        ...     nullable=False
        ... )

        >>> # Create a string column with regex validation
        >>> email_col = Column(
        ...     name="email",
        ...     data_type="string",
        ...     regex_pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        ...     description="User email address"
        ... )

    Notes:
        - The class performs validation during initialisation to ensure type-specific
          attributes are correctly specified (e.g., precision for decimal type).
        - When specifying min_value and max_value for numeric types, the values must
          be numeric and max_value must be greater than min_value.
        - The regex_pattern attribute is only valid for string type columns.
        - The datetime_format attribute is only valid for date and timestamp types.
        - For decimal type, both precision and scale must be specified, and scale
          must be between 0 and precision.
    """

    name: str
    data_type: str
    nullable: Optional[bool] = True
    description: Optional[str] = None
    datetime_format: Optional[str] = None  # For date/time/number formats
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    valid_values: Optional[List[Any]] = None
    precision: Optional[int] = None  # For decimal/number types
    scale: Optional[int] = None  # For decimal types
    regex_pattern: Optional[str] = None  # For string validation
    is_primary_key: Optional[bool] = False
    is_unique: Optional[bool] = False
    metadata: Optional[Dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self):
        self.type_mapping = {
            "boolean": BooleanType,
            "date": DateType,
            "timestamp": TimestampType,
            "decimal": DecimalType,
            "double": DoubleType,
            "integer": IntegerType,
            "long": LongType,
            "string": StringType,
        }
        if not isinstance(self.data_type, str):
            raise ValueError(f"data_type must be in string!")
        if not self.type_mapping.get(self.data_type, None):
            raise ValueError(f"Unsupported data type: {self.data_type}")

        self._validate_type_attributes()

    def _validate_type_attributes(self):
        """Validate that type-specific attributes are provided correctly."""
        # Decimal type requires precision
        if self.data_type == "decimal":
            if self.precision is None:
                raise ValueError("Decimal type requires precision specification")
            if self.precision <= 0:
                raise ValueError("Decimal precision must be positive")
            if self.scale is None:
                raise ValueError("Decimal type requires scale specification")
            if self.scale is not None and (
                self.scale < 0 or self.scale > self.precision
            ):
                raise ValueError("Decimal scale must be between 0 and precision")

        # datetime_format should only be present for date/timestamp types
        if self.datetime_format is not None and self.data_type not in [
            "date",
            "timestamp",
        ]:
            raise ValueError(
                f"datetime_format not applicable for type {self.data_type}"
            )

        # min/max values should be numeric for numeric types
        numeric_types = {"integer", "long", "double", "decimal"}
        if self.data_type in numeric_types:
            if self.min_value is not None and not isinstance(
                self.min_value, (int, float)
            ):
                raise ValueError(f"min_value for {self.data_type} must be numeric")
            if self.max_value is not None and not isinstance(
                self.max_value, (int, float)
            ):
                raise ValueError(f"max_value for {self.data_type} must be numeric")
            if self.min_value is not None and self.max_value is not None:
                if self.max_value < self.min_value:
                    raise ValueError("max_value cannot be less than min_value")

        # regex_pattern only applies to string type
        if self.regex_pattern is not None and self.data_type != "string":
            raise ValueError("regex_pattern only applicable for string type")

        # valid_values type should match the data_type
        if self.valid_values is not None:
            if self.data_type == "boolean":
                if not all(isinstance(v, bool) for v in self.valid_values):
                    raise ValueError("valid_values for boolean must be boolean")
            elif self.data_type in {"integer", "long"}:
                if not all(isinstance(v, int) for v in self.valid_values):
                    raise ValueError(
                        f"valid_values for {self.data_type} must be integers"
                    )
            elif self.data_type in {"double", "decimal"}:
                if not all(isinstance(v, (int, float)) for v in self.valid_values):
                    raise ValueError(
                        f"valid_values for {self.data_type} must be numeric"
                    )

    def to_spark_type(self):
        """Convert to PySpark data type."""
        spark_type = self.type_mapping[self.data_type]

        # Handle special cases
        if self.data_type == "decimal":
            scale = 0 if self.scale is None else self.scale
            return spark_type(self.precision, scale)

        return spark_type()

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary format."""
        result = {
            "type": self.data_type,
            "nullable": self.nullable,
        }
        for attr in [
            "description",
            "datetime_format",
            "min_value",
            "max_value",
            "valid_values",
            "precision",
            "scale",
            "regex_pattern",
            "is_primary_key",
            "is_unique",
        ]:
            result[attr] = getattr(self, attr)
        return result
