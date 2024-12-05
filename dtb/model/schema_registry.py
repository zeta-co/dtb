import bisect
import datetime
from typing import Any, Dict, List, Optional
from .schema_version import SchemaVersion


class SchemaRegistry:
    """A registry that manages versioned database schemas with temporal validity.

    The SchemaRegistry maintains a chronologically ordered collection of schema versions,
    providing capabilities to track schema evolution over time. It ensures there are no
    overlapping validity periods between versions and offers methods to look up the
    active schema for any given date.

    Attributes:
        _versions (List[SchemaVersion]): Internal list of schema versions ordered by start date.

    Methods:
        add_schema_version(start_date, columns, end_date=None):
            Adds a new schema version to the registry.

        get_schema_for_date(date):
            Retrieves the schema that was active at a given date.

        print_timeline():
            Prints a human-readable timeline of all schema versions.

        load_from_list(schemas):
            Loads multiple schema versions from a list of dictionaries.

    Examples:
        >>> registry = SchemaRegistry()

        >>> # Add an initial schema
        >>> registry.add_schema_version(
        ...     start_date=datetime(2024, 1, 1),
        ...     columns={
        ...         "id": "string",
        ...         "name": {
        ...             "data_type": "string",
        ...             "nullable": False
        ...         }
        ...     },
        ...     end_date=datetime(2024, 6, 30)
        ... )

        >>> # Add a new version with an additional column
        >>> registry.add_schema_version(
        ...     start_date=datetime(2024, 7, 1),
        ...     columns={
        ...         "id": "string",
        ...         "name": {
        ...             "data_type": "string",
        ...             "nullable": False
        ...         },
        ...         "email": "string"  # New column
        ...     }
        ... )

        >>> # Look up schema for a specific date
        >>> schema = registry.get_schema_for_date(datetime(2024, 3, 15))

        >>> # Load multiple versions at once
        >>> registry.load_from_list([
        ...     {
        ...         "start_date": datetime(2024, 1, 1),
        ...         "end_date": datetime(2024, 6, 30),
        ...         "columns": {"id": "string", "name": "string"}
        ...     },
        ...     {
        ...         "start_date": datetime(2024, 7, 1),
        ...         "columns": {"id": "string", "name": "string", "email": "string"}
        ...     }
        ... ])

        >>> # Print timeline for debugging
        >>> registry.print_timeline()
        # Output:
        # Schema V1: 2024-01-01 to 2024-06-30
        # Columns:
        #   - id: string
        #   - name: string
        #
        # Schema V2: 2024-07-01 to PRESENT
        # Columns:
        #   - id: string
        #   - name: string
        #   - email: string

    Notes:
        - Schema versions are automatically ordered chronologically
        - Each schema must have a start_date and may have an end_date
        - A None end_date indicates the schema version is currently active
        - Schema versions cannot have overlapping validity periods
        - Version numbers are assigned sequentially based on chronological order
        - When looking up schemas by date, returns None if no schema was active

    Raises:
        ValueError: If attempting to add schema versions with overlapping dates
        ValueError: If column specifications are invalid
    """

    def __init__(self):
        self._versions: List[SchemaVersion] = []

    def add_schema_version(
        self,
        start_date: datetime.datetime,
        columns: Dict[str, str],
        end_date: Optional[datetime.datetime] = None,
    ) -> None:
        """Add a new schema version."""
        version = len(self._versions) + 1
        schema_version = SchemaVersion(
            start_date=start_date, end_date=end_date, columns=columns, version=version
        )
        # Insert maintaining chronological order
        insert_pos = bisect.bisect_right(
            self._versions, schema_version.start_date, key=lambda x: x.start_date
        )
        self._versions.insert(insert_pos, schema_version)
        self._validate_timeline()

    def _validate_timeline(self) -> None:
        """Ensure schema versions don't overlap."""
        for i in range(len(self._versions) - 1):
            current = self._versions[i]
            next_version = self._versions[i + 1]
            if current.end_date is None or current.end_date > next_version.start_date:
                raise ValueError(
                    f"Schema version {current.version} overlaps with version "
                    f"{next_version.version}"
                )

    def get_schema_version_for_date(self, date: datetime.datetime) -> Optional[SchemaVersion]:
        """Get schema version that was active at the given date."""
        for version in self._versions:
            if version.start_date <= date and (
                version.end_date is None or date <= version.end_date
            ):
                return version
        return None

    def get_schema_for_date(self, date: datetime.datetime) -> Optional[Dict[str, str]]:
        """Get schema that was active at the given date."""
        schema_version = self.get_schema_version_for_date(date)
        if schema_version:
            return schema_version.columns
        return None

    def print_timeline(self) -> None:
        """Print schema version timeline for debugging."""
        for version in self._versions:
            print(f"\n{version}")
            print("Columns:")
            for col, type_ in version.columns.items():
                print(f"  - {col}: {type_}")

    def load_from_list(self, schemas: List[Dict[str, Any]]) -> None:
        """Load schema versions from a list of dictionaries.

        Args:
            schemas: List of dictionaries, each containing:
                - start_date (datetime): Start date of schema version
                - end_date (Optional[datetime]): End date of schema version
                - columns (Dict): Column definitions

        Returns:
            SchemaRegistry: Registry populated with the provided schema versions

        Raises:
            ValueError: If schema versions are invalid or contain overlapping dates
        """
        self._versions = []
        sorted_schemas = sorted(schemas, key=lambda x: x["start_date"])
        for schema in sorted_schemas:
            # Add each schema version to registry
            self.add_schema_version(
                start_date=schema["start_date"],
                end_date=schema.get("end_date"),
                columns=schema["columns"],
            )
