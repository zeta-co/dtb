import bisect
from datetime import datetime
from typing import Dict, List, Optional
from .schema_version import SchemaVersion


class SchemaRegistry:
    """Manages schema versions and provides schema lookup capabilities."""

    def __init__(self):
        self._versions: List[SchemaVersion] = []

    def add_schema_version(
        self,
        start_date: datetime,
        columns: Dict[str, str],
        end_date: Optional[datetime] = None,
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

    def get_schema_for_date(self, date: datetime) -> Optional[Dict[str, str]]:
        """Get schema that was active at the given date."""
        for version in self._versions:
            if version.start_date <= date and (
                version.end_date is None or date < version.end_date
            ):
                return version.columns
        return None

    def print_timeline(self) -> None:
        """Print schema version timeline for debugging."""
        for version in self._versions:
            print(f"\n{version}")
            print("Columns:")
            for col, type_ in version.columns.items():
                print(f"  - {col}: {type_}")
