from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union


class FilterStrategy(ABC):
    """Abstract strategy for handling runtime filters"""

    @abstractmethod
    def apply_filter(self) -> Union[str, Dict[str, Any]]:
        """Apply filter and return either modified path or filter conditions"""
        pass

    # TODO
    @abstractmethod
    def _validate_filters(self, filter_params: Optional[Dict[str, Any]]):
        """Validate provided filters against metadata requirements"""
        pass


class FileListFilterStrategy(FilterStrategy):
    """Strategy for using a list of files as filter for Spark Reader"""

    def apply_filter(self, file_list: List[str]) -> str:
        return ",".join(file_list)


class SqlFilterStrategy(FilterStrategy):
    """Strategy for handling SQL-based filtering for tables"""

    def apply_filter(self, sql_condition: str) -> str:
        return sql_condition
