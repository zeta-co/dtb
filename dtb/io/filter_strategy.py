from abc import ABC, abstractmethod
from typing import List


class FilterStrategy(ABC):
    """Abstract base class defining interface for runtime filtering strategies.
    
    This class provides a common interface for implementing different filtering
    approaches for data sources, such as file list filtering or SQL conditions.
    """

    @abstractmethod
    def apply_filter(self) -> str:
        """Applies the filter strategy and returns the filtered result.
        
        Returns:
            str: Either a modified path or filter conditions depending on strategy.
        """
        pass

    # TODO
    # @abstractmethod
    # def _validate_filter(self):
    #     """Validate provided filters against metadata requirements"""
    #     pass


class FileListFilterStrategy(FilterStrategy):
    """Implementation of FilterStrategy for filtering based on file lists.
    
    This strategy is used when the input source needs to be filtered to specific
    files within a directory.
    """

    def apply_filter(self, file_list: List[str]) -> str:
        """Applies file list filtering by joining paths.
        
        Args:
            file_list (List[str]): List of file paths to include in the filter.
            
        Returns:
            str: Comma-separated string of file paths.
        """
        return ",".join(file_list)


class SqlFilterStrategy(FilterStrategy):
    """Strategy for handling SQL-based filtering for tables"""

    def apply_filter(self, sql_condition: str) -> str:
        return sql_condition
