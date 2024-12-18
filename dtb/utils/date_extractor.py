from datetime import datetime
import re
from typing import Optional, Dict
from dataclasses import dataclass


@dataclass
class DatePattern:
    """
    Represents a datetime pattern configuration.
    
    Attributes:
        regex_pattern: Regular expression pattern with one capture group for the date.
        datetime_format: strptime format string for parsing the captured date string.
        description: Human-readable description of the date format with example.
    """
    regex_pattern: str
    datetime_format: str
    description: str = ""


class DateExtractor:
    """
    A utility class for extracting datetime objects from strings using predefined or custom patterns.
    
    This class provides methods to extract datetime objects from strings using either predefined
    patterns (accessible via pattern keys) or custom patterns. It includes common date formats
    and supports both date-only and timestamp patterns.
    
    Class Attributes:
        DEFAULT_PATTERNS: A dictionary of predefined DatePattern objects for common formats.
    
    Example:
        >>> DateExtractor.extract_from("file_2024-01-15_data.csv", "yyyy-mm-dd")
        datetime.datetime(2024, 1, 15, 0, 0)
        
        >>> DateExtractor.extract_custom("data_@2024-01-15@_file.csv",
        ...                             r"@(\d{4}-\d{2}-\d{2})@",
        ...                             "%Y-%m-%d")
        datetime.datetime(2024, 1, 15, 0, 0)
    """

    # Default patterns
    DEFAULT_PATTERNS: Dict[str, DatePattern] = {
        "yyyy-mm-dd": DatePattern(
            regex_pattern=r"(\d{4}-\d{2}-\d{2})",
            datetime_format="%Y-%m-%d",
            description="YYYY-MM-DD format (e.g., 2024-01-01)",
        ),
        "yyyy/mm/dd": DatePattern(
            regex_pattern=r"(\d{4}/\d{2}/\d{2})",
            datetime_format="%Y/%m/%d",
            description="YYYY/MM/DD format (e.g., 2024/01/01)",
        ),
        "yyyymmdd": DatePattern(
            regex_pattern=r"(\d{8})",
            datetime_format="%Y%m%d",
            description="YYYYMMDD format (e.g., 20240101)",
        ),
        "dd-mm-yyyy": DatePattern(
            regex_pattern=r"(\d{2}-\d{2}-\d{4})",
            datetime_format="%d-%m-%Y",
            description="DD-MM-YYYY format (e.g., 01-01-2024)",
        ),
        "dd/mm/yyyy": DatePattern(
            regex_pattern=r"(\d{2}/\d{2}/\d{4})",
            datetime_format="%d/%m/%Y",
            description="DD/MM/YYYY format (e.g., 01/01/2024)",
        ),
        "yyyy-mm-dd_hh:mm:ss": DatePattern(
            regex_pattern=r"(\d{4}-\d{2}-\d{2}[_T ]\d{2}:\d{2}:\d{2})",
            datetime_format="%Y-%m-%d %H:%M:%S",
            description="YYYY-MM-DD HH:MM:SS format (e.g., 2024-01-01 15:30:00)",
        ),
        "yyyymmdd_hhmmss": DatePattern(
            regex_pattern=r"(\d{8}_\d{6})",
            datetime_format="%Y%m%d_%H%M%S",
            description="YYYYMMDD_HHMMSS format (e.g., 20240101_153000)",
        ),
        "yyyy-mm-ddThh:mm:ss": DatePattern(
            regex_pattern=r"(\d{4}-\d{2}-\d{2}[_T ]\d{2}:\d{2}:\d{2})",
            datetime_format="%Y-%m-%dT%H:%M:%S",
            description="YYYY-MM-DDTHH:MM:SS format (e.g., 2024-01-01T15:30:00)",
        ),
    }

    def __init__(self, custom_patterns: Optional[Dict[str, DatePattern]] = None):
        """
        Initialise with optional custom patterns.

        Args:
            custom_patterns: Dictionary of custom patterns to add/override defaults
        """
        self.patterns = {**self.DEFAULT_PATTERNS}
        if custom_patterns:
            self.patterns.update(custom_patterns)

    @classmethod
    def extract_from(cls, text: str, pattern_key: str) -> Optional[datetime]:
        """
        Extract datetime from text using a predefined pattern.
        
        Args:
            text: String containing the datetime to be extracted.
            pattern_key: Key of the predefined pattern to use for extraction.
            
        Returns:
            Optional[datetime]: Extracted datetime object if successful, None otherwise.
            
        Raises:
            KeyError: If pattern_key is not found in DEFAULT_PATTERNS.
            
        Example:
            >>> DateExtractor.extract_from("log_2024-01-15_data.csv", "yyyy-mm-dd")
            datetime.datetime(2024, 1, 15, 0, 0)
        """
        if pattern_key not in cls.DEFAULT_PATTERNS:
            raise KeyError(
                f"Unknown pattern: {pattern_key}. Available patterns: {', '.join(cls.DEFAULT_PATTERNS.keys())}"
            )

        pattern = cls.DEFAULT_PATTERNS[pattern_key]
        return cls._extract_with_pattern(text, pattern)

    @classmethod
    def extract_custom(
        cls, text: str, regex_pattern: str, datetime_format: str
    ) -> Optional[datetime]:
        """
        Extract datetime using a custom pattern.
        
        Args:
            text: String containing the datetime to be extracted.
            regex_pattern: Regular expression pattern with one capture group for the date.
            datetime_format: strptime format string for parsing the captured date string.
            
        Returns:
            Optional[datetime]: Extracted datetime object if successful, None otherwise.
            
        Example:
            >>> DateExtractor.extract_custom(
            ...     "data_@2024-01-15@_file.csv",
            ...     r"@(\d{4}-\d{2}-\d{2})@",
            ...     "%Y-%m-%d"
            ... )
            datetime.datetime(2024, 1, 15, 0, 0)
        """
        pattern = DatePattern(regex_pattern, datetime_format)
        return cls._extract_with_pattern(text, pattern)

    @staticmethod
    def _extract_with_pattern(text: str, pattern: DatePattern) -> Optional[datetime]:
        """
        Extract datetime using given pattern.
        
        Args:
            text: String containing the datetime to be extracted.
            pattern: DatePattern object containing regex and format patterns.
            
        Returns:
            Optional[datetime]: Extracted datetime object if successful, None otherwise.
        """
        match = re.search(pattern.regex_pattern, text)
        if not match:
            return None

        try:
            date_str = match.group(1)
            return datetime.strptime(date_str, pattern.datetime_format)
        except ValueError:
            return None

    @classmethod
    def list_patterns(cls) -> Dict[str, str]:
        """
        List all available default patterns with their descriptions.
        
        Returns:
            Dict[str, str]: Dictionary mapping pattern keys to their descriptions.
            
        Example:
            >>> patterns = DateExtractor.list_patterns()
            >>> patterns['yyyy-mm-dd']
            'YYYY-MM-DD format (e.g., 2024-01-01)'
        """
        return {
            key: pattern.description for key, pattern in cls.DEFAULT_PATTERNS.items()
        }
