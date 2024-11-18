from datetime import datetime
import re
from typing import Callable, Optional, Protocol


class DateExtractor(Protocol):
    """Protocol defining the interface for date extractors."""
    def extract_date(self, filename: str) -> Optional[datetime]:
        """Extract date from filename."""
        pass


class RegexDateExtractor:
    """Extracts date using regex pattern and custom parser."""
    def __init__(self, 
                 pattern: str, 
                 parse_func: Callable[[str], datetime],
                 description: str = ""):
        self.pattern = pattern
        self.parse_func = parse_func
        self.description = description

    def extract_date(self, filename: str) -> Optional[datetime]:
        match = re.search(self.pattern, filename)
        if match:
            try:
                return self.parse_func(match.group(1))
            except ValueError:
                return None
        return None
    

# Common date extractors - can be extended without modifying existing code
class DateExtractors:
    """Factory for common date extractors."""
    @staticmethod
    def yyyy_mm_dd() -> RegexDateExtractor:
        return RegexDateExtractor(
            pattern=r"(\d{4}-\d{2}-\d{2})",
            parse_func=lambda x: datetime.strptime(x, "%Y-%m-%d"),
            description="YYYY-MM-DD"
        )
    
    @staticmethod
    def yyyymmdd() -> RegexDateExtractor:
        return RegexDateExtractor(
            pattern=r"(\d{8})",
            parse_func=lambda x: datetime.strptime(x, "%Y%m%d"),
            description="YYYYMMDD"
        )
    
    @staticmethod
    def custom_format(pattern: str, 
                     date_format: str,
                     description: str = "") -> RegexDateExtractor:
        return RegexDateExtractor(
            pattern=pattern,
            parse_func=lambda x: datetime.strptime(x, date_format),
            description=description
        )
