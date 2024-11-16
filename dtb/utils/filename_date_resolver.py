from datetime import datetime
from typing import List, Optional
from .date_extractor import ProtocolDateExtractor


class FilenameDateResolver:
    """Responsible for extracting dates from filenames using multiple extractors."""
    def __init__(self):
        self._extractors: List[ProtocolDateExtractor] = []
    
    def add_extractor(self, extractor: ProtocolDateExtractor) -> None:
        """Add a new date extractor."""
        self._extractors.append(extractor)
    
    def get_date(self, filename: str) -> Optional[datetime]:
        """Try all registered extractors to get date from filename."""
        for extractor in self._extractors:
            if date := extractor.extract_date(filename):
                return date
        return None
