from abc import ABC, abstractmethod
from typing import Dict, Any
from .metadata import Metadata


class Dataset(ABC):
    """
    Abstract base class for datasets in the ETL process.
    
    Attributes:
        metadata (Dict[str, Any]): Metadata describing the dataset properties.
    """

    def __init__(self, metadata: Dict[str, Any]):
        """
        Initialise the Dataset with metadata.

        Args:
            metadata (Dict[str, Any]): Metadata describing the dataset properties.
        """
        self.metadata = Metadata(metadata)
