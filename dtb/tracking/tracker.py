from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pyspark.sql import SparkSession


class Tracker(ABC):

    _state: Dict[str, Any]

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        initial_state: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._spark = spark
        self._state = initial_state or {}

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def end(self) -> None:
        pass

    def get_state(self) -> Dict[str, Any]:
        return self._state
