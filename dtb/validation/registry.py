from abc import ABC, abstractmethod
from typing import Any
from .check_log_entry_builder import CheckLogEntryBuilder


class Registry(ABC):
    """Generic registry for calculators and loggers"""

    def __init__(self):
        self._dict = {}

    def register(self, key: str, value: Any) -> None:
        self._dict[key] = value

    @abstractmethod
    def get(self, key: str) -> Any:
        pass


class CheckLogEntryBuilderRegistry(Registry):
    """Registry for log entry builder"""
    
    def get(self, key: str) -> CheckLogEntryBuilder:
        if key not in self._dict:
            raise ValueError(f"No CheckLogEntryBuilder registered for key: {key}")
        return self._dict[key]
