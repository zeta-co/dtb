from abc import ABC
from dataclasses import dataclass
from ..utils.pyspark import class_to_struct_type


@dataclass(frozen=False)
class LogEntry(ABC):

    @classmethod
    def to_struct_type(cls):
        return class_to_struct_type(cls)
