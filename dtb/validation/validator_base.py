import logging
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from .validation_logger import ValidationLogger
from .validation_result import ValidationResult


class BaseValidator(ABC):
    def __init__(self, spark: SparkSession, logger: ValidationLogger):
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)
        self.validation_logger = logger

    @property
    def name(self) -> str:
        return self.__class__.__name__
