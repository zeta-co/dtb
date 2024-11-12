from abc import ABC, abstractmethod
from typing import Any, Dict
from pyspark.sql import DataFrame
from ..metadata import Metadata


class WriteStrategy(ABC):
    """Abstract base class for write strategy implementations."""
    
    @abstractmethod
    def write_batch(
        self,
        df: DataFrame,
        writer: Any,
        metadata: Metadata
    ) -> None:
        """Execute batch write strategy.
        
        Args:
            df (DataFrame): DataFrame to write.
            writer: DataFrame writer object.
            metadata (Metadata): Output metadata.
        """
        pass
    
    @abstractmethod
    def write_stream(
        self,
        df: DataFrame,
        writer: Any,
        metadata: Metadata
    ) -> None:
        """Execute streaming write strategy.
        
        Args:
            df (DataFrame): DataFrame to write.
            writer: Streaming writer object.
            metadata (Metadata): Output metadata.
        """
        pass


class AppendStrategy(WriteStrategy):
    """Strategy for append-mode writes."""
    
    def write_batch(
        self,
        df: DataFrame,
        writer: Any,
        metadata: Metadata
    ) -> None:
        writer = writer.mode("append")
        
        if metadata.is_table:
            writer.saveAsTable(
                f"{metadata.table_catalog}."
                f"{metadata.table_schema}."
                f"{metadata.table_name}"
            )
        else:
            writer.save(metadata.path)
    
    def write_stream(
        self,
        df: DataFrame,
        writer: Any,
        metadata: Metadata
    ) -> None:
        writer = writer.outputMode("append")
        
        if metadata.is_table:
            writer.toTable(
                f"{metadata.table_catalog}."
                f"{metadata.table_schema}."
                f"{metadata.table_name}"
            )
        else:
            writer.start(metadata.path)


class OverwriteStrategy(WriteStrategy):
    """Strategy for overwrite-mode writes."""
    
    def _should_evolve_schema(
        self,
        new_schema: Dict[str, Any],
        existing_schema: Dict[str, Any]
    ) -> bool:
        """Determine if schema evolution is needed.
        
        Args:
            new_schema (Dict[str, Any]): New schema from DataFrame.
            existing_schema (Dict[str, Any]): Existing schema in target.
            
        Returns:
            bool: True if schema evolution is needed.
        """
        # TODO: Implement schema comparison logic
        return True
    
    def write_batch(
        self,
        df: DataFrame,
        writer: Any,
        metadata: Metadata
    ) -> None:
        writer = writer.mode("overwrite")
        
        if metadata.type == "delta":
            # Handle schema evolution for Delta tables
            writer = writer.option("overwriteSchema", "true")
        
        if metadata.is_table:
            writer.saveAsTable(
                f"{metadata.table_catalog}."
                f"{metadata.table_schema}."
                f"{metadata.table_name}"
            )
        else:
            writer.save(metadata.path)
    
    def write_stream(
        self,
        df: DataFrame,
        writer: Any,
        metadata: Metadata
    ) -> None:
        raise ValueError("Streaming doesn't support overwrite!")
