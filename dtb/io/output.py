from typing import Any, Dict
from pyspark.sql import DataFrame, SparkSession
from ..model.dataset import Dataset
from ..model.metadata import Metadata
from .write_strategy import AppendStrategy, OverwriteStrategy


class Output(Dataset):
    """Handles writing DataFrame to various targets with schema evolution support.
    
    This class provides a unified interface for writing data to different storage
    formats with support for both batch and streaming writes. It implements
    different write strategies based on the specified mode.
    
    Attributes:
        metadata (Metadata): Configuration metadata for the output.
        write_strategy (WriteStrategy): Strategy for handling writes.
    """
    
    _write_strategies = {
        'append': AppendStrategy,
        'overwrite': OverwriteStrategy
    }

    def __init__(self, metadata: Dict[str, Any]):
        """Initialise Output instance.
        
        Args:
            metadata (Metadata): Output configuration metadata.
            
        Raises:
            ValueError: If specified write mode is not supported.
        """
        super().__init__(metadata)
        
        # Initialise write mode and strategy
        write_mode = self.metadata.mode
        strategy_class = self._write_strategies.get(write_mode)
        if not strategy_class:
            raise ValueError(f"Unsupported write mode: {write_mode}")
        self.write_strategy = strategy_class()

    def _sync_table_properties(self, spark: Any) -> None:
        """Synchronises table properties with metadata configuration."""
        if self.metadata.is_table:
            table_path = (
                f"{self.metadata.table_catalog}."
                f"{self.metadata.table_schema}."
                f"{self.metadata.table_name}"
            )
            # TODO: Implement table property sync
            pass
    
    def _prepare_writer(self, df: DataFrame, is_stream: bool = False) -> Any:
        """Prepares writer with common options.
        
        Args:
            df (DataFrame): DataFrame to write.
            is_stream (bool): Whether to prepare streaming writer.
            
        Returns:
            DataFrame writer with common options applied.
        """
        writer = (df.writeStream if is_stream else df.write).format(self.metadata.type)
        
        # Apply format options
        for key, value in self.metadata.format_options.items():
            writer = writer.option(key, value)
        
        # Apply partitioning
        if self.metadata.partition_by:
            writer = writer.partitionBy(self.metadata.partition_by)
        
        # Handle Delta-specific options
        if self.metadata.type == "delta" and self.metadata.sort_by:
            writer = writer.sortBy(self.metadata.sort_by)
        
        return writer
    
    def write(self, df: DataFrame, spark: SparkSession) -> None:
        """Writes DataFrame to target location.
        
        This method handles both batch and streaming writes using the appropriate
        write strategy based on the configured mode.
        
        Args:
            df (DataFrame): DataFrame to write.
            spark: SparkSession instance.
        
        Raises:
            ValueError: If data quality checks fail or schemas are incompatible.
        """
        
        # Sync table properties
        self._sync_table_properties(spark)
        
        # Prepare writer and execute strategy
        if self.metadata.is_stream:
            writer = self._prepare_writer(df, is_stream=True)
            self.write_strategy.write_stream(df, writer, self.metadata)
        else:
            writer = self._prepare_writer(df)
            self.write_strategy.write_batch(df, writer, self.metadata)
