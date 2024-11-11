from typing import Union
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter, DataFrameWriterV2
from pyspark.sql.streaming import DataStreamWriter
from .dataset import Dataset


class Output(Dataset):
    """
    Represents an output dataset in the ETL process.
    """
    
    def df(self, spark: SparkSession) -> DataFrame:
        """
        This method is not implemented for Output datasets.
        """
        pass

    def writer(self, df: DataFrame) -> Union[DataFrameWriter, DataFrameWriterV2, DataStreamWriter]:
        """
        Create a writer for the DataFrame based on metadata.

        Args:
            df (DataFrame): The DataFrame to be written.

        Returns:
            Union[DataFrameWriter, DataFrameWriterV2, DataStreamWriter]: A configured writer for the DataFrame.
        """
        meta = self.metadata
        if meta.is_stream:
            writer = df.writeStream
        else:
            writer = df.write
        if meta.mode:
            writer = writer.mode(meta.mode)
        if meta.output_mode:
            writer = writer.outputMode(meta.output_mode)
        if meta.format_options:
            writer = writer.options(**meta.format_options)
        if meta.partition_by:
            writer = writer.partitionBy(meta.partition_by)
        if meta.sort_by:
            writer = writer.sortBy(meta.sort_by)
        return writer

    def write(self, df: DataFrame) -> None:
        """
        Write the DataFrame to the output destination based on metadata.

        This method supports various output formats including files, tables, and streams.

        Args:
            df (DataFrame): The DataFrame to be written.
        """
        meta = self.metadata
        format = meta.type
        target = meta.path
        if not meta.is_stream:
            if meta.is_table:
                self.writer(df).toTable(target)
            else:
                self.writer(df).format(format).start(target)
        else:
            if meta.is_table:
                self.writer(df).saveAsTable(target)
            else:
                self.writer(df).format(format).save(target)
