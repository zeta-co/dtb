from typing import Union
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter, DataFrameWriterV2
from pyspark.sql.streaming import DataStreamWriter
from .dataset import Dataset


class Output(Dataset):
    
    def df(self, spark: SparkSession) -> DataFrame:
        pass

    def writer(self, df: DataFrame) -> Union[DataFrameWriter, DataFrameWriterV2, DataStreamWriter]:
        meta = self.metadata
        if meta.get("stream", False):
            writer = df.writeStream
        else:
            writer = df.write
        if "mode" in meta:
            writer = writer.mode(meta["mode"])
        if "outputMode" in meta:
            writer = writer.outputMode(meta["outputMode"])
        if "options" in meta:
            writer = writer.options(meta["options"])
        if "partitionBy" in meta:
            writer = writer.partitionBy(meta["partitionBy"])
        if "sortBy" in meta:
            writer = writer.sortBy(meta["sortBy"])
        if "trigger" in meta:
            writer = writer.trigger(meta["trigger"])
        return writer

    def write(self, df: DataFrame) -> None:
        meta = self.metadata
        format = meta["format"]
        target = meta["save"]
        if meta.get("stream", False):
            if format == "table":
                self.writer(df).toTable(target)
            else:
                self.writer(df).format(format).start(target)
        else:
            if format == "table":
                self.writer(df).saveAsTable(target)
            else:
                self.writer(df).format(format).save(target)
