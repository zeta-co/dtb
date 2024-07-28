from pyspark.sql import SparkSession, DataFrame
from .dataset import Dataset


class Input(Dataset):

    def df(self, spark: SparkSession) -> DataFrame:
        meta = self.metadata
        if meta:
            format = meta["format"]
            stream = meta.get("stream", False)
            if format == "cloudFiles" or stream:
                reader = spark.readStream
            else:
                reader = spark.read
            if format == "table":
                return reader.table(meta["load"])
            else:
                reader = reader.format(meta["format"])
                if "options" in meta:
                    reader = reader.options(meta["options"])
                if "schema" in meta:
                    reader = reader.schema(meta["schema"])
                return reader.load(meta["load"])
        return None
