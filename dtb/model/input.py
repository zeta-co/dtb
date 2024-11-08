from pyspark.sql import SparkSession, DataFrame
from .dataset import Dataset


class Input(Dataset):
    """
    Represents an input dataset in the ETL process.
    """

    def df(self, spark: SparkSession) -> DataFrame:
        """
        Create a DataFrame from the input dataset based on metadata.

        This method supports various input formats including files, tables, and streams.

        Args:
            spark (SparkSession): The active Spark session.

        Returns:
            DataFrame: A Spark DataFrame representing the input data.
        """
        meta = self.metadata
        if meta:
            format = meta["format"]
            stream = meta.get("stream", False)
            table = meta.get("table", False)
            if format == "cloudFiles" or stream:
                reader = spark.readStream
            else:
                reader = spark.read
            if table:
                return reader.table(meta["load"])
            else:
                reader = reader.format(format)
                if "options" in meta:
                    reader = reader.options(**meta["options"])
                if "schema" in meta:
                    reader = reader.schema(meta["schema"])
                return reader.load(meta["load"])
        return None
