from pyspark.sql import SparkSession, DataFrame


class LogTableReader:

    @staticmethod
    def read_log_table(
        spark: SparkSession, log_table_name: str, job_id: str, run_id: str
    ) -> DataFrame:
        query = f"SELECT TableName, VersionFrom FROM {log_table_name} WHERE JobID = '{job_id}' AND RunID = '{run_id}'"
        return spark.sql(query)
