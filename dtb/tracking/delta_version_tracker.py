import datetime
from typing import Dict, Any, List
from pyspark.sql import SparkSession
from .tracker import Tracker
from .delta_version_log_entry import DeltaVersionLogEntry
from ..utils.delta_table import get_delta_table



class DeltaVersionTracker(Tracker):
    def __init__(self, spark: SparkSession, metadate: Dict[str, Any]) -> None:
        super().__init__()
        self.job_id = metadate.get("job_id", None)
        self.run_id = metadate.get("run_id", None)
        self.table_name = metadate.get("table_name", None)
        self.table_path = metadate.get("table_path", None)
        self.delta_table = get_delta_table(spark, self.table_name, self.table_path)

    def start(self, entry: DeltaVersionLogEntry) -> None:
        entry.JobID = self.job_id
        entry.RunID = self.run_id
        entry.RunDatetime = datetime.datetime.now()
        if not self.delta_table:
            entry.VersionFrom = None
        else:
            entry.TableID = self.delta_table.detail().collect()[0]["id"]
            entry.VersionFrom = (
                self.delta_table.history()
                .select("version")
                .orderBy("version", ascending=False)
                .first()[0]
            )

    def end(self, spark: SparkSession, entry: DeltaVersionLogEntry) -> None:
        self.delta_table = get_delta_table(spark, self.table_name, self.table_path)
        if not entry.TableID:
            entry.TableID = self.delta_table.detail().collect()[0]["id"]
        last_ver = (
            self.delta_table.history()
            .orderBy("version", ascending=False)
            .first()
            .asDict()
        )
        entry.VersionTo = last_ver["version"]
        entry.VersionDatetime = last_ver["timestamp"]
        entry.Operation = last_ver["operation"]
        if last_ver.get("job"):
            job = last_ver["job"].asDict()
            entry.JobID = entry.JobID or job.get("jobId", None)
            entry.RunID = entry.RunID or job.get("runId", None)
        if not entry.TableName and "name" in last_ver:
            entry.TableName = last_ver["name"]
        if not entry.TablePath and "location" in last_ver:
            entry.TablePath = last_ver["location"]
