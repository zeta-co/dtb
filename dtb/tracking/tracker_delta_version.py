import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession
from .tracker import Tracker
from ..utils.delta_table import get_delta_table


class DeltaVersionTracker(Tracker):
    """Tracks version changes in Delta tables during ETL operations.

    This tracker specifically monitors Delta table version changes, capturing
    information about version numbers, timestamps, and operations performed.
    It can track both named tables and path-based Delta tables.

    Attributes:
        delta_table: Reference to the Delta table being tracked
        _state (Dict[str, Any]): Internal state dictionary containing tracking metadata
        _spark (Optional[SparkSession]): SparkSession instance for Delta operations
    """

    def __init__(
        self,
        spark: SparkSession | None = None,
        initial_state: Dict[str, Any] | None = None,
    ) -> None:
        """Initialize the Delta version tracker.
        
        Args:
            spark: SparkSession instance used for Delta operations
            initial_state: Initial state dictionary containing tracking metadata
        """
        super().__init__(spark, initial_state)
        self.delta_table = None

    def start(self) -> None:
        """Begin tracking Delta table versions.
        
        Captures the current state of the Delta table including its current version,
        table ID, and timestamp. This should be called before the ETL operation starts.
        """
        self.delta_table = get_delta_table(
            self._spark,
            self._state.get("table_name", None),
            self._state.get("table_path", None),
        )
        self._state["datetime"] = datetime.datetime.now()
        if not self.delta_table:
            self._state["version_from"] = None
        else:
            self._state["table_id"] = self.delta_table.detail().collect()[0]["id"]
            self._state["version_from"] = (
                self.delta_table.history()
                .select("version")
                .orderBy("version", ascending=False)
                .first()[0]
            )

    def end(self) -> None:
        """Complete the version tracking process.
        
        Captures the final state of the Delta table after the ETL operation,
        including the new version number, operation type, and associated job details.
        This should be called after the ETL operation completes.
        """
        self.delta_table = get_delta_table(
            self._spark,
            self._state.get("table_name", None),
            self._state.get("table_path", None),
        )
        if not self.delta_table:
            return None
        if not self._state.get("table_id", None):
            self._state["table_id"] = self.delta_table.detail().collect()[0]["id"]
        last_ver = (
            self.delta_table.history()
            .orderBy("version", ascending=False)
            .first()
            .asDict()
        )
        self._state["version_to"] = last_ver["version"]
        self._state["version_datetime"] = last_ver["timestamp"]
        self._state["operation"] = last_ver["operation"]
        if last_ver.get("job"):
            job = last_ver["job"].asDict()
            self._state["job_id"] = self._state.get("job_id", None) or job.get(
                "jobId", None
            )
            self._state["run_id"] = self._state.get("run_id", None) or job.get(
                "runId", None
            )
        if not self._state.get("table_name", None) and "name" in last_ver:
            self._state["table_name"] = last_ver["name"]
        if not self._state.get("table_path", None) and "location" in last_ver:
            self._state["table_path"] = last_ver["location"]

    def get_log_entry_dict(self) -> Dict[str, Any]:
        """Returns a dictionary containing the tracked Delta table version information.
        
        Returns:
            Dict[str, Any]: A dictionary containing tracked metadata including job details,
                table information, and version changes. Returns empty dict if no Delta
                table is being tracked.
        """
        if not self.delta_table:
            return {}
        return {
            "JobID": self._state.get("job_id", None),
            "JobName": self._state.get("job_name", None),
            "RunID": self._state.get("run_id", None),
            "Operation": self._state.get("operation", None),
            "Datetime": self._state.get("datetime", None),
            "TableID": self._state.get("table_id", None),
            "TableName": self._state.get("table_name", None),
            "TablePath": self._state.get("table_path", None),
            "VersionFrom": self._state.get("version_from", None),
            "VersionTo": self._state.get("version_to", None),
            "VersionDatetime": self._state.get("version_datetime", None),
        }
