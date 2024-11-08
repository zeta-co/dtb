from datetime import datetime
from typing import Dict, Union
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from ..model.table import Table
from ..model.delta_version import DeltaVersion
from ..utils.delta_table import table_is_delta


class DeltaTableOperator:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def rollback_table(
        self, table: Table, version: DeltaVersion, dry_run: bool = False
    ) -> Dict[str, Union[bool, str, datetime]]:
        if not table_is_delta(self.spark, table.full_name):
            return {
                "success": False,
                "error": f"Table [{table.full_name}] is not a Delta table"
            }
        try:
            delta_table = DeltaTable.forName(self.spark, table.full_name)
            current_version = delta_table.history().select("version").orderBy("version", ascending=False).first()[0]
            table_creation_time = delta_table.detail().select("createdAt").first()[0]

            if version.version_or_timestamp is None:
                action = "truncate"
                to_version = None
            elif isinstance(version.version_or_timestamp, datetime):
                if version.version_or_timestamp < table_creation_time:
                    action = "truncate"
                    to_version = None
                else:
                    action = "rollback"
                    timestamp_str = version.version_or_timestamp.strftime("%Y-%m-%d %H:%M:%S")
                    to_version = delta_table.history().filter(f"timestamp <= '{timestamp_str}'").select("version").orderBy("version", ascending=False).first()[0]
            else:
                action = "rollback"
                to_version = int(version.version_or_timestamp)

            if not dry_run:
                if action == "truncate":
                    self._truncate_table(delta_table)
                else:
                    if isinstance(version.version_or_timestamp, datetime):
                        delta_table.restoreToTimestamp(timestamp_str)
                    else:
                        delta_table.restoreToVersion(to_version)

            return {
                "success": True,
                "from_version": str(current_version),
                "to_version": str(to_version) if to_version is not None else None,
                "rollback_timestamp": datetime.now() if not dry_run else None,
                "action": action,
                "dry_run": dry_run
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _truncate_table(self, delta_table: DeltaTable):
        delta_table.delete()
