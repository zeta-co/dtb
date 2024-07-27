from datetime import datetime
from typing import Dict, List, Tuple, Union
from pyspark.sql import SparkSession
from dtb.delta.delta_table_operator import DeltaTableOperator
from dtb.rollback.rollback_criteria import RollbackCriteria
from dtb.rollback.rollback_detail import RollbackDetail
from dtb.rollback.rollback_plan import RollbackPlan
from dtb.rollback.rollback_request import RollbackRequest
from dtb.rollback.rollback_result import RollbackResult
from dtb.table.table import Table
from dtb.table.table_selector import TableSelector
from dtb.logging.version import Version
from dtb.logging.log_service import LogService


class RollbackManager:   
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.table_selector = TableSelector(spark)
        self.delta_table_operator = DeltaTableOperator(spark)

    def execute_rollback(self, request: RollbackRequest, log_service: LogService) -> Union[RollbackResult, RollbackPlan]:
        if request.criteria == RollbackCriteria.WORKFLOW:
            result = self._rollback_workflow(request)
        elif request.criteria == RollbackCriteria.SCHEMA:
            result = self._rollback_schema(request)
        elif request.criteria == RollbackCriteria.TABLE_LIST:
            result = self._rollback_table_list(request)
        else:
            raise ValueError(f"Unknown rollback criteria: [{request.criteria}]!")
        
        if not request.dry_run:
            log_service.log_rollback(result)
            return result
        else:
            return RollbackPlan(result)

    def _rollback_workflow(self, request: RollbackRequest) -> RollbackResult:
        log_table_name = request.kwargs.get('log_table_name')
        job_id = request.kwargs.get('job_id')
        run_id = request.kwargs.get('run_id')

        if not all([log_table_name, job_id, run_id]):
            raise ValueError("log_table_name, job_id and run_id must be specified!")

        tables_to_rollback = self.table_selector.select_tables_from_log(log_table_name, job_id, run_id)
        return self._perform_rollback(tables_to_rollback, request.user, request.dry_run)

    def _rollback_schema(self, request: RollbackRequest) -> RollbackResult:
        schema = request.kwargs.get('schema')
        timestamp = request.kwargs.get('timestamp')

        if not all([schema, timestamp]):
            raise ValueError("schema and timestamp must be specified!")

        tables_to_rollback = self.table_selector.select_tables_by_schema(schema)
        return self._perform_rollback([(table, timestamp) for table in tables_to_rollback], request.user, request.dry_run)

    def _rollback_table_list(self, request: RollbackRequest) -> RollbackResult:
        tables = request.kwargs.get('tables')
        timestamp = request.kwargs.get('timestamp')

        if not all([tables, timestamp]):
            raise ValueError("tables and timestamp must be specified!")

        tables_to_rollback = self.table_selector.select_tables_by_list(tables)
        return self._perform_rollback([(table, timestamp) for table in tables_to_rollback], request.user, request.dry_run)

    def _perform_rollback(
            self, 
            tables_to_rollback: List[Tuple[Table, Union[int, datetime]]], 
            user: str,
            dry_run: bool
        ) -> Union[RollbackResult, List[Dict[str, Union[str, int, datetime]]]]:
        details = []
        for table_obj, version_or_timestamp in tables_to_rollback:
            version_obj = Version(version_or_timestamp)

            result = self.delta_table_operator.rollback_table(table_obj, version_obj, dry_run)
            if result["success"]:
                detail = {
                    "catalog": table_obj.catalog,
                    "schema": table_obj.schema,
                    "table": table_obj.name,
                    "from_version": result["from_version"],
                    "to_version": result["to_version"],
                    "rollback_timestamp": result["rollback_timestamp"],
                    "success": True,
                    "action": result["action"]
                }
            else:
                detail = {
                    "catalog": table_obj.catalog,
                    "schema": table_obj.schema,
                    "table": table_obj.name,
                    "from_version": None,
                    "to_version": None,
                    "rollback_timestamp": None,
                    "success": False,
                    "error_message": result["error"]
                }
            details.append(detail)

        if dry_run:
            return details
        else:
            return RollbackResult([RollbackDetail(**d) for d in details], user)
