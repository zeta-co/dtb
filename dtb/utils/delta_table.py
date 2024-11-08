from delta import DeltaTable
from pyspark.sql import SparkSession
from .exception import NotDeltaTableException, TableNotExistException


def table_is_delta(spark: SparkSession, table_name: str) -> bool:
    try:
        DeltaTable.forName(spark, table_name)
        return True
    except Exception:
        return False


def get_delta_table_from_name(
    spark: SparkSession, 
    table_name: str = None, 
    raise_exception: bool = False
) -> DeltaTable:
    if spark.catalog.tableExists(table_name):
        if table_is_delta(spark, table_name):
            return DeltaTable.forName(spark, table_name)
        elif raise_exception:
            raise NotDeltaTableException(f"Table [{table_name}] is not a delta table!")
        else:
            return None
    elif raise_exception:
        raise TableNotExistException(f"Table [{table_name}] doesn't exist!")
    else:
        return None


def get_delta_table_from_path(
    spark: SparkSession, 
    path: str = None, 
    raise_exception: bool = False
) -> DeltaTable:
    if DeltaTable.isDeltaTable(spark, path):
        return DeltaTable.forPath(spark, path)
    elif raise_exception:
        raise NotDeltaTableException(f"[{path}] is not a path for delta table!")
    else:
        return None


def get_delta_table(
    spark: SparkSession,
    table_name: str = None,
    path: str = None,
    raise_exception: bool = False,
):
    delta_table = None
    if table_name:
        delta_table = get_delta_table_from_name(spark, table_name, raise_exception)
    elif path:
        delta_table = get_delta_table_from_path(spark, path, raise_exception)
    else:
        raise ValueError(f"Please specify either 'table_name' or 'table_path'!")
    return delta_table
