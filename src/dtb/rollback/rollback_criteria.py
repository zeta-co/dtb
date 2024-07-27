from enum import Enum

class RollbackCriteria(Enum):
    WORKFLOW = 1
    TABLE_LIST = 2
    SCHEMA = 3
