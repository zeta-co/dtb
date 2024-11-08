from datetime import datetime
from typing import Union
from ..utils.exception import DeltaTableVersionInvalidException


class DeltaVersion:
    def __init__(self, version_or_timestamp: Union[int, datetime]):
        if not isinstance(version_or_timestamp, (int, datetime)):
            raise DeltaTableVersionInvalidException(
                f"Invalid delta table version type: [{type(version_or_timestamp)}]\nOnly int and datetime are supported!"
            )
        self.version_or_timestamp = version_or_timestamp
