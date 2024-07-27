from datetime import datetime
from typing import Union


class Version:
    def __init__(self, version_or_timestamp: Union[str, datetime]):
        self.version_or_timestamp = version_or_timestamp
