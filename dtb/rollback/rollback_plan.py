from datetime import datetime
from typing import Dict, List, Union


class RollbackPlan:
    def __init__(self, details: List[Dict[str, Union[str, int, datetime]]]):
        self.details = details

    def __str__(self):
        return (
            "Restore Plan:\n"
            + "\n".join(
                [
                    f"  - {detail['catalog']}.{detail['schema']}.{detail['table']}: "
                    f"From version {detail['from_version']} to {detail['to_version']}"
                    for detail in self.details
                ]
            )
        )
