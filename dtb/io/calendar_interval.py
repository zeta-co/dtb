import re
from datetime import timedelta


class CalendarInterval:
    """Represents a time duration for Delta table properties."""

    _INTERVAL_PATTERN = re.compile(
        r"^interval\s+(?:(\d+)\s+year[s]?)?\s*"
        r"(?:(\d+)\s+month[s]?)?\s*"
        r"(?:(\d+)\s+week[s]?)?\s*"
        r"(?:(\d+)\s+day[s]?)?\s*"
        r"(?:(\d+)\s+hour[s]?)?\s*"
        r"(?:(\d+)\s+minute[s]?)?\s*"
        r"(?:(\d+)\s+second[s]?)?$",
        re.IGNORECASE,
    )

    def __init__(self, interval_str: str):
        """Initialise CalendarInterval from string.

        Args:
            interval_str (str): Interval string (e.g., "interval 1 week").

        Raises:
            ValueError: If interval string is invalid.
        """
        self.original_str = interval_str
        match = self._INTERVAL_PATTERN.match(interval_str.strip())
        if not match:
            raise ValueError(f"Invalid interval format: {interval_str}")

        years, months, weeks, days, hours, minutes, seconds = map(
            lambda x: int(x) if x else 0, match.groups()
        )

        self.timedelta = timedelta(
            days=days + (weeks * 7) + (years * 365) + (months * 30),
            hours=hours,
            minutes=minutes,
            seconds=seconds,
        )

    def __str__(self) -> str:
        return self.original_str
