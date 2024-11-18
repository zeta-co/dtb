import re


class ByteSize:
    """Represents size in bytes for Delta table properties."""

    _SIZE_PATTERN = re.compile(r"^(\d+)\s*([kmgt]?b)?$", re.IGNORECASE)

    _MULTIPLIERS = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}

    def __init__(self, size_str: str):
        """Initialize ByteSize from string.

        Args:
            size_str (str): Size string (e.g., "100MB").

        Raises:
            ValueError: If size string is invalid.
        """
        self.original_str = size_str
        match = self._SIZE_PATTERN.match(size_str.strip())
        if not match:
            raise ValueError(f"Invalid size format: {size_str}")

        value, unit = match.groups()
        self.bytes = int(value) * self._MULTIPLIERS.get((unit or "B").upper(), 1)

    def __str__(self) -> str:
        return self.original_str
