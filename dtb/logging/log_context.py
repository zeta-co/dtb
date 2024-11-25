from typing import Any, Dict, ItemsView, KeysView, Optional, Set, Union, ValuesView


class LogContext:
    """
    Flexible container for log context that supports dynamic fields.
    Implements dictionary-like access while maintaining type hints for core fields.
    """

    def __init__(
        self,
        job_id: str,
        job_name: str,
        run_id: str,
        table_name: str,
        table_path: str,
        *,  # Force keyword arguments for remaining parameters
        metadata: Dict[str, Any] = None,
        **kwargs: Any
    ):
        # Initialise internal storage
        self._data: Dict[str, Any] = {}

        # Set required core fields
        self._data["job_id"] = job_id
        self._data["job_name"] = job_name
        self._data["run_id"] = run_id
        self._data["table_name"] = table_name
        self._data["table_path"] = table_path

        # Add any additional metadata
        if metadata:
            self._data.update(metadata)

        # Add any additional kwargs
        self._data.update(kwargs)

        # Track core field names for type hints and validation
        self._core_fields = {"job_id", "job_name", "run_id", "table_name", "table_path"}

    @property
    def job_id(self) -> str:
        """Get job_id with type hint"""
        return self._data["job_id"]

    @property
    def job_name(self) -> str:
        """Get job_name with type hint"""
        return self._data["job_name"]

    @property
    def run_id(self) -> str:
        """Get run_id with type hint"""
        return self._data["run_id"]
    
    @property
    def table_name(self) -> str:
        """Get table_name with type hint"""
        return self._data["table_name"]
    
    @property
    def table_path(self) -> str:
        """Get table_path with type hint"""
        return self._data["table_path"]

    def with_fields(self, **kwargs: Any) -> "LogContext":
        """
        Create new LogContext with additional fields.
        Useful for adding context-specific fields without modifying original.
        """
        new_data = self._data.copy()
        new_data.update(kwargs)
        new_context = LogContext(self.job_id, self.run_id)
        new_context._data = new_data
        return new_context

    def to_dict(
        self,
        include_fields: Optional[Set[str]] = None,
        exclude_fields: Optional[Set[str]] = None,
    ) -> Dict[str, Any]:
        """
        Convert to dictionary with optional field filtering.

        Args:
            include_fields: Specific fields to include (None means include all)
            exclude_fields: Fields to exclude

        Returns:
            Dict containing the specified fields
        """
        if include_fields is None:
            result = self._data.copy()
        else:
            result = {k: v for k, v in self._data.items() if k in include_fields}

        if exclude_fields:
            result = {k: v for k, v in result.items() if k not in exclude_fields}

        return result

    def merge(self, other: Union[Dict[str, Any], "LogContext"]) -> "LogContext":
        """
        Create new LogContext by merging with another context or dictionary.
        Useful for combining different contexts.
        """
        if isinstance(other, LogContext):
            other_data = other._data
        else:
            other_data = other

        new_data = self._data.copy()
        new_data.update(other_data)

        new_context = LogContext(self.job_id, self.run_id)
        new_context._data = new_data
        return new_context

    def __getitem__(self, key: str) -> Any:
        """Enable dictionary-like access: context['field_name']"""
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Enable dictionary-like assignment: context['field_name'] = value"""
        self._data[key] = value

    def __contains__(self, key: str) -> bool:
        """Enable 'in' operator: 'field_name' in context"""
        return key in self._data

    def __iter__(self):
        """Enable iteration over fields"""
        return iter(self._data)

    def get(self, key: str, default: Any = None) -> Any:
        """Dictionary-like get with default"""
        return self._data.get(key, default)

    def keys(self) -> KeysView[str]:
        """Get field names"""
        return self._data.keys()

    def values(self) -> ValuesView[Any]:
        """Get field values"""
        return self._data.values()

    def items(self) -> ItemsView[str, Any]:
        """Get field items"""
        return self._data.items()
