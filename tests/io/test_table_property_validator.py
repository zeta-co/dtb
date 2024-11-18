import pytest
from typing import Any
from dtb.io.table_property_validator import TablePropertyValidator


class TestDeltaPropertyValidator:
    @pytest.mark.parametrize(
        "key,value,expected_valid",
        [
            # Boolean properties
            ("delta.appendOnly", True, True),
            ("delta.appendOnly", False, True),
            ("delta.appendOnly", "true", True),
            ("delta.appendOnly", "false", True),
            ("delta.appendOnly", "invalid", False),
            
            # Integer properties
            ("delta.checkpointInterval", 10, True),
            ("delta.checkpointInterval", "10", True),
            ("delta.checkpointInterval", 0, False),
            ("delta.checkpointInterval", "invalid", False),
            
            # Enum properties
            ("delta.checkpointPolicy", "classic", True),
            ("delta.checkpointPolicy", "v2", True),
            ("delta.checkpointPolicy", "invalid", False),
            
            # Calendar interval properties
            ("delta.deletedFileRetentionDuration", "interval 1 week", True),
            ("delta.deletedFileRetentionDuration", "invalid", False),
            
            # Size in bytes properties
            ("delta.targetFileSize", "128MB", True),
            ("delta.targetFileSize", "invalid", False),
            
            # Unknown property
            ("delta.unknownProperty", "value", False),
        ]
    )
    def test_property_validation(self, key: str, value: Any, expected_valid: bool):
        is_valid, error = TablePropertyValidator.validate(key, value)
        assert is_valid == expected_valid
        if not expected_valid:
            assert error is not None

    def test_property_info(self):
        info = TablePropertyValidator.get_property_info("delta.isolationLevel")
        assert info is not None
        assert info["type"] == "enum"
        assert "Serializable" in info["allowed_values"]
        assert "WriteSerializable" in info["allowed_values"]
        assert info["default_value"] == "WriteSerializable"
        assert "description" in info

        # Unknown property
        assert TablePropertyValidator.get_property_info("unknown.property") is None
