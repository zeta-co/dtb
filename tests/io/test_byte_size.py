import pytest
from dtb.io.byte_size import ByteSize


class TestByteSize:
    @pytest.mark.parametrize(
        "size_str,expected_bytes,expected_valid",
        [
            ("100B", 100, True),
            ("1KB", 1024, True),
            ("1MB", 1024 ** 2, True),
            ("1GB", 1024 ** 3, True),
            ("1TB", 1024 ** 4, True),
            ("100", 100, True),  # Defaults to bytes
            ("100mb", 1024 ** 2 * 100, True),  # Case insensitive
            ("1.5MB", None, False),
            ("MB", None, False),
            ("-1MB", None, False),
            ("invalid", None, False),
        ]
    )
    def test_byte_size_validation(
        self,
        size_str: str,
        expected_bytes: int,
        expected_valid: bool
    ):
        if expected_valid:
            size = ByteSize(size_str)
            assert size.bytes == expected_bytes
            assert str(size) == size_str
        else:
            with pytest.raises(ValueError):
                ByteSize(size_str)
