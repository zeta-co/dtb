import pytest
from typing import Dict, Any
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from dtb.logging.log_entry_mapper import LogEntryMapper


class TestLogEntryMapper:
    @pytest.fixture
    def simple_schema(self) -> StructType:
        """Fixture providing a simple schema for testing"""
        return StructType(
            [
                StructField("FirstName", StringType(), True),
                StructField("LastName", StringType(), True),
                StructField("Age", IntegerType(), True),
            ]
        )

    @pytest.fixture
    def complex_schema(self) -> StructType:
        """Fixture providing a more complex schema for testing"""
        return StructType(
            [
                StructField(
                    "UserDetails",
                    StructType(
                        [
                            StructField("FirstName", StringType(), True),
                            StructField("LastName", StringType(), True),
                        ]
                    ),
                    True,
                ),
                StructField("AccountStatus", StringType(), True),
                StructField("LoginCount", IntegerType(), True),
            ]
        )

    def test_snake_to_pascal(self):
        """Test snake_case to PascalCase conversion"""
        test_cases = [
            ("hello_world", "HelloWorld"),
            ("user_first_name", "UserFirstName"),
            ("simple", "Simple"),
            ("already_Pascal_Case", "AlreadyPascalCase"),
            ("multiple___underscores", "MultipleUnderscores"),
            ("", ""),
            ("_leading_underscore", "LeadingUnderscore"),
            ("trailing_underscore_", "TrailingUnderscore"),
        ]

        for input_str, expected in test_cases:
            assert LogEntryMapper.snake_to_pascal(input_str) == expected

    def test_get_schema_fields(self, simple_schema):
        """Test extraction of field names from schema"""
        expected_fields = {"FirstName", "LastName", "Age"}
        result = LogEntryMapper.get_schema_fields(simple_schema)
        assert result == expected_fields

    def test_get_schema_fields_complex(self, complex_schema):
        """Test extraction of field names from complex schema"""
        expected_fields = {"UserDetails", "AccountStatus", "LoginCount"}
        result = LogEntryMapper.get_schema_fields(complex_schema)
        assert result == expected_fields

    def test_transform_key_pascal(self):
        """Test key transformation to PascalCase"""
        test_cases = [
            ("user_name", "UserName"),
            ("email_address", "EmailAddress"),
            ("simple", "Simple"),
            ("already_Pascal_Case", "AlreadyPascalCase"),
        ]

        for input_key, expected in test_cases:
            assert LogEntryMapper.transform_key(input_key, "pascal") == expected

    def test_transform_key_default(self):
        """Test key transformation with unsupported naming convention"""
        test_cases = [
            ("user_name", "user_name"),
            ("email_address", "email_address"),
            ("simple", "simple"),
        ]

        for input_key, expected in test_cases:
            assert LogEntryMapper.transform_key(input_key, "unsupported") == expected

    def test_map_to_schema_simple(self, simple_schema):
        """Test mapping dictionary to simple schema"""
        input_data = {
            "first_name": "John",
            "last_name": "Doe",
            "age": 30,
            "extra_field": "ignored",
        }

        expected = {"FirstName": "John", "LastName": "Doe", "Age": 30}

        result = LogEntryMapper.map_to_schema(input_data, simple_schema)
        assert result == expected

    def test_map_to_schema_missing_fields(self, simple_schema):
        """Test mapping with missing fields"""
        input_data = {"first_name": "John", "extra_field": "ignored"}

        expected = {"FirstName": "John"}

        result = LogEntryMapper.map_to_schema(input_data, simple_schema)
        assert result == expected

    def test_map_to_schema_empty_input(self, simple_schema):
        """Test mapping with empty input dictionary"""
        input_data: Dict[str, Any] = {}
        expected: Dict[str, Any] = {}

        result = LogEntryMapper.map_to_schema(input_data, simple_schema)
        assert result == expected

    def test_map_to_schema_complex(self, complex_schema):
        """Test mapping with complex schema"""
        input_data = {
            "user_details": {"first_name": "John", "last_name": "Doe"},
            "account_status": "active",
            "login_count": 42,
            "extra_field": "ignored",
        }

        expected = {
            "UserDetails": {"first_name": "John", "last_name": "Doe"},
            "AccountStatus": "active",
            "LoginCount": 42,
        }

        result = LogEntryMapper.map_to_schema(input_data, complex_schema)
        assert result == expected

    def test_map_to_schema_custom_naming(self, simple_schema):
        """Test mapping with custom naming convention"""
        input_data = {"first_name": "John", "last_name": "Doe", "age": 30}

        # Using default naming (no transformation)
        expected = {"first_name": "John", "last_name": "Doe", "age": 30}

        result = LogEntryMapper.map_to_schema(
            input_data, simple_schema, target_naming="none"
        )
        assert (
            result != expected
        )  # Should not match because schema fields are in PascalCase

    def test_map_to_schema_case_sensitivity(self, simple_schema):
        """Test mapping with different case variations"""
        input_data = {"FIRST_NAME": "John", "Last_Name": "Doe", "AGE": 30}

        expected = {"FirstName": "John", "LastName": "Doe", "Age": 30}

        result = LogEntryMapper.map_to_schema(input_data, simple_schema)
        assert result == expected
