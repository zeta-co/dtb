import pytest
from datetime import datetime
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    StringType,
    TimestampType,
)
from dtb.model.schema_version import SchemaVersion


class TestSchemaVersion:
    @pytest.fixture
    def sample_dates(self):
        """Fixture providing sample dates for testing"""
        return {
            "start": datetime(2023, 1, 1),
            "end": datetime(2023, 12, 31),
        }

    @pytest.fixture
    def sample_schema(self):
        """Fixture providing a sample schema with various data types"""
        return {
            "id": "integer",
            "name": "string",
            "active": "boolean",
            "created_at": "datetime",
            "amount": "decimal",
        }

    @pytest.fixture
    def complex_schema(self):
        """Fixture providing a schema with nullable specifications"""
        return {
            "id": {"type": "integer", "nullable": False},
            "name": {"type": "string", "nullable": True},
            "status": {"type": "string", "nullable": False},
            "created_at": {"type": "datetime", "nullable": True},
        }

    def test_init(self, sample_dates, sample_schema):
        """Test basic initialization"""
        schema = SchemaVersion(
            start_date=sample_dates["start"],
            end_date=sample_dates["end"],
            columns=sample_schema,
            version=1,
        )

        assert schema.start_date == sample_dates["start"]
        assert schema.end_date == sample_dates["end"]
        assert schema.columns == sample_schema
        assert schema.version == 1

    def test_str_representation_with_end_date(self, sample_dates, sample_schema):
        """Test string representation with end date"""
        schema = SchemaVersion(
            start_date=sample_dates["start"],
            end_date=sample_dates["end"],
            columns=sample_schema,
            version=1,
        )

        expected = "Schema V1: 2023-01-01 to 2023-12-31"
        assert str(schema) == expected

    def test_str_representation_current(self, sample_dates, sample_schema):
        """Test string representation for currently active schema"""
        schema = SchemaVersion(
            start_date=sample_dates["start"],
            end_date=None,
            columns=sample_schema,
            version=2,
        )

        expected = "Schema V2: 2023-01-01 to PRESENT"
        assert str(schema) == expected

    def test_struct_type_basic_types(self, sample_dates):
        """Test conversion of basic data types to StructType"""
        basic_schema = {
            "string_field": "string",
            "int_field": "integer",
            "long_field": "long",
            "double_field": "double",
            "decimal_field": "decimal",
            "date_field": "date",
            "datetime_field": "datetime",
            "bool_field": "boolean",
        }

        schema = SchemaVersion(
            start_date=sample_dates["start"],
            end_date=None,
            columns=basic_schema,
            version=1,
        )

        struct_type = schema.struct_type
        assert isinstance(struct_type, StructType)

        # Verify each field type
        type_mapping = {
            "string_field": StringType,
            "int_field": IntegerType,
            "long_field": LongType,
            "double_field": DoubleType,
            "decimal_field": DecimalType,
            "date_field": DateType,
            "datetime_field": TimestampType,
            "bool_field": BooleanType,
        }

        for field in struct_type.fields:
            assert isinstance(field.dataType, type_mapping[field.name])
            assert field.nullable  # Default nullable=True

    def test_struct_type_with_nullable_specs(self, sample_dates, complex_schema):
        """Test conversion with explicit nullable specifications"""
        schema = SchemaVersion(
            start_date=sample_dates["start"],
            end_date=None,
            columns=complex_schema,
            version=1,
        )

        struct_type = schema.struct_type

        # Verify nullable specifications
        field_dict = {field.name: field for field in struct_type.fields}
        assert not field_dict["id"].nullable
        assert field_dict["name"].nullable
        assert not field_dict["status"].nullable
        assert field_dict["created_at"].nullable

    def test_invalid_data_type(self, sample_dates):
        """Test handling of invalid data type"""
        invalid_schema = {"field1": "invalid_type"}

        schema = SchemaVersion(
            start_date=sample_dates["start"],
            end_date=None,
            columns=invalid_schema,
            version=1,
        )

        with pytest.raises(ValueError) as exc_info:
            _ = schema.struct_type
        assert "Unsupported data type" in str(exc_info.value)

    def test_case_insensitivity(self, sample_dates):
        """Test case insensitive handling of data types"""
        mixed_case_schema = {
            "field1": "STRING",
            "field2": "Integer",
            "field3": "BOOLEAN",
        }

        schema = SchemaVersion(
            start_date=sample_dates["start"],
            end_date=None,
            columns=mixed_case_schema,
            version=1,
        )

        struct_type = schema.struct_type
        field_dict = {field.name: field for field in struct_type.fields}

        assert isinstance(field_dict["field1"].dataType, StringType)
        assert isinstance(field_dict["field2"].dataType, IntegerType)
        assert isinstance(field_dict["field3"].dataType, BooleanType)

    def test_empty_schema(self, sample_dates):
        """Test handling of empty schema"""
        schema = SchemaVersion(
            start_date=sample_dates["start"], end_date=None, columns={}, version=1
        )

        struct_type = schema.struct_type
        assert len(struct_type.fields) == 0

    def test_schema_with_mixed_format(self, sample_dates):
        """Test schema with mixed format (simple strings and dicts)"""
        mixed_schema = {
            "id": {"type": "integer", "nullable": False},
            "name": "string",
            "status": {"type": "string", "nullable": True},
            "age": "integer",
        }

        schema = SchemaVersion(
            start_date=sample_dates["start"],
            end_date=None,
            columns=mixed_schema,
            version=1,
        )

        struct_type = schema.struct_type
        field_dict = {field.name: field for field in struct_type.fields}

        assert not field_dict["id"].nullable
        assert field_dict["name"].nullable  # Default True
        assert field_dict["status"].nullable
        assert field_dict["age"].nullable  # Default True

    @pytest.mark.parametrize("version", [1, 2, 100])
    def test_different_versions(self, sample_dates, sample_schema, version):
        """Test different version numbers"""
        schema = SchemaVersion(
            start_date=sample_dates["start"],
            end_date=sample_dates["end"],
            columns=sample_schema,
            version=version,
        )

        assert schema.version == version
        assert f"Schema V{version}" in str(schema)
