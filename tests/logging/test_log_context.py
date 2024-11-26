import pytest
from dtb.logging.log_context import (
    LogContext,
)  # Assuming the class is in log_context.py


class TestLogContext:
    @pytest.fixture
    def base_context(self) -> LogContext:
        """Fixture providing a basic LogContext instance for tests"""
        return LogContext(
            job_id="test-job",
            job_name="Test Job",
            run_id="test-run",
            table_name="test_table",
            table_path="/path/to/table",
        )

    def test_init_required_fields(self):
        """Test initialization with required fields"""
        context = LogContext(
            job_id="test-job",
            job_name="Test Job",
            run_id="test-run",
            table_name="test_table",
            table_path="/path/to/table",
        )

        assert context.job_id == "test-job"
        assert context.job_name == "Test Job"
        assert context.run_id == "test-run"
        assert context.table_name == "test_table"
        assert context.table_path == "/path/to/table"

    def test_init_with_metadata(self):
        """Test initialization with additional metadata"""
        metadata = {"extra_field": "value", "number": 42}
        context = LogContext(
            job_id="test-job",
            job_name="Test Job",
            run_id="test-run",
            table_name="test_table",
            table_path="/path/to/table",
            metadata=metadata,
        )

        assert context["extra_field"] == "value"
        assert context["number"] == 42
        assert context.job_id == "test-job"

    def test_init_with_kwargs(self):
        """Test initialization with kwargs"""
        context = LogContext(
            job_id="test-job",
            job_name="Test Job",
            run_id="test-run",
            table_name="test_table",
            table_path="/path/to/table",
            custom_field="custom",
        )

        assert context["custom_field"] == "custom"
        assert context.job_id == "test-job"

    def test_property_access(self, base_context):
        """Test property access for core fields"""
        assert base_context.job_id == "test-job"
        assert base_context.job_name == "Test Job"
        assert base_context.run_id == "test-run"
        assert base_context.table_name == "test_table"
        assert base_context.table_path == "/path/to/table"

    def test_to_dict_all_fields(self, base_context):
        """Test converting to dictionary with all fields"""
        result = base_context.to_dict()

        assert result["job_id"] == "test-job"
        assert result["job_name"] == "Test Job"
        assert result["run_id"] == "test-run"
        assert result["table_name"] == "test_table"
        assert result["table_path"] == "/path/to/table"

    def test_to_dict_include_fields(self, base_context):
        """Test converting to dictionary with included fields"""
        result = base_context.to_dict(include_fields={"job_id", "run_id"})

        assert set(result.keys()) == {"job_id", "run_id"}
        assert result["job_id"] == "test-job"
        assert result["run_id"] == "test-run"

    def test_to_dict_exclude_fields(self, base_context):
        """Test converting to dictionary with excluded fields"""
        result = base_context.to_dict(exclude_fields={"job_id", "run_id"})

        assert "job_id" not in result
        assert "run_id" not in result
        assert "job_name" in result
        assert "table_name" in result
        assert "table_path" in result

    def test_dictionary_operations(self, base_context):
        """Test dictionary-like operations"""
        # Test __getitem__
        assert base_context["job_id"] == "test-job"

        # Test __setitem__
        base_context["new_field"] = "new_value"
        assert base_context["new_field"] == "new_value"

        # Test __contains__
        assert "job_id" in base_context
        assert "non_existent" not in base_context

        # Test get() with default
        assert base_context.get("job_id") == "test-job"
        assert base_context.get("non_existent", "default") == "default"

        # Test keys(), values(), items()
        assert "job_id" in base_context.keys()
        assert "test-job" in base_context.values()
        assert ("job_id", "test-job") in base_context.items()

    def test_iteration(self, base_context):
        """Test iteration over context"""
        fields = set()
        for field in base_context:
            fields.add(field)

        assert "job_id" in fields
        assert "job_name" in fields
        assert "run_id" in fields
        assert "table_name" in fields
        assert "table_path" in fields

    def test_error_cases(self, base_context):
        """Test error cases"""
        # Test accessing non-existent key
        with pytest.raises(KeyError):
            _ = base_context["non_existent"]

        # Test to_dict with invalid field names
        result = base_context.to_dict(include_fields={"non_existent"})
        assert len(result) == 0
