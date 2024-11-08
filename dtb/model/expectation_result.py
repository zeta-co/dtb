class ExpectationResult:
    def __init__(self, passed, failed_count=0, failed_records=None, failure_reason=None):
        self.passed = passed
        self.failed_count = failed_count
        self.failed_records = failed_records or []
        self.failure_reason = failure_reason

    def __bool__(self):
        return self.passed

    def __str__(self):
        if self.passed:
            return "Validation passed"
        else:
            return f"Validation failed: {self.failed_count} records failed. Reason: {self.failure_reason}"