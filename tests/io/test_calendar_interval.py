import pytest
from dtb.io.calendar_interval import CalendarInterval


class TestCalendarInterval:
    @pytest.mark.parametrize(
        "interval_str,expected_valid",
        [
            ("interval 1 week", True),
            ("interval 2 days", True),
            ("interval 1 year 6 months", True),
            ("interval 24 hours", True),
            ("interval 30 minutes", True),
            ("interval 1 year 2 months 3 weeks 4 days 5 hours 6 minutes 7 seconds", True),
            ("1 week", False),
            ("invalid interval", False),
            ("interval abc", False),
            ("interval -1 week", False),
            ("interval 1.5 weeks", False),
        ]
    )
    def test_calendar_interval_validation(self, interval_str: str, expected_valid: bool):
        if expected_valid:
            interval = CalendarInterval(interval_str)
            assert str(interval) == interval_str
        else:
            with pytest.raises(ValueError):
                CalendarInterval(interval_str)

    def test_calendar_interval_timedelta(self):
        interval = CalendarInterval("interval 1 week 2 days")
        assert interval.timedelta.days == 9
