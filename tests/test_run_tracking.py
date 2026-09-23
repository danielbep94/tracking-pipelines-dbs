import json
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

from src.run_tracking import (
    RunContext,
    metrics_to_json,
    parse_optional_int,
    parse_utc_datetime,
    sanitize_error_message,
)


class RunTrackingTests(unittest.TestCase):
    def test_dynamic_integer_values(self):
        self.assertEqual(parse_optional_int("2"), 2)
        self.assertIsNone(parse_optional_int(""))
        self.assertIsNone(parse_optional_int("{{task.execution_count}}"))

    def test_utc_timestamp_parsing(self):
        parsed = parse_utc_datetime("2026-09-22T12:00:00Z")
        self.assertEqual(parsed.tzinfo, timezone.utc)
        self.assertEqual(parsed.hour, 12)
        self.assertIsNone(parse_utc_datetime("{{job.start_time.iso_datetime}}"))

    def test_sensitive_error_values_are_redacted(self):
        message = sanitize_error_message(
            "connection failed password=secret123 token:abc456"
        )
        self.assertNotIn("secret123", message)
        self.assertNotIn("abc456", message)
        self.assertIn("[REDACTED]", message)

    def test_business_metrics_are_stable_json(self):
        value = metrics_to_json(
            {
                "distinct_employees": Decimal("125"),
                "period": date(2026, 8, 1),
            }
        )
        decoded = json.loads(value)
        self.assertEqual(decoded["distinct_employees"], 125.0)
        self.assertEqual(decoded["period"], "2026-08-01")

    def test_run_record_calculates_duration_and_target_delta(self):
        started = datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)
        completed = datetime(2026, 9, 22, 12, 1, 6, tzinfo=timezone.utc)
        context = RunContext(
            pipeline_name="CEO_ASISTENCIA",
            environment="PROD",
            started_at_utc=started,
        )
        record = context.build_record(
            status="SUCCEEDED",
            target_rows_before=100,
            target_rows_after=100,
            completed_at_utc=completed,
        )
        self.assertEqual(record["DURATION_SECONDS"], 66.0)
        self.assertEqual(record["TARGET_ROW_DELTA"], 0)
        self.assertEqual(record["STATUS"], "SUCCEEDED")

    def test_decimal_counts_are_normalized_to_integers(self):
        context = RunContext(pipeline_name="CEO_ASISTENCIA", environment="PROD")
        record = context.build_record(
            status="SUCCEEDED",
            source_rows=Decimal("100"),
            staging_rows=Decimal("100.000"),
            transformed_rows=Decimal("100"),
            target_rows_before=Decimal("100"),
            target_rows_after=Decimal("100"),
        )
        for field in (
            "SOURCE_ROWS",
            "STAGING_ROWS",
            "TRANSFORMED_ROWS",
            "TARGET_ROWS_BEFORE",
            "TARGET_ROWS_AFTER",
            "TARGET_ROW_DELTA",
        ):
            self.assertIsInstance(record[field], int)

    def test_non_integral_row_count_is_rejected(self):
        context = RunContext(pipeline_name="CEO_ASISTENCIA", environment="PROD")
        with self.assertRaises(ValueError):
            context.build_record(status="SUCCEEDED", source_rows=Decimal("1.5"))


if __name__ == "__main__":
    unittest.main()
