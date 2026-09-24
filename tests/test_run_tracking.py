import json
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

from run_tracking import (
    RunContext,
    RunTracker,
    get_snowflake_options,
    metrics_to_json,
    parse_optional_int,
    parse_utc_datetime,
    run_tracking,
    sanitize_error_message,
)


class RunTrackingTests(unittest.TestCase):
    def test_dynamic_integer_values(self):
        self.assertEqual(parse_optional_int("2"), 2)
        self.assertIsNone(parse_optional_int(""))
        self.assertIsNone(parse_optional_int("{{task.execution_count}}"))

    def test_utc_timestamp_parsing(self):
        parsed = parse_utc_datetime("2026-09-22T12:00:00Z")
        self.assertIsNotNone(parsed)
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
            pipeline_name="TEST_PIPELINE",
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
        context = RunContext(pipeline_name="TEST_PIPELINE", environment="PROD")
        record = context.build_record(
            status="SUCCEEDED",
            source_rows=Decimal("100"),
            staging_rows=Decimal("100.000"),
            transformed_rows=Decimal("100"),
            target_rows_before=Decimal("100"),
            target_rows_after=Decimal("100"),
        )
        for field_name in (
            "SOURCE_ROWS",
            "STAGING_ROWS",
            "TRANSFORMED_ROWS",
            "TARGET_ROWS_BEFORE",
            "TARGET_ROWS_AFTER",
            "TARGET_ROW_DELTA",
        ):
            self.assertIsInstance(record[field_name], int)

    def test_non_integral_row_count_is_rejected(self):
        context = RunContext(pipeline_name="TEST_PIPELINE", environment="PROD")
        with self.assertRaises(ValueError):
            context.build_record(status="SUCCEEDED", source_rows=Decimal("1.5"))

    def test_run_tracker_context_manager_success(self):
        mock_spark = MagicMock()
        mock_sf_options = {"sfURL": "test.snowflakecomputing.com"}

        with patch("run_tracking.append_run_record_safely") as mock_append:
            with run_tracking(
                spark=mock_spark,
                snowflake_options=mock_sf_options,
                pipeline_name="UNIT_TEST_PIPELINE",
                table_name="PRD_MDP.MDP_STG.PIPELINE_RUNS",
            ) as tracker:
                tracker.source_rows = 100
                tracker.transformed_rows = 95
                tracker.set_metrics(custom_key="custom_value")

            mock_append.assert_called_once()
            record = mock_append.call_args[1]["record"]
            self.assertEqual(record["STATUS"], "SUCCEEDED")
            self.assertEqual(record["PIPELINE_NAME"], "UNIT_TEST_PIPELINE")
            self.assertEqual(record["SOURCE_ROWS"], 100)
            self.assertEqual(record["TRANSFORMED_ROWS"], 95)
            self.assertIn("custom_key", record["BUSINESS_METRICS_JSON"])

    def test_run_tracker_finish_method(self):
        mock_spark = MagicMock()
        mock_sf_options = {"sfURL": "test.snowflakecomputing.com"}

        with patch("run_tracking.append_run_record_safely") as mock_append:
            tracker = run_tracking(
                spark=mock_spark,
                snowflake_options=mock_sf_options,
                pipeline_name="FINISH_TEST_PIPELINE",
            )
            tracker.source_rows = 500
            result = tracker.finish()

            self.assertTrue(result)
            mock_append.assert_called_once()
            record = mock_append.call_args[1]["record"]
            self.assertEqual(record["STATUS"], "SUCCEEDED")
            self.assertEqual(record["SOURCE_ROWS"], 500)

    def test_run_tracker_context_manager_failure_reraises(self):
        mock_spark = MagicMock()
        mock_sf_options = {"sfURL": "test.snowflakecomputing.com"}

        with patch("run_tracking.append_run_record_safely") as mock_append:
            with self.assertRaises(ZeroDivisionError):
                with run_tracking(
                    spark=mock_spark,
                    snowflake_options=mock_sf_options,
                    pipeline_name="FAILING_PIPELINE",
                ) as tracker:
                    _ = 1 / 0

            mock_append.assert_called_once()
            record = mock_append.call_args[1]["record"]
            self.assertEqual(record["STATUS"], "FAILED")
            self.assertEqual(record["ERROR_TYPE"], "ZeroDivisionError")
            self.assertIn("division by zero", record["ERROR_MESSAGE"])

    def test_telemetry_failure_does_not_crash_pipeline(self):
        mock_spark = MagicMock()
        mock_sf_options = {"sfURL": "test.snowflakecomputing.com"}

        with patch("run_tracking.append_run_record", side_effect=RuntimeError("Snowflake down")):
            tracker = run_tracking(
                spark=mock_spark,
                snowflake_options=mock_sf_options,
                pipeline_name="RESILIENCE_TEST",
            )
            # Should log error and return safely without throwing
            res = tracker.finish()
            self.assertTrue(res)

    def test_get_snowflake_options_missing_dbutils(self):
        with self.assertRaises(ValueError):
            get_snowflake_options(None)


if __name__ == "__main__":
    unittest.main()
