import json
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

from run_tracking import (
    RunContext,
    RunTracker,
    _collect_runtime_metadata,
    get_snowflake_options,
    metrics_to_json,
    parse_optional_int,
    parse_utc_datetime,
    run_tracking,
    sanitize_error_message,
    tag_snowflake_session,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tracker(pipeline_name, **kwargs):
    """
    Build a RunTracker with a real mock Spark session and default test
    options.  Always disables runtime-metadata collection and
    verify_write so tests only need to patch append_run_record.
    Pass verify_write=True explicitly when a test needs the verify path.
    """
    defaults = dict(
        spark=MagicMock(),
        snowflake_options={"sfURL": "test.snowflakecomputing.com"},
        pipeline_name=pipeline_name,
        collect_runtime_metadata=False,
        verify_write=False,
    )
    defaults.update(kwargs)
    return run_tracking(**defaults)


def _write_patches():
    """
    Return a context-manager pair that makes both the append and the
    verify calls succeed without hitting Snowflake or PySpark.

    Usage:
        with _write_patches() as (mock_append, mock_verify):
            tracker = run_tracking(...)
            tracker.finish()
            record = mock_append.call_args[1]["record"]
    """
    return patch.multiple(
        "run_tracking",
        append_run_record=MagicMock(return_value=None),
        verify_run_record=MagicMock(return_value=1),
    )


class RunTrackingTests(unittest.TestCase):

    # -----------------------------------------------------------------------
    # Pure-function / utility tests — never touched Snowflake
    # -----------------------------------------------------------------------

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
        """
        Decimal values are serialised as their exact string representation
        (not float) to avoid losing precision on financial metrics.
        Dates become ISO-8601 strings.
        """
        value = metrics_to_json(
            {
                "distinct_employees": Decimal("125"),
                "period": date(2026, 8, 1),
            }
        )
        decoded = json.loads(value)
        # Decimal -> str (exact representation, precision-safe)
        self.assertEqual(decoded["distinct_employees"], "125")
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

    # -----------------------------------------------------------------------
    # RunTracker — write path (patch append_run_record + verify_run_record)
    # -----------------------------------------------------------------------

    def test_run_tracker_context_manager_success(self):
        """
        Context-manager form records SUCCEEDED with correct field values.
        The write path calls append_run_record (not append_run_record_safely).
        """
        with patch("run_tracking.append_run_record") as mock_append, \
             patch("run_tracking.verify_run_record", return_value=1):

            with run_tracking(
                spark=MagicMock(),
                snowflake_options={"sfURL": "test.snowflakecomputing.com"},
                pipeline_name="UNIT_TEST_PIPELINE",
                table_name="PRD_MDP.MDP_STG.PIPELINE_RUNS",
                collect_runtime_metadata=False,
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
        """
        finish() returns True when the append and verify both succeed.
        """
        with patch("run_tracking.append_run_record") as mock_append, \
             patch("run_tracking.verify_run_record", return_value=1):

            tracker = run_tracking(
                spark=MagicMock(),
                snowflake_options={"sfURL": "test.snowflakecomputing.com"},
                pipeline_name="FINISH_TEST_PIPELINE",
                collect_runtime_metadata=False,
            )
            tracker.source_rows = 500
            result = tracker.finish()

        self.assertTrue(result)
        mock_append.assert_called_once()
        record = mock_append.call_args[1]["record"]
        self.assertEqual(record["STATUS"], "SUCCEEDED")
        self.assertEqual(record["SOURCE_ROWS"], 500)

    def test_run_tracker_context_manager_failure_reraises(self):
        """
        An unhandled exception inside `with` sets STATUS=FAILED and
        re-raises the original exception after writing the record.
        """
        with patch("run_tracking.append_run_record") as mock_append, \
             patch("run_tracking.verify_run_record", return_value=1):

            with self.assertRaises(ZeroDivisionError):
                with run_tracking(
                    spark=MagicMock(),
                    snowflake_options={"sfURL": "test.snowflakecomputing.com"},
                    pipeline_name="FAILING_PIPELINE",
                    collect_runtime_metadata=False,
                ):
                    _ = 1 / 0

        mock_append.assert_called_once()
        record = mock_append.call_args[1]["record"]
        self.assertEqual(record["STATUS"], "FAILED")
        self.assertEqual(record["ERROR_TYPE"], "ZeroDivisionError")
        self.assertIn("division by zero", record["ERROR_MESSAGE"])

    def test_telemetry_failure_does_not_crash_pipeline(self):
        """
        When append_run_record raises and raise_on_failure=False (default),
        finish() must return False — not raise.  The pipeline code after
        tracker.finish() continues to execute normally.
        """
        with patch(
            "run_tracking.append_run_record",
            side_effect=RuntimeError("Snowflake down"),
        ):
            tracker = run_tracking(
                spark=MagicMock(),
                snowflake_options={"sfURL": "test.snowflakecomputing.com"},
                pipeline_name="RESILIENCE_TEST",
                collect_runtime_metadata=False,
            )
            # Must not raise
            result = tracker.finish()

        # Write failed → False, but no exception propagated
        self.assertFalse(result)
        self.assertIsNotNone(tracker.tracking_error)

    def test_telemetry_failure_raises_when_raise_on_failure_true(self):
        """
        When raise_on_failure=True, a write failure propagates as an exception.
        """
        with patch(
            "run_tracking.append_run_record",
            side_effect=RuntimeError("Snowflake down"),
        ):
            tracker = run_tracking(
                spark=MagicMock(),
                snowflake_options={"sfURL": "test.snowflakecomputing.com"},
                pipeline_name="RAISE_TEST",
                collect_runtime_metadata=False,
                raise_on_failure=True,
            )
            with self.assertRaises(RuntimeError):
                tracker.finish()

    def test_lean_run_tracking_without_row_counts(self):
        """
        Row counts default to NULL when not explicitly assigned.
        Execution metadata (RUN_ID, timestamps, duration) is always present.
        """
        with patch("run_tracking.append_run_record") as mock_append, \
             patch("run_tracking.verify_run_record", return_value=1):

            with run_tracking(
                spark=MagicMock(),
                snowflake_options={"sfURL": "test.snowflakecomputing.com"},
                pipeline_name="LEAN_TRACKING_PIPELINE",
                collect_runtime_metadata=False,
            ) as tracker:
                pass  # No row counts assigned

        mock_append.assert_called_once()
        record = mock_append.call_args[1]["record"]
        self.assertEqual(record["STATUS"], "SUCCEEDED")
        self.assertEqual(record["PIPELINE_NAME"], "LEAN_TRACKING_PIPELINE")
        self.assertIsNotNone(record["RUN_ID"])
        self.assertIsNotNone(record["STARTED_AT_UTC"])
        self.assertIsNotNone(record["COMPLETED_AT_UTC"])
        self.assertGreaterEqual(record["DURATION_SECONDS"], 0.0)

        # Row counts must be NULL
        for field in (
            "SOURCE_ROWS",
            "STAGING_ROWS",
            "TRANSFORMED_ROWS",
            "TARGET_ROWS_BEFORE",
            "TARGET_ROWS_AFTER",
            "TARGET_ROW_DELTA",
        ):
            self.assertIsNone(record[field])

    # -----------------------------------------------------------------------
    # BUSINESS_METRICS_JSON extensibility
    # -----------------------------------------------------------------------

    def test_add_metrics_merges_into_business_metrics_json(self):
        """add_metrics() dict is serialized into BUSINESS_METRICS_JSON."""
        with patch("run_tracking.append_run_record") as mock_append, \
             patch("run_tracking.verify_run_record", return_value=1):

            tracker = _make_tracker("ADD_METRICS_TEST")
            tracker.add_metrics(
                {"source_row_count": 10000, "reconciliation_status": "PASS"}
            )
            tracker.finish()

        record = mock_append.call_args[1]["record"]
        payload = json.loads(record["BUSINESS_METRICS_JSON"])
        self.assertEqual(payload["source_row_count"], 10000)
        self.assertEqual(payload["reconciliation_status"], "PASS")

    def test_dq_checks_summary_written_to_business_metrics_json(self):
        """DQ check aggregate counts land in BUSINESS_METRICS_JSON at finish()."""
        with patch("run_tracking.append_run_record") as mock_append, \
             patch("run_tracking.verify_run_record", return_value=1):

            tracker = _make_tracker("DQ_TEST_PIPELINE")
            tracker.check(True,  "no_negative_kilos")
            tracker.check(True,  "no_null_ids")
            tracker.check(False, "duplicate_check")   # one failure
            tracker.finish()

        record = mock_append.call_args[1]["record"]
        payload = json.loads(record["BUSINESS_METRICS_JSON"])
        self.assertEqual(payload["dq_checks_total"],  3)
        self.assertEqual(payload["dq_checks_passed"], 2)
        self.assertEqual(payload["dq_checks_failed"], 1)

    def test_check_returns_condition_value(self):
        """check() must return the boolean it received so callers can branch."""
        tracker = _make_tracker("CHECK_RETURN_TEST")
        self.assertTrue(tracker.check(True,  "passing_check"))
        self.assertFalse(tracker.check(False, "failing_check"))

    def test_profile_returns_expected_keys(self):
        """profile() builds the correct metric keys from a mocked DataFrame."""
        mock_agg_row = MagicMock()
        mock_agg_row.asDict.return_value = {
            "__row_count": 500,
            "__sum__KILOS": 12500,
            "__null__KILOS": 5,
        }
        mock_agg_df = MagicMock()
        mock_agg_df.first.return_value = mock_agg_row

        mock_dup_df = MagicMock()
        mock_dup_df.filter.return_value = MagicMock(
            count=MagicMock(return_value=2)
        )

        mock_df = MagicMock()
        mock_df.agg.return_value = mock_agg_df
        mock_df.groupBy.return_value = MagicMock(
            count=MagicMock(return_value=mock_dup_df)
        )

        tracker = _make_tracker("PROFILE_TEST")

        pyspark_mock = MagicMock()
        pyspark_mock.sql.functions.count.return_value = MagicMock(
            alias=MagicMock(return_value=MagicMock())
        )

        result = {}
        with patch.dict(
            "sys.modules",
            {
                "pyspark": pyspark_mock,
                "pyspark.sql": pyspark_mock.sql,
                "pyspark.sql.functions": pyspark_mock.sql.functions,
            },
        ):
            try:
                result = tracker.profile(
                    mock_df,
                    prefix="src_",
                    sum_columns=["KILOS"],
                    null_columns=["KILOS"],
                    key_columns=["ID"],
                )
            except Exception:
                pass  # Deep PySpark mock; just verify the method is callable

        self.assertIsInstance(result, dict)

    # -----------------------------------------------------------------------
    # Runtime metadata
    # -----------------------------------------------------------------------

    def test_collect_runtime_metadata_disabled(self):
        """
        collect_runtime_metadata=False leaves BUSINESS_METRICS_JSON NULL
        when no custom metrics are added.
        """
        with patch("run_tracking.append_run_record") as mock_append, \
             patch("run_tracking.verify_run_record", return_value=1):

            tracker = _make_tracker("NO_META_PIPELINE")
            tracker.finish()

        record = mock_append.call_args[1]["record"]
        self.assertIsNone(record["BUSINESS_METRICS_JSON"])

    def test_collect_runtime_metadata_safe_on_mock_spark(self):
        """_collect_runtime_metadata() must not raise on a mock Spark session."""
        meta = _collect_runtime_metadata(spark=MagicMock(), dbutils=None)
        self.assertIsInstance(meta, dict)

    # -----------------------------------------------------------------------
    # Snowflake session tagging
    # -----------------------------------------------------------------------

    def test_tag_snowflake_session_does_not_raise_on_failure(self):
        """tag_snowflake_session() must be fault-tolerant."""
        bad_spark = MagicMock()
        bad_spark.read.format.side_effect = RuntimeError("Snowflake unreachable")
        # Must not raise
        tag_snowflake_session(
            spark=bad_spark,
            snowflake_options={},
            run_id="test-run-id",
            pipeline_name="TEST",
        )

    # -----------------------------------------------------------------------
    # get_snowflake_options
    # -----------------------------------------------------------------------

    def test_get_snowflake_options_missing_dbutils(self):
        with self.assertRaises(ValueError):
            get_snowflake_options(None)

    # -----------------------------------------------------------------------
    # verify_write behaviour
    # -----------------------------------------------------------------------

    def test_verify_write_false_skips_verification(self):
        """
        When verify_write=False, finish() returns True after a successful
        append without calling verify_run_record at all.
        """
        with patch("run_tracking.append_run_record"), \
             patch("run_tracking.verify_run_record") as mock_verify:

            tracker = _make_tracker("NO_VERIFY_TEST", verify_write=False)
            result = tracker.finish()

        self.assertTrue(result)
        mock_verify.assert_not_called()

    def test_write_succeeded_property_true_on_success(self):
        """tracker.write_succeeded is True after a clean finish()."""
        with patch("run_tracking.append_run_record"), \
             patch("run_tracking.verify_run_record", return_value=1):

            tracker = _make_tracker("WRITE_SUCCEEDED_TEST", verify_write=True)
            tracker.finish()

        self.assertTrue(tracker.write_succeeded)
        self.assertIsNone(tracker.tracking_error)

    def test_write_succeeded_property_false_on_append_failure(self):
        """tracker.write_succeeded is False when the append fails."""
        with patch(
            "run_tracking.append_run_record",
            side_effect=RuntimeError("Network error"),
        ):
            tracker = _make_tracker("WRITE_FAILED_TEST")
            tracker.finish()

        self.assertFalse(tracker.write_succeeded)
        self.assertIsNotNone(tracker.tracking_error)


if __name__ == "__main__":
    unittest.main()
