"""Unit tests for RetailPulse Batch Ingestion Pipeline Orchestrator."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from ingestion.batch.config import (
    BatchPipelineConfig,
    ConfigurationError,
    DatabaseConfig,
    GCSConfig,
)
from ingestion.batch.gcs_uploader import GCSBatchUploader, GCSUploadError, UploadResult
from ingestion.batch.pipeline import BatchIngestionPipeline
from ingestion.batch.postgres_extractor import (
    ExtractionResult,
    PostgresBatchExtractor,
    PostgresExtractionError,
)


class TestBatchIngestionPipeline(unittest.TestCase):
    """Test suite for pipeline orchestration, error recovery, and dry-run safety."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.staging_dir = Path(self.temp_dir.name)

        self.db_config = DatabaseConfig(
            host="localhost",
            port=5433,
            dbname="retailpulse",
            user="retailpulse_user",
            password="test_password",
        )
        self.gcs_config = GCSConfig(
            project_id="retailpulse-project",
            bucket_name="retailpulse-landing",
            base_prefix="raw",
        )
        self.pipeline_config = BatchPipelineConfig(
            database=self.db_config,
            gcs=self.gcs_config,
            local_staging_dir=self.staging_dir,
            tables=("customers", "orders"),
            ingestion_date="2026-11-02",
        )

        self.mock_extractor = MagicMock(spec=PostgresBatchExtractor)
        self.mock_uploader = MagicMock(spec=GCSBatchUploader)

        self.pipeline = BatchIngestionPipeline(
            config=self.pipeline_config,
            extractor=self.mock_extractor,
            uploader=self.mock_uploader,
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_run_success_all_tables(self) -> None:
        """Verify successful end-to-end extraction and upload of all configured tables."""
        # Setup mock return values
        csv_customers = self.staging_dir / "customers.csv"
        csv_customers.touch()
        csv_orders = self.staging_dir / "orders.csv"
        csv_orders.touch()

        self.mock_extractor.extract_table.side_effect = [
            ExtractionResult("customers", csv_customers, 1000, 50000, 0.5, ("id", "name")),
            ExtractionResult("orders", csv_orders, 10000, 300000, 1.2, ("id", "total")),
        ]

        self.mock_uploader.upload_file.side_effect = [
            UploadResult("customers", csv_customers, "bucket", "blob1", "gs://bucket/blob1", 50000, 0.2),
            UploadResult("orders", csv_orders, "bucket", "blob2", "gs://bucket/blob2", 300000, 0.4),
        ]

        summary = self.pipeline.run(dry_run=False, cleanup_staging=False)

        self.assertTrue(summary.is_success)
        self.assertEqual(summary.tables_attempted, 2)
        self.assertEqual(summary.tables_succeeded, 2)
        self.assertEqual(summary.tables_failed, 0)
        self.assertEqual(summary.total_rows, 11000)
        self.assertEqual(len(summary.metrics), 2)

        # Check independent duration metrics are measured
        for m in summary.metrics:
            self.assertGreaterEqual(m.extraction_duration, 0.0)
            self.assertGreaterEqual(m.upload_duration, 0.0)
            self.assertGreaterEqual(m.total_duration, 0.0)
            self.assertEqual(m.status, "SUCCESS")

    def test_dry_run_mode_skips_uploader_and_returns_no_fake_gcs_uri(self) -> None:
        """Verify dry-run mode extracts data locally, skips GCS, and leaves gcs_uri as None."""
        csv_customers = self.staging_dir / "customers.csv"
        csv_customers.touch()

        self.mock_extractor.extract_table.return_value = ExtractionResult(
            "customers", csv_customers, 1000, 50000, 0.4, ("id", "name")
        )

        # In dry run, uploader should never be invoked
        summary = self.pipeline.run(tables=("customers",), dry_run=True)

        self.assertTrue(summary.is_success)
        self.assertEqual(summary.tables_succeeded, 1)
        self.mock_uploader.upload_file.assert_not_called()

        metric = summary.metrics[0]
        self.assertEqual(metric.table_name, "customers")
        self.assertEqual(metric.status, "SUCCESS")
        self.assertEqual(metric.upload_duration, 0.0)
        # Verify no fake GCS URI is reported
        self.assertIsNone(metric.gcs_uri)

    def test_extraction_failure_is_captured_and_pipeline_fails(self) -> None:
        """Verify extraction failure marks table as FAILED and summary is_success=False."""
        self.mock_extractor.extract_table.side_effect = PostgresExtractionError("DB connection failed")

        summary = self.pipeline.run(tables=("customers",), dry_run=False)

        self.assertFalse(summary.is_success)
        self.assertEqual(summary.tables_failed, 1)
        self.assertEqual(summary.tables_succeeded, 0)
        self.mock_uploader.upload_file.assert_not_called()

        metric = summary.metrics[0]
        self.assertEqual(metric.status, "FAILED")
        self.assertIn("Extraction error", metric.error_message or "")

    def test_upload_failure_is_captured_and_pipeline_fails(self) -> None:
        """Verify GCS upload failure marks table as FAILED and summary is_success=False."""
        csv_customers = self.staging_dir / "customers.csv"
        csv_customers.touch()

        self.mock_extractor.extract_table.return_value = ExtractionResult(
            "customers", csv_customers, 1000, 50000, 0.5, ("id", "name")
        )
        self.mock_uploader.upload_file.side_effect = GCSUploadError("Bucket quota exceeded")

        summary = self.pipeline.run(tables=("customers",), dry_run=False)

        self.assertFalse(summary.is_success)
        self.assertEqual(summary.tables_failed, 1)
        self.assertEqual(summary.tables_succeeded, 0)

        metric = summary.metrics[0]
        self.assertEqual(metric.status, "FAILED")
        self.assertIn("Upload error", metric.error_message or "")

    def test_cleanup_staging_error_does_not_fail_successful_upload(self) -> None:
        """Verify failure to delete staging file does NOT fail the successful upload."""
        csv_customers = self.staging_dir / "customers.csv"
        csv_customers.touch()

        self.mock_extractor.extract_table.return_value = ExtractionResult(
            "customers", csv_customers, 1000, 50000, 0.5, ("id", "name")
        )
        self.mock_uploader.upload_file.return_value = UploadResult(
            "customers", csv_customers, "bucket", "blob", "gs://bucket/blob", 50000, 0.3
        )

        # Mock Path.unlink to raise OSError (e.g., file lock on Windows)
        with patch.object(Path, "unlink", side_effect=OSError("File locked")):
            summary = self.pipeline.run(tables=("customers",), cleanup_staging=True)

        # Pipeline must still report SUCCESS because the upload succeeded
        self.assertTrue(summary.is_success)
        self.assertEqual(summary.metrics[0].status, "SUCCESS")

    def test_disallowed_table_name_rejected_by_pipeline(self) -> None:
        """Verify pipeline immediately rejects any table not in ALLOWED_SOURCE_TABLES."""
        with self.assertRaises(ConfigurationError):
            self.pipeline.run(tables=("invalid_table_name",))


if __name__ == "__main__":
    unittest.main()
