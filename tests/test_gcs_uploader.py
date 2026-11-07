"""Unit tests for Google Cloud Storage Batch Uploader."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from ingestion.batch.config import GCSConfig
from ingestion.batch.gcs_uploader import (
    GCSBatchUploader,
    GCSUploadError,
    GoogleCloudError,
    PreconditionFailed,
)


class TestGCSBatchUploader(unittest.TestCase):
    """Test suite for GCS upload, generation preconditions, and error safety."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.staging_dir = Path(self.temp_dir.name)
        self.sample_csv = self.staging_dir / "customers.csv"
        self.sample_csv.write_text("customer_id,name\n1,Alice\n", encoding="utf-8")

        self.gcs_config = GCSConfig(
            project_id="retailpulse-prod",
            bucket_name="retailpulse-bronze-bucket",
            base_prefix="raw",
            allow_overwrite=False,
        )
        self.mock_client = MagicMock()
        self.uploader = GCSBatchUploader(
            gcs_config=self.gcs_config,
            client=self.mock_client,
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_build_destination_blob_name(self) -> None:
        """Verify standard partitioned blob key generation."""
        blob_name = self.uploader.build_destination_blob_name("customers", "2026-11-02")
        self.assertEqual(blob_name, "raw/customers/ingestion_date=2026-11-02/customers.csv")

    def test_build_destination_blob_name_invalid_table(self) -> None:
        """Verify invalid table names raise GCSUploadError."""
        with self.assertRaises(GCSUploadError):
            self.uploader.build_destination_blob_name("unapproved_table", "2026-11-02")

    def test_upload_file_with_overwrite_disabled_uses_generation_precondition_0(self) -> None:
        """Verify upload uses if_generation_match=0 when allow_overwrite=False."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        self.mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        result = self.uploader.upload_file(
            local_path=self.sample_csv,
            destination_blob_name="raw/customers/ingestion_date=2026-11-02/customers.csv",
            table_name="customers",
            allow_overwrite=False,
        )

        mock_blob.upload_from_filename.assert_called_once_with(
            filename=str(self.sample_csv),
            content_type="text/csv",
            if_generation_match=0,
        )
        self.assertEqual(result.gcs_uri, "gs://retailpulse-bronze-bucket/raw/customers/ingestion_date=2026-11-02/customers.csv")
        self.assertEqual(result.bytes_uploaded, self.sample_csv.stat().st_size)

    def test_upload_file_with_overwrite_enabled_uses_generation_precondition_none(self) -> None:
        """Verify upload uses if_generation_match=None when allow_overwrite=True."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        self.mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        self.uploader.upload_file(
            local_path=self.sample_csv,
            destination_blob_name="raw/customers/ingestion_date=2026-11-02/customers.csv",
            table_name="customers",
            allow_overwrite=True,
        )

        mock_blob.upload_from_filename.assert_called_once_with(
            filename=str(self.sample_csv),
            content_type="text/csv",
            if_generation_match=None,
        )

    def test_upload_precondition_failed_maps_to_safe_error(self) -> None:
        """Verify PreconditionFailed (object already exists) raises clear GCSUploadError."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        self.mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        mock_blob.upload_from_filename.side_effect = PreconditionFailed("412 Precondition Failed")

        with self.assertRaises(GCSUploadError) as ctx:
            self.uploader.upload_file(
                local_path=self.sample_csv,
                destination_blob_name="raw/customers/ingestion_date=2026-11-02/customers.csv",
                table_name="customers",
                allow_overwrite=False,
            )
        self.assertIn("already exists and overwrite is disabled", str(ctx.exception))

    def test_upload_missing_local_file_raises_error(self) -> None:
        """Verify uploading non-existent file raises GCSUploadError before calling GCS API."""
        non_existent = self.staging_dir / "does_not_exist.csv"
        with self.assertRaises(GCSUploadError) as ctx:
            self.uploader.upload_file(
                local_path=non_existent,
                destination_blob_name="raw/customers/file.csv",
                table_name="customers",
            )
        self.assertIn("Staged file not found", str(ctx.exception))

    def test_upload_fails_if_gcs_config_invalid(self) -> None:
        """Verify upload aborts with clean error if bucket_name was not configured."""
        invalid_cfg = GCSConfig(project_id=None, bucket_name=None)
        uploader = GCSBatchUploader(gcs_config=invalid_cfg)
        with self.assertRaises(GCSUploadError) as ctx:
            uploader.upload_file(
                local_path=self.sample_csv,
                destination_blob_name="raw/test.csv",
                table_name="customers",
            )
        self.assertIn("Cannot upload file", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
