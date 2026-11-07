"""Unit tests for RetailPulse batch ingestion configuration module."""

from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from ingestion.batch.config import (
    ALLOWED_SOURCE_TABLES,
    BatchPipelineConfig,
    ConfigurationError,
    DatabaseConfig,
    GCSConfig,
    load_batch_pipeline_config,
    mask_secret,
    normalize_gcs_prefix,
    validate_ingestion_date,
    validate_source_tables,
)


class TestBatchConfig(unittest.TestCase):
    """Test suite for configuration dataclasses and validation rules."""

    def test_database_config_password_masking(self) -> None:
        """Ensure database password is masked in repr and not exposed."""
        db = DatabaseConfig(
            host="localhost",
            port=5433,
            dbname="retailpulse",
            user="retailpulse_user",
            password="SuperSecretPassword123!",
        )
        repr_str = repr(db)
        self.assertNotIn("SuperSecretPassword123!", repr_str)
        self.assertIn("password=***", repr_str)

    def test_database_config_connect_kwargs(self) -> None:
        """Ensure connect_kwargs provides dictionary of parameters for psycopg.connect()."""
        db = DatabaseConfig(
            host="db.example.com",
            port=5432,
            dbname="testdb",
            user="user1",
            password="secret_password",
            connect_timeout=15,
        )
        kwargs = db.connect_kwargs
        self.assertEqual(kwargs["host"], "db.example.com")
        self.assertEqual(kwargs["port"], 5432)
        self.assertEqual(kwargs["dbname"], "testdb")
        self.assertEqual(kwargs["user"], "user1")
        self.assertEqual(kwargs["password"], "secret_password")
        self.assertEqual(kwargs["connect_timeout"], 15)

    def test_database_config_invalid_ports(self) -> None:
        """Ensure invalid port numbers raise ConfigurationError."""
        with self.assertRaises(ConfigurationError):
            DatabaseConfig(password="pass", port=0)
        with self.assertRaises(ConfigurationError):
            DatabaseConfig(password="pass", port=-1)
        with self.assertRaises(ConfigurationError):
            DatabaseConfig(password="pass", port=65536)

    def test_database_config_empty_password(self) -> None:
        """Ensure missing password raises ConfigurationError."""
        with self.assertRaises(ConfigurationError):
            DatabaseConfig(password="")

    def test_validate_source_tables_valid(self) -> None:
        """Ensure valid tables pass validation."""
        valid = ("customers", "orders")
        result = validate_source_tables(valid)
        self.assertEqual(result, valid)

    def test_validate_source_tables_empty(self) -> None:
        """Ensure empty table list is rejected."""
        with self.assertRaises(ConfigurationError):
            validate_source_tables([])
        with self.assertRaises(ConfigurationError):
            validate_source_tables(["", "  "])

    def test_validate_source_tables_disallowed(self) -> None:
        """Ensure unapproved tables raise ConfigurationError."""
        with self.assertRaises(ConfigurationError) as ctx:
            validate_source_tables(["customers", "invalid_table"])
        self.assertIn("invalid_table", str(ctx.exception))
        self.assertIn("Only the following tables are permitted", str(ctx.exception))

    def test_validate_ingestion_date_valid(self) -> None:
        """Ensure valid YYYY-MM-DD date strings pass."""
        self.assertEqual(validate_ingestion_date("2026-11-02"), "2026-11-02")
        self.assertEqual(validate_ingestion_date("2024-02-29"), "2024-02-29")  # Leap year

    def test_validate_ingestion_date_invalid(self) -> None:
        """Ensure malformed or impossible dates raise ConfigurationError."""
        with self.assertRaises(ConfigurationError):
            validate_ingestion_date("2026/11/02")
        with self.assertRaises(ConfigurationError):
            validate_ingestion_date("2026-11-2")
        with self.assertRaises(ConfigurationError):
            validate_ingestion_date("2026-02-30")  # Impossible day

    def test_normalize_gcs_prefix(self) -> None:
        """Ensure GCS prefix removes leading, trailing, and duplicate slashes."""
        self.assertEqual(normalize_gcs_prefix("raw"), "raw")
        self.assertEqual(normalize_gcs_prefix("/raw/landing/"), "raw/landing")
        self.assertEqual(normalize_gcs_prefix("///raw///"), "raw")
        self.assertEqual(normalize_gcs_prefix(""), "raw")
        self.assertEqual(normalize_gcs_prefix(None), "raw")

    def test_gcs_config_validation(self) -> None:
        """Ensure GCSConfig validates required properties on demand."""
        cfg_missing_all = GCSConfig(project_id=None, bucket_name=None)
        with self.assertRaises(ConfigurationError):
            cfg_missing_all.validate()

        cfg_missing_bucket = GCSConfig(project_id="my-project", bucket_name=None)
        with self.assertRaises(ConfigurationError):
            cfg_missing_bucket.validate()

        cfg_valid = GCSConfig(project_id="my-project", bucket_name="my-bucket")
        # Should not raise
        cfg_valid.validate()

    def test_load_batch_pipeline_config_dry_run_without_gcp(self) -> None:
        """Ensure dry-run config loads successfully without GCP environment variables."""
        env_vars = {
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5433",
            "POSTGRES_DB": "retailpulse",
            "POSTGRES_USER": "retailpulse_user",
            "POSTGRES_PASSWORD": "ValidPassword",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            # require_gcs=False allows dry-run without GCP_PROJECT_ID or bucket
            config = load_batch_pipeline_config(require_gcs=False)
            self.assertEqual(config.database.port, 5433)
            self.assertEqual(config.tables, ALLOWED_SOURCE_TABLES)
            self.assertIsNone(config.gcs.project_id)
            self.assertIsNone(config.gcs.bucket_name)

    def test_load_batch_pipeline_config_require_gcs_fails_without_gcp(self) -> None:
        """Ensure require_gcs=True fails when GCP configuration is absent."""
        env_vars = {
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5433",
            "POSTGRES_DB": "retailpulse",
            "POSTGRES_USER": "retailpulse_user",
            "POSTGRES_PASSWORD": "ValidPassword",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            with self.assertRaises(ConfigurationError):
                load_batch_pipeline_config(require_gcs=True)

    def test_mask_secret(self) -> None:
        """Test secret masking utility."""
        self.assertEqual(mask_secret(None), "***")
        self.assertEqual(mask_secret(""), "***")
        self.assertEqual(mask_secret("ab"), "***")
        self.assertEqual(mask_secret("abcdef"), "****ef")


if __name__ == "__main__":
    unittest.main()
