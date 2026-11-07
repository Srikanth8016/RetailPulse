"""Unit tests for PostgreSQL Batch Extractor."""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import psycopg

from ingestion.batch.config import DatabaseConfig
from ingestion.batch.postgres_extractor import (
    PostgresBatchExtractor,
    PostgresExtractionError,
)


class ColumnMock:
    """Mock for psycopg cursor.description column object."""

    def __init__(self, name: str) -> None:
        self.name = name


class TestPostgresBatchExtractor(unittest.TestCase):
    """Test suite for server-side chunked PostgreSQL extraction to CSV."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.staging_dir = Path(self.temp_dir.name)
        self.db_config = DatabaseConfig(
            host="localhost",
            port=5433,
            dbname="retailpulse",
            user="retailpulse_user",
            password="test_password",
            db_schema="public",
        )
        self.extractor = PostgresBatchExtractor(
            db_config=self.db_config,
            staging_dir=self.staging_dir,
            chunk_size=2,  # Small chunk size to test iteration
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @patch("psycopg.connect")
    def test_extract_table_server_side_named_cursor_and_chunking(
        self,
        mock_connect: MagicMock,
    ) -> None:
        """Verify extractor uses a named server-side cursor and fetches rows in chunks."""
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.transaction.return_value.__enter__.return_value = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock description and chunked fetchmany responses
        mock_cursor.description = [
            ColumnMock("customer_id"),
            ColumnMock("first_name"),
            ColumnMock("email"),
        ]

        # Simulate 2 chunks of 2 rows each, then empty to terminate
        chunk1 = [(1, "Alice", "alice@example.com"), (2, "Bob", "bob@example.com")]
        chunk2 = [(3, "Charlie", "charlie@example.com")]
        chunk3: list[tuple[int, str, str]] = []
        mock_cursor.fetchmany.side_effect = [chunk1, chunk2, chunk3]

        # Execute extraction
        result = self.extractor.extract_table("customers")

        # Verify psycopg.connect received keyword arguments, not manual raw string
        mock_connect.assert_called_once_with(**self.db_config.connect_kwargs)

        # Verify server-side cursor was requested with specific name
        mock_conn.cursor.assert_called_once_with(name="cur_extract_customers")

        # Verify fetchmany was called with configured chunk_size=2
        self.assertEqual(mock_cursor.fetchmany.call_args_list, [call(2), call(2), call(2)])

        # Verify returned metrics
        self.assertEqual(result.table_name, "customers")
        self.assertEqual(result.row_count, 3)
        self.assertEqual(result.columns, ("customer_id", "first_name", "email"))
        self.assertTrue(result.csv_path.exists())

        # Verify CSV contents and headers on disk
        with open(result.csv_path, mode="r", newline="", encoding="utf-8") as f:
            reader = list(csv.reader(f))
            self.assertEqual(reader[0], ["customer_id", "first_name", "email"])
            self.assertEqual(reader[1], ["1", "Alice", "alice@example.com"])
            self.assertEqual(reader[2], ["2", "Bob", "bob@example.com"])
            self.assertEqual(reader[3], ["3", "Charlie", "charlie@example.com"])

    @patch("psycopg.connect")
    def test_extract_empty_table(self, mock_connect: MagicMock) -> None:
        """Verify empty tables produce CSVs containing column headers and row_count=0."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.transaction.return_value.__enter__.return_value = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        mock_cursor.description = [ColumnMock("product_id"), ColumnMock("price")]
        mock_cursor.fetchmany.return_value = []

        result = self.extractor.extract_table("products")

        self.assertEqual(result.row_count, 0)
        self.assertTrue(result.csv_path.exists())

        with open(result.csv_path, mode="r", newline="", encoding="utf-8") as f:
            lines = f.readlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(lines[0].strip(), "product_id,price")

    def test_extract_invalid_table_rejected_by_allowlist(self) -> None:
        """Ensure unapproved tables are rejected immediately without connecting."""
        with self.assertRaises(PostgresExtractionError) as ctx:
            self.extractor.extract_table("unapproved_users_table")
        self.assertIn("not in the allowed source tables list", str(ctx.exception))

    @patch("psycopg.connect")
    def test_database_error_cleans_up_staging_file(self, mock_connect: MagicMock) -> None:
        """Verify incomplete staging files are removed when a database query fails."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.transaction.return_value.__enter__.return_value = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Simulate database query error
        mock_cursor.execute.side_effect = psycopg.DatabaseError("Connection terminated abnormally")

        target_csv = self.staging_dir / "orders.csv"

        with self.assertRaises(PostgresExtractionError):
            self.extractor.extract_table("orders", output_csv_path=target_csv)

        # File should not remain on disk after failure
        self.assertFalse(target_csv.exists())


if __name__ == "__main__":
    unittest.main()
