"""PostgreSQL Source Data Batch Extractor.

This module handles extracting transactional data from PostgreSQL source tables
using psycopg 3 with named server-side cursors for memory-safe chunked streaming
directly into CSV files in a local staging directory.
"""

from __future__ import annotations

import csv
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import psycopg
from psycopg import sql

from ingestion.batch.config import ALLOWED_SOURCE_TABLES, DatabaseConfig

logger = logging.getLogger("retailpulse.ingestion.postgres_extractor")


class PostgresExtractionError(Exception):
    """Raised when an error occurs during PostgreSQL extraction or CSV serialization."""


@dataclass(frozen=True)
class ExtractionResult:
    """Summary metrics of a completed table extraction."""

    table_name: str
    csv_path: Path
    row_count: int
    file_size_bytes: int
    duration_seconds: float
    columns: tuple[str, ...]

    def __repr__(self) -> str:
        return (
            f"ExtractionResult(table={self.table_name!r}, "
            f"rows={self.row_count:,}, "
            f"size_kb={self.file_size_bytes / 1024:.2f}, "
            f"duration={self.duration_seconds:.2f}s, "
            f"path={self.csv_path})"
        )


class PostgresBatchExtractor:
    """Extracts tables from PostgreSQL to CSV files with header metadata using server-side cursors."""

    def __init__(
        self,
        db_config: DatabaseConfig,
        staging_dir: Path,
        chunk_size: int = 5000,
    ) -> None:
        """Initialize the extractor.

        Args:
            db_config: PostgreSQL connection parameters.
            staging_dir: Local filesystem directory for staging output CSVs.
            chunk_size: Number of records to fetch per cursor iteration.
        """
        self.db_config = db_config
        self.staging_dir = Path(staging_dir)
        self.chunk_size = chunk_size

    def _ensure_staging_dir(self) -> None:
        """Ensure the local staging directory exists on the filesystem."""
        try:
            self.staging_dir.mkdir(parents=True, exist_ok=True)
        except OSError as err:
            logger.error("Failed to create staging directory at %s: %s", self.staging_dir, err)
            raise PostgresExtractionError(
                f"Cannot create local staging directory: {self.staging_dir}"
            ) from err

    def extract_table(
        self,
        table_name: str,
        output_csv_path: Path | None = None,
    ) -> ExtractionResult:
        """Extract a single PostgreSQL table to a CSV file using a named server-side cursor.

        Args:
            table_name: Name of the table to extract (must be in ALLOWED_SOURCE_TABLES).
            output_csv_path: Optional explicit output path. Defaults to staging_dir/{table_name}.csv.

        Returns:
            ExtractionResult containing row count, file size, columns, and duration.

        Raises:
            PostgresExtractionError: If table is invalid, or extraction/writing fails.
        """
        # Validate table name against strict allowlist before querying
        if table_name not in ALLOWED_SOURCE_TABLES:
            raise PostgresExtractionError(
                f"Table '{table_name}' is not in the allowed source tables list: {list(ALLOWED_SOURCE_TABLES)}"
            )

        self._ensure_staging_dir()

        target_path = output_csv_path or (self.staging_dir / f"{table_name}.csv")
        logger.info(
            "Starting batch extraction for table '%s.%s' -> %s (chunk_size=%d)",
            self.db_config.db_schema,
            table_name,
            target_path,
            self.chunk_size,
        )

        start_time = time.perf_counter()
        row_count = 0
        column_names: tuple[str, ...] = ()

        # Safe parameterized SQL composition using psycopg.sql Identifiers
        query = sql.SQL(
            "SELECT * FROM {schema}.{table}"
        ).format(
            schema=sql.Identifier(self.db_config.db_schema),
            table=sql.Identifier(table_name),
        )

        # Unique server-side cursor name scoped to this table extraction
        cursor_name = f"cur_extract_{table_name}"

        try:
            # Connect using keyword arguments to avoid manual string interpolation of credentials
            with psycopg.connect(**self.db_config.connect_kwargs) as conn:
                # PostgreSQL server-side cursors require an active transaction block
                with conn.transaction():
                    # Named cursor triggers Psycopg 3 ServerCursor (DECLARE ... CURSOR)
                    with conn.cursor(name=cursor_name) as cur:
                        logger.debug(
                            "Opened server-side cursor '%s' for table '%s'",
                            cursor_name,
                            table_name,
                        )
                        cur.execute(query)

                        if cur.description is None:
                            raise PostgresExtractionError(
                                f"Cursor description is None for table '{table_name}'. "
                                "Query did not return a valid result set."
                            )

                        column_names = tuple(col.name for col in cur.description)

                        # Stream records directly to CSV
                        with open(
                            target_path,
                            mode="w",
                            newline="",
                            encoding="utf-8",
                        ) as csv_file:
                            writer = csv.writer(
                                csv_file,
                                delimiter=",",
                                quotechar='"',
                                quoting=csv.QUOTE_MINIMAL,
                            )

                            # Write column header row even if table is empty
                            writer.writerow(column_names)

                            # Fetch in memory-bounded chunks from the server
                            while True:
                                rows: Sequence[Sequence[Any]] = cur.fetchmany(self.chunk_size)
                                if not rows:
                                    break
                                writer.writerows(rows)
                                row_count += len(rows)

        except psycopg.Error as err:
            logger.error(
                "PostgreSQL database error while extracting table '%s': %s",
                table_name,
                err,
                exc_info=False,
            )
            if target_path.exists():
                target_path.unlink(missing_ok=True)
            # Never include credentials in error messages
            raise PostgresExtractionError(
                f"Failed to extract table '{table_name}' from database '{self.db_config.dbname}' "
                f"at {self.db_config.host}:{self.db_config.port}: {err}"
            ) from err

        except (IOError, OSError) as err:
            logger.error(
                "I/O error while writing CSV for table '%s' at %s: %s",
                table_name,
                target_path,
                err,
                exc_info=True,
            )
            if target_path.exists():
                target_path.unlink(missing_ok=True)
            raise PostgresExtractionError(
                f"Failed to write staging CSV for table '{table_name}': {err}"
            ) from err

        duration = time.perf_counter() - start_time
        file_size = target_path.stat().st_size

        result = ExtractionResult(
            table_name=table_name,
            csv_path=target_path,
            row_count=row_count,
            file_size_bytes=file_size,
            duration_seconds=duration,
            columns=column_names,
        )

        logger.info(
            "Successfully extracted table '%s': %d rows, %.2f KB in %.2fs",
            table_name,
            row_count,
            file_size / 1024,
            duration,
        )
        return result

    def extract_tables(
        self,
        tables: Sequence[str],
    ) -> dict[str, ExtractionResult]:
        """Extract multiple tables sequentially.

        Args:
            tables: Sequence of table names to extract.

        Returns:
            Dictionary mapping table name to its ExtractionResult.

        Raises:
            PostgresExtractionError: If any table fails to extract.
        """
        results: dict[str, ExtractionResult] = {}
        for table in tables:
            results[table] = self.extract_table(table)
        return results
