"""Batch Ingestion Pipeline Orchestrator.

Orchestrates the extraction of transactional tables from PostgreSQL to local CSV
staging and uploads the staged files into Google Cloud Storage partitioned by
table and ingestion date:
    raw/{table_name}/ingestion_date=YYYY-MM-DD/{table_name}.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from ingestion.batch.config import (
    ALLOWED_SOURCE_TABLES,
    BatchPipelineConfig,
    ConfigurationError,
    load_batch_pipeline_config,
    validate_source_tables,
)
from ingestion.batch.gcs_uploader import GCSBatchUploader, GCSUploadError, UploadResult
from ingestion.batch.postgres_extractor import (
    ExtractionResult,
    PostgresBatchExtractor,
    PostgresExtractionError,
)

# Configure module-level logger
logger = logging.getLogger("retailpulse.ingestion.pipeline")


@dataclass(frozen=True)
class TableIngestionMetrics:
    """Consolidated metrics for a single table ingestion lifecycle."""

    table_name: str
    rows_extracted: int
    csv_bytes: int
    gcs_uri: str | None
    extraction_duration: float
    upload_duration: float
    total_duration: float
    status: str
    error_message: str | None = None


@dataclass(frozen=True)
class PipelineRunSummary:
    """Execution summary for the complete batch ingestion run."""

    ingestion_date: str
    tables_attempted: int
    tables_succeeded: int
    tables_failed: int
    total_rows: int
    total_bytes: int
    total_duration: float
    metrics: tuple[TableIngestionMetrics, ...]

    @property
    def is_success(self) -> bool:
        """True if all attempted tables succeeded without failure."""
        return self.tables_failed == 0 and self.tables_succeeded > 0


class BatchIngestionPipeline:
    """Coordinates PostgreSQL extraction and GCS landing for batch datasets."""

    def __init__(
        self,
        config: BatchPipelineConfig,
        extractor: PostgresBatchExtractor | None = None,
        uploader: GCSBatchUploader | None = None,
    ) -> None:
        """Initialize the ingestion pipeline.

        Args:
            config: Validated batch pipeline configuration.
            extractor: Optional custom PostgresBatchExtractor instance.
            uploader: Optional custom GCSBatchUploader instance.
        """
        self.config = config
        self.extractor = extractor or PostgresBatchExtractor(
            db_config=config.database,
            staging_dir=config.local_staging_dir,
            chunk_size=config.chunk_size,
        )
        self.uploader = uploader or GCSBatchUploader(gcs_config=config.gcs)

    def ingest_table(
        self,
        table_name: str,
        dry_run: bool = False,
        cleanup_staging: bool = False,
        allow_overwrite: bool | None = None,
    ) -> TableIngestionMetrics:
        """Extract a single table and upload to GCS.

        Args:
            table_name: Name of the source table (must be in ALLOWED_SOURCE_TABLES).
            dry_run: If True, extract to CSV locally but skip GCS upload.
            cleanup_staging: If True, remove local staging CSV after successful upload.
            allow_overwrite: Optional override for GCS object overwrite policy.

        Returns:
            TableIngestionMetrics summarizing the table ingestion.
        """
        table_start_time = time.perf_counter()
        logger.info("=" * 60)
        logger.info("Starting ingestion lifecycle for table: %s", table_name)
        logger.info("=" * 60)

        # Validate table name against strict allowlist
        if table_name not in ALLOWED_SOURCE_TABLES:
            total_duration = time.perf_counter() - table_start_time
            err_msg = (
                f"Table '{table_name}' is not allowed. "
                f"Must be one of {list(ALLOWED_SOURCE_TABLES)}"
            )
            logger.error(err_msg)
            return TableIngestionMetrics(
                table_name=table_name,
                rows_extracted=0,
                csv_bytes=0,
                gcs_uri=None,
                extraction_duration=0.0,
                upload_duration=0.0,
                total_duration=total_duration,
                status="FAILED",
                error_message=err_msg,
            )

        staging_csv = self.config.get_staging_csv_path(table_name)
        blob_name = self.config.get_gcs_blob_name(table_name)

        # Step 1: Extract from PostgreSQL (measure extraction duration independently)
        extract_start = time.perf_counter()
        try:
            extraction_result: ExtractionResult = self.extractor.extract_table(
                table_name=table_name,
                output_csv_path=staging_csv,
            )
            extraction_duration = time.perf_counter() - extract_start
        except PostgresExtractionError as err:
            extraction_duration = time.perf_counter() - extract_start
            total_duration = time.perf_counter() - table_start_time
            logger.error("Extraction failed for table '%s': %s", table_name, err)
            return TableIngestionMetrics(
                table_name=table_name,
                rows_extracted=0,
                csv_bytes=0,
                gcs_uri=None,
                extraction_duration=extraction_duration,
                upload_duration=0.0,
                total_duration=total_duration,
                status="FAILED",
                error_message=f"Extraction error: {err}",
            )
        except Exception as err:
            extraction_duration = time.perf_counter() - extract_start
            total_duration = time.perf_counter() - table_start_time
            logger.error(
                "Unexpected system error during extraction of '%s': %s",
                table_name,
                err,
                exc_info=True,
            )
            return TableIngestionMetrics(
                table_name=table_name,
                rows_extracted=0,
                csv_bytes=0,
                gcs_uri=None,
                extraction_duration=extraction_duration,
                upload_duration=0.0,
                total_duration=total_duration,
                status="FAILED",
                error_message=f"Unexpected extraction error: {err}",
            )

        # Step 2: Upload to GCS (measure upload duration independently)
        upload_duration = 0.0
        gcs_uri: str | None = None

        if dry_run:
            logger.info(
                "[DRY RUN] PostgreSQL extraction succeeded for table '%s'. "
                "GCS upload skipped. Local staging file: %s",
                table_name,
                staging_csv,
            )
            # Do NOT report a fake GCS URI as an actual upload
            gcs_uri = None
        else:
            upload_start = time.perf_counter()
            try:
                upload_result: UploadResult = self.uploader.upload_file(
                    local_path=staging_csv,
                    destination_blob_name=blob_name,
                    table_name=table_name,
                    content_type="text/csv",
                    allow_overwrite=allow_overwrite,
                )
                upload_duration = time.perf_counter() - upload_start
                gcs_uri = upload_result.gcs_uri

                # Handle staging cleanup errors gracefully without failing successful cloud upload
                if cleanup_staging:
                    try:
                        staging_csv.unlink(missing_ok=True)
                        logger.debug("Cleaned up local staging file: %s", staging_csv)
                    except OSError as cleanup_err:
                        logger.warning(
                            "Could not delete staging file '%s' after successful upload: %s",
                            staging_csv,
                            cleanup_err,
                        )

            except GCSUploadError as err:
                upload_duration = time.perf_counter() - upload_start
                total_duration = time.perf_counter() - table_start_time
                logger.error("Upload failed for table '%s': %s", table_name, err)
                return TableIngestionMetrics(
                    table_name=table_name,
                    rows_extracted=extraction_result.row_count,
                    csv_bytes=extraction_result.file_size_bytes,
                    gcs_uri=None,
                    extraction_duration=extraction_duration,
                    upload_duration=upload_duration,
                    total_duration=total_duration,
                    status="FAILED",
                    error_message=f"Upload error: {err}",
                )
            except Exception as err:
                upload_duration = time.perf_counter() - upload_start
                total_duration = time.perf_counter() - table_start_time
                logger.error(
                    "Unexpected system error during upload of '%s': %s",
                    table_name,
                    err,
                    exc_info=True,
                )
                return TableIngestionMetrics(
                    table_name=table_name,
                    rows_extracted=extraction_result.row_count,
                    csv_bytes=extraction_result.file_size_bytes,
                    gcs_uri=None,
                    extraction_duration=extraction_duration,
                    upload_duration=upload_duration,
                    total_duration=total_duration,
                    status="FAILED",
                    error_message=f"Unexpected upload error: {err}",
                )

        total_duration = time.perf_counter() - table_start_time
        logger.info(
            "Completed table '%s' successfully: %d rows (%.2f KB) in %.2fs total",
            table_name,
            extraction_result.row_count,
            extraction_result.file_size_bytes / 1024,
            total_duration,
        )

        return TableIngestionMetrics(
            table_name=table_name,
            rows_extracted=extraction_result.row_count,
            csv_bytes=extraction_result.file_size_bytes,
            gcs_uri=gcs_uri,
            extraction_duration=extraction_duration,
            upload_duration=upload_duration,
            total_duration=total_duration,
            status="SUCCESS",
            error_message=None,
        )

    def run(
        self,
        tables: Sequence[str] | None = None,
        dry_run: bool = False,
        cleanup_staging: bool = False,
        allow_overwrite: bool | None = None,
    ) -> PipelineRunSummary:
        """Execute the batch ingestion pipeline for specified or configured tables.

        Args:
            tables: Optional sequence of tables to ingest. Defaults to config.tables.
            dry_run: If True, skip GCS uploads.
            cleanup_staging: If True, delete staging CSV files after upload.
            allow_overwrite: Optional override for overwrite policy.

        Returns:
            PipelineRunSummary detailing execution counts, throughput, and status.
        """
        run_start_time = time.perf_counter()

        # Validate target tables against allowlist
        raw_tables = tables if tables is not None else self.config.tables
        target_tables = validate_source_tables(raw_tables)

        target_bucket = self.config.gcs.bucket_name or "(Not configured - dry-run mode)"
        logger.info("************************************************************")
        logger.info("RETAILPULSE BATCH INGESTION PIPELINE (POSTGRESQL -> GCS)")
        logger.info("Ingestion Date Partition: %s", self.config.ingestion_date)
        logger.info("Target Tables (%d):        %s", len(target_tables), ", ".join(target_tables))
        logger.info("Local Staging Directory:  %s", self.config.local_staging_dir)
        logger.info("Target GCS Bucket:        %s", target_bucket)
        logger.info("Dry Run Mode:             %s", dry_run)
        logger.info("Cleanup Staging Files:    %s", cleanup_staging)
        logger.info("************************************************************")

        table_metrics: list[TableIngestionMetrics] = []
        succeeded_count = 0
        failed_count = 0
        total_rows = 0
        total_bytes = 0

        for table_name in target_tables:
            metrics = self.ingest_table(
                table_name=table_name,
                dry_run=dry_run,
                cleanup_staging=cleanup_staging,
                allow_overwrite=allow_overwrite,
            )
            table_metrics.append(metrics)
            if metrics.status == "SUCCESS":
                succeeded_count += 1
                total_rows += metrics.rows_extracted
                total_bytes += metrics.csv_bytes
            else:
                failed_count += 1

        run_duration = time.perf_counter() - run_start_time
        summary = PipelineRunSummary(
            ingestion_date=self.config.ingestion_date,
            tables_attempted=len(target_tables),
            tables_succeeded=succeeded_count,
            tables_failed=failed_count,
            total_rows=total_rows,
            total_bytes=total_bytes,
            total_duration=run_duration,
            metrics=tuple(table_metrics),
        )

        self._print_summary_report(summary, dry_run=dry_run)
        return summary

    def _print_summary_report(self, summary: PipelineRunSummary, dry_run: bool = False) -> None:
        """Format and log an audit summary report of the batch ingestion run."""
        border = "=" * 80
        logger.info("\n%s", border)
        logger.info("BATCH INGESTION RUN SUMMARY REPORT%s", " (DRY-RUN)" if dry_run else "")
        logger.info("%s", border)
        logger.info(
            "%-15s | %-10s | %-12s | %-10s | %-10s | %-8s",
            "Table",
            "Rows",
            "Size (KB)",
            "Extract(s)",
            "Upload(s)",
            "Status",
        )
        logger.info("-" * 80)

        for m in summary.metrics:
            upload_disp = "SKIPPED" if dry_run and m.status == "SUCCESS" else f"{m.upload_duration:10.2f}"
            logger.info(
                "%-15s | %10d | %12.2f | %10.2f | %10s | %-8s",
                m.table_name,
                m.rows_extracted,
                m.csv_bytes / 1024,
                m.extraction_duration,
                upload_disp,
                m.status,
            )

        logger.info("-" * 80)
        logger.info(
            "Totals: %d tables attempted | %d succeeded | %d failed",
            summary.tables_attempted,
            summary.tables_succeeded,
            summary.tables_failed,
        )
        logger.info(
            "Total Extracted Rows:  %s",
            f"{summary.total_rows:,}",
        )
        logger.info(
            "Total Data Volume:     %.2f MB",
            summary.total_bytes / (1024 * 1024),
        )
        logger.info(
            "Total Pipeline Time:   %.2fs",
            summary.total_duration,
        )
        logger.info("%s", border)

        if not summary.is_success:
            logger.warning(
                "Pipeline completed with %d failed table(s). Review logs above for details.",
                summary.tables_failed,
            )
        else:
            logger.info("Pipeline completed all table tasks successfully.")


def run_batch_ingestion_pipeline(
    env_file_path: Path | str | None = None,
    tables: Sequence[str] | None = None,
    ingestion_date: str | None = None,
    dry_run: bool = False,
    cleanup_staging: bool = False,
    allow_overwrite: bool = False,
) -> PipelineRunSummary:
    """Convenience functional interface to configure and execute batch ingestion.

    Args:
        env_file_path: Optional explicit .env path.
        tables: Optional table subset to ingest.
        ingestion_date: Optional ingestion date string (YYYY-MM-DD).
        dry_run: If True, extracts data locally but skips GCS upload.
                 GCS credentials/bucket are not required when dry_run=True.
        cleanup_staging: If True, cleans up local CSV files post upload.
        allow_overwrite: If True, allows overwriting existing GCS blobs.

    Returns:
        PipelineRunSummary instance.
    """
    config = load_batch_pipeline_config(
        env_file_path=env_file_path,
        ingestion_date=ingestion_date,
        tables=tables,
        require_gcs=not dry_run,
        allow_overwrite=allow_overwrite,
    )
    pipeline = BatchIngestionPipeline(config=config)
    return pipeline.run(
        tables=tables,
        dry_run=dry_run,
        cleanup_staging=cleanup_staging,
        allow_overwrite=allow_overwrite,
    )


def _setup_logging(log_level: str = "INFO") -> None:
    """Configure structured logging for CLI execution."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _parse_args() -> argparse.Namespace:
    """Parse command line arguments for the batch ingestion pipeline."""
    parser = argparse.ArgumentParser(
        description="RetailPulse PostgreSQL to Google Cloud Storage Batch Ingestion Pipeline."
    )
    parser.add_argument(
        "--table",
        "-t",
        action="append",
        dest="tables",
        help=f"Specific table(s) to ingest. Allowed: {list(ALLOWED_SOURCE_TABLES)}. Defaults to all.",
    )
    parser.add_argument(
        "--ingestion-date",
        "-d",
        type=str,
        default=None,
        help="Ingestion date partition (format: YYYY-MM-DD). Defaults to current UTC date.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract PostgreSQL tables to local staging CSVs but skip upload to GCS.",
    )
    parser.add_argument(
        "--cleanup-staging",
        action="store_true",
        help="Delete local staging CSV files after successful GCS upload.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files in GCS if they already exist in the target partition.",
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=None,
        help="Path to custom .env file for environment variables.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set the logging level (default: INFO).",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point for batch ingestion."""
    args = _parse_args()
    _setup_logging(args.log_level)

    try:
        tables_to_ingest = tuple(args.tables) if args.tables else None
        if tables_to_ingest:
            validate_source_tables(tables_to_ingest)

        summary = run_batch_ingestion_pipeline(
            env_file_path=args.env_file,
            tables=tables_to_ingest,
            ingestion_date=args.ingestion_date,
            dry_run=args.dry_run,
            cleanup_staging=args.cleanup_staging,
            allow_overwrite=args.overwrite,
        )
        if not summary.is_success:
            sys.exit(1)
    except ConfigurationError as err:
        logger.error("Configuration Error: %s", err)
        sys.exit(2)
    except Exception as err:
        logger.critical("Fatal error during batch ingestion execution: %s", err, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
