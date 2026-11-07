
"""Incremental PostgreSQL orders extraction to GCS.

Reads the BigQuery watermark, applies a five-minute overlap,
extracts orders using a consistent PostgreSQL snapshot, and
writes both a CSV file and run metadata JSON.

The watermark is NOT advanced by this module.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from google.cloud import bigquery
from psycopg import sql

from ingestion.batch.config import load_batch_pipeline_config
from ingestion.batch.gcs_uploader import GCSBatchUploader


logger = logging.getLogger("retailpulse.ingestion.incremental_orders")

PROJECT_ID = "clean-abacus-513514-c8"
WATERMARK_TABLE = (
    "clean-abacus-513514-c8.retailpulse_silver.pipeline_watermarks"
)
PIPELINE_NAME = "orders"
OVERLAP_MINUTES = 5
FETCH_CHUNK_SIZE = 2000


def get_current_watermark(client: bigquery.Client) -> datetime:
    """Read the last successfully stored watermark from BigQuery."""

    query = f"""
        SELECT watermark_timestamp
        FROM `{WATERMARK_TABLE}`
        WHERE pipeline_name = @pipeline_name
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "pipeline_name", "STRING", PIPELINE_NAME
            )
        ]
    )

    rows = list(client.query(query, job_config=job_config).result())

    if len(rows) != 1:
        raise RuntimeError(
            f"Expected exactly one watermark row for "
            f"'{PIPELINE_NAME}', found {len(rows)}."
        )

    watermark = rows[0]["watermark_timestamp"]

    if watermark is None:
        raise RuntimeError("The orders watermark is NULL.")

    if watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)

    return watermark.astimezone(timezone.utc)


def extract_changed_orders(
    db_config,
    output_path: Path,
    lower_bound: datetime,
) -> tuple[int, datetime | None, datetime]:
    """Extract changed orders using a consistent PostgreSQL snapshot.

    Returns:
        row_count: Number of extracted records.
        max_updated_at: Maximum source updated_at in the extracted data.
        upper_bound: Fixed upper boundary for this extraction run.
    """

    output_path.parent.mkdir(parents=True, exist_ok=True)

    row_count = 0
    max_updated_at: datetime | None = None

    query = sql.SQL(
        """
        SELECT *
        FROM {}.{}
        WHERE updated_at >= %s
          AND updated_at <= %s
        ORDER BY updated_at, order_id
        """
    ).format(
        sql.Identifier(db_config.db_schema),
        sql.Identifier("orders"),
    )

    try:
        with psycopg.connect(**db_config.connect_kwargs) as conn:
            with conn.transaction():

                # Establish a consistent, read-only source snapshot.
                with conn.cursor() as control_cursor:
                    control_cursor.execute(
                        "SET TRANSACTION ISOLATION LEVEL "
                        "REPEATABLE READ, READ ONLY"
                    )
                    control_cursor.execute("SELECT CURRENT_TIMESTAMP")
                    upper_bound = control_cursor.fetchone()[0]

                if upper_bound.tzinfo is None:
                    upper_bound = upper_bound.replace(
                        tzinfo=timezone.utc
                    )

                upper_bound = upper_bound.astimezone(timezone.utc)

                if lower_bound > upper_bound:
                    raise RuntimeError(
                        f"Extraction lower bound {lower_bound.isoformat()} "
                        f"is later than upper bound "
                        f"{upper_bound.isoformat()}."
                    )

                logger.info(
                    "PostgreSQL extraction upper bound: %s",
                    upper_bound.isoformat(),
                )

                cursor_name = f"incremental_orders_{uuid.uuid4().hex}"

                with conn.cursor(name=cursor_name) as cursor:
                    cursor.execute(
                        query,
                        (lower_bound, upper_bound),
                    )

                    if cursor.description is None:
                        raise RuntimeError(
                            "PostgreSQL did not return column metadata."
                        )

                    columns = [
                        column.name for column in cursor.description
                    ]

                    if "updated_at" not in columns:
                        raise RuntimeError(
                            "The PostgreSQL orders table has no "
                            "updated_at column."
                        )

                    updated_at_index = columns.index("updated_at")

                    with output_path.open(
                        "w", newline="", encoding="utf-8"
                    ) as csv_file:
                        writer = csv.writer(csv_file)
                        writer.writerow(columns)

                        while True:
                            batch = cursor.fetchmany(FETCH_CHUNK_SIZE)

                            if not batch:
                                break

                            writer.writerows(batch)
                            row_count += len(batch)

                            for row in batch:
                                timestamp = row[updated_at_index]

                                if timestamp is not None and (
                                    max_updated_at is None
                                    or timestamp > max_updated_at
                                ):
                                    max_updated_at = timestamp

        if row_count > 0 and max_updated_at is None:
            raise RuntimeError(
                "Orders were extracted, but no updated_at value "
                "was found."
            )

        return row_count, max_updated_at, upper_bound

    except Exception:
        output_path.unlink(missing_ok=True)
        raise


def run_incremental_extraction(dry_run: bool = False) -> None:
    """Run incremental extraction and optionally upload its artifacts."""

    config = load_batch_pipeline_config(require_gcs=not dry_run)

    project_id = config.gcs.project_id or PROJECT_ID

    logger.info("Starting incremental orders extraction")
    logger.info("GCP project: %s", project_id)
    logger.info("Pipeline: %s", PIPELINE_NAME)

    # Step 1: Read the last successful watermark.
    bq_client = bigquery.Client(project=project_id)
    watermark = get_current_watermark(bq_client)

    # Step 2: Apply overlap to capture late-arriving updates.
    lower_bound = watermark - timedelta(minutes=OVERLAP_MINUTES)

    logger.info("Stored watermark: %s", watermark.isoformat())
    logger.info("Extraction lower bound: %s", lower_bound.isoformat())

    # Step 3: Generate a unique identifier for this run.
    run_id = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    output_path = (
        config.local_staging_dir
        / "incremental"
        / f"orders_{run_id}.csv"
    )

    metadata_path = (
        config.local_staging_dir
        / "incremental"
        / f"orders_{run_id}_metadata.json"
    )

    # Step 4: Extract using one consistent PostgreSQL snapshot.
    row_count, max_updated_at, upper_bound = extract_changed_orders(
        db_config=config.database,
        output_path=output_path,
        lower_bound=lower_bound,
    )

    logger.info("Extracted rows: %s", f"{row_count:,}")
    logger.info("Maximum source updated_at: %s", max_updated_at)
    logger.info("Extraction upper bound: %s", upper_bound.isoformat())
    logger.info("Local CSV: %s", output_path)

    # Step 5: Build unique GCS object names.
    ingestion_date = datetime.now(timezone.utc).date().isoformat()
    base_prefix = config.gcs.base_prefix.strip("/")

    blob_prefix = (
        f"{base_prefix}/orders/incremental/"
        f"extraction_date={ingestion_date}/run_id={run_id}"
    )

    csv_blob_name = f"{blob_prefix}/orders.csv"
    metadata_blob_name = f"{blob_prefix}/orders_metadata.json"

    csv_gcs_uri = (
        f"gs://{config.gcs.bucket_name}/{csv_blob_name}"
    )
    metadata_gcs_uri = (
        f"gs://{config.gcs.bucket_name}/{metadata_blob_name}"
    )

    # Step 6: Persist the extraction checkpoint locally.
    run_metadata = {
        "pipeline_name": PIPELINE_NAME,
        "source_table": "orders",
        "run_id": run_id,
        "project_id": project_id,
        "previous_watermark": watermark.isoformat(),
        "extraction_lower_bound": lower_bound.isoformat(),
        "extraction_upper_bound": upper_bound.isoformat(),
        "overlap_minutes": OVERLAP_MINUTES,
        "extracted_row_count": row_count,
        "maximum_source_updated_at": (
            max_updated_at.isoformat()
            if max_updated_at is not None
            else None
        ),
        "csv_gcs_uri": csv_gcs_uri,
        "metadata_gcs_uri": metadata_gcs_uri,
        "watermark_advanced": False,
        "status": "EXTRACTED",
    }

    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    with metadata_path.open("w", encoding="utf-8") as metadata_file:
        json.dump(run_metadata, metadata_file, indent=2)

    logger.info("Local run metadata: %s", metadata_path)

    if dry_run:
        logger.info("[DRY RUN] GCS upload skipped.")
        logger.info(
            "[DRY RUN] Extraction upper bound saved in local metadata."
        )
        return

    if row_count == 0:
        logger.info(
            "No changed orders found. No CSV or metadata upload required."
        )
        return

    # Step 7: Upload the CSV and metadata as separate GCS objects.
    uploader = GCSBatchUploader(gcs_config=config.gcs)

    csv_result = uploader.upload_file(
        local_path=output_path,
        destination_blob_name=csv_blob_name,
        table_name="orders",
        content_type="text/csv",
        allow_overwrite=False,
    )

    logger.info("CSV upload successful: %s", csv_result.gcs_uri)

    metadata_result = uploader.upload_file(
        local_path=metadata_path,
        destination_blob_name=metadata_blob_name,
        table_name="orders",
        content_type="application/json",
        allow_overwrite=False,
    )

    logger.info(
        "Metadata upload successful: %s",
        metadata_result.gcs_uri,
    )

    logger.info("Uploaded CSV bytes: %s", f"{csv_result.bytes_uploaded:,}")
    logger.info(
        "Uploaded metadata bytes: %s",
        f"{metadata_result.bytes_uploaded:,}",
    )

    logger.info(
        "Watermark remains unchanged. It must only advance after "
        "the downstream BigQuery load and MERGE succeed."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="RetailPulse incremental PostgreSQL orders extractor"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract locally without uploading to GCS.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
    )

    try:
        run_incremental_extraction(dry_run=args.dry_run)
    except Exception as error:
        logger.exception(
            "Incremental extraction failed: %s",
            error,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
