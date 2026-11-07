"""Google Cloud Storage Batch Uploader.

This module handles uploading local staging CSV files into Google Cloud Storage
data lake raw landing buckets, partitioned by table and ingestion date:
    raw/{table_name}/ingestion_date=YYYY-MM-DD/{table_name}.csv
Includes overwrite protection using GCS generation preconditions.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

try:
    from google.cloud import storage
    from google.cloud.exceptions import GoogleCloudError, PreconditionFailed
    HAS_GCS_PACKAGE = True
except (ImportError, ModuleNotFoundError):
    storage = None  # type: ignore[assignment]

    class GoogleCloudError(Exception):  # type: ignore[no-redef]
        """Fallback definition when google-cloud-storage is not installed."""

    class PreconditionFailed(GoogleCloudError):  # type: ignore[no-redef]
        """Fallback definition when google-cloud-storage is not installed."""

    HAS_GCS_PACKAGE = False

from ingestion.batch.config import ALLOWED_SOURCE_TABLES, GCSConfig

logger = logging.getLogger("retailpulse.ingestion.gcs_uploader")


class GCSUploadError(Exception):
    """Raised when an error occurs during local file upload to Google Cloud Storage."""


__all__ = [
    "GCSBatchUploader",
    "GCSUploadError",
    "GoogleCloudError",
    "PreconditionFailed",
    "UploadResult",
]


@dataclass(frozen=True)
class UploadResult:
    """Summary metrics of a completed GCS file upload."""

    table_name: str
    local_path: Path
    bucket_name: str
    blob_name: str
    gcs_uri: str
    bytes_uploaded: int
    duration_seconds: float

    def __repr__(self) -> str:
        return (
            f"UploadResult(table={self.table_name!r}, "
            f"uri={self.gcs_uri!r}, "
            f"size_kb={self.bytes_uploaded / 1024:.2f}, "
            f"duration={self.duration_seconds:.2f}s)"
        )


class GCSBatchUploader:
    """Uploads staged data files to Google Cloud Storage with overwrite protection."""

    def __init__(
        self,
        gcs_config: GCSConfig,
        client: storage.Client | None = None,
    ) -> None:
        """Initialize the GCS uploader.

        Args:
            gcs_config: GCS configuration parameters (project, bucket, base prefix).
            client: Optional pre-configured storage.Client instance. If None,
                    initialized lazily upon first upload.
        """
        self.gcs_config = gcs_config
        self._client: storage.Client | None = client

    def get_client(self) -> storage.Client:
        """Lazily initialize and return the Google Cloud Storage client.

        Validates required configuration before client creation.

        Returns:
            storage.Client instance.

        Raises:
            GCSUploadError: If configuration is missing or client initialization fails.
        """
        if self._client is None:
            if not HAS_GCS_PACKAGE or storage is None:
                raise GCSUploadError(
                    "The 'google-cloud-storage' library is not installed in the active environment. "
                    "Install it via: pip install -r requirements.txt (or pip install google-cloud-storage)"
                )

            # Validate GCS settings only when cloud operations are invoked
            try:
                self.gcs_config.validate()
            except Exception as err:
                raise GCSUploadError(f"GCS Configuration validation failed: {err}") from err

            try:
                if self.gcs_config.credentials_path:
                    logger.debug(
                        "Initializing GCS client with explicit credentials path: %s",
                        self.gcs_config.credentials_path,
                    )
                    self._client = storage.Client.from_service_account_json(
                        json_credentials_path=self.gcs_config.credentials_path,
                        project=self.gcs_config.project_id,
                    )
                else:
                    logger.debug(
                        "Initializing GCS client with Application Default Credentials (ADC) for project '%s'",
                        self.gcs_config.project_id,
                    )
                    self._client = storage.Client(project=self.gcs_config.project_id)
            except Exception as err:
                logger.error("Failed to initialize Google Cloud Storage client: %s", err)
                raise GCSUploadError(
                    f"Could not initialize GCS client for project '{self.gcs_config.project_id}': {err}"
                ) from err
        return self._client

    def build_destination_blob_name(
        self,
        table_name: str,
        ingestion_date: str,
    ) -> str:
        """Generate standard GCS object key path for a source table.

        Format:
            raw/{table_name}/ingestion_date=YYYY-MM-DD/{table_name}.csv

        Args:
            table_name: Source table name (must be in ALLOWED_SOURCE_TABLES).
            ingestion_date: Ingestion date partition string (YYYY-MM-DD).

        Returns:
            Relative blob path string.
        """
        if table_name not in ALLOWED_SOURCE_TABLES:
            raise GCSUploadError(
                f"Cannot build destination blob name for invalid table '{table_name}'. "
                f"Allowed tables: {list(ALLOWED_SOURCE_TABLES)}"
            )
        return f"{self.gcs_config.base_prefix}/{table_name}/ingestion_date={ingestion_date}/{table_name}.csv"

    def upload_file(
        self,
        local_path: Path,
        destination_blob_name: str,
        table_name: str,
        content_type: str = "text/csv",
        allow_overwrite: bool | None = None,
    ) -> UploadResult:
        """Upload a local staged file to the configured GCS bucket.

        Uses GCS generation preconditions (if_generation_match=0) when overwrite is disabled
        to prevent accidental overwriting of existing partitioned datasets.

        Args:
            local_path: Local filesystem path to the file.
            destination_blob_name: Target blob key inside the bucket.
            table_name: Name of the associated table for metrics tracking.
            content_type: MIME content type for the uploaded blob (default: text/csv).
            allow_overwrite: Optional override for overwrite policy. Defaults to config setting.

        Returns:
            UploadResult with metadata and transfer duration.

        Raises:
            GCSUploadError: If file is missing, GCS upload fails, or overwrite precondition fails.
        """
        # Ensure configuration is valid before proceeding
        try:
            self.gcs_config.validate()
        except Exception as err:
            raise GCSUploadError(f"Cannot upload file: {err}") from err

        resolved_path = Path(local_path)
        if not resolved_path.is_file():
            raise GCSUploadError(f"Staged file not found for upload: {resolved_path}")

        file_size = resolved_path.stat().st_size
        bucket_name = self.gcs_config.bucket_name
        gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"

        # Resolve overwrite policy
        should_overwrite = (
            allow_overwrite
            if allow_overwrite is not None
            else self.gcs_config.allow_overwrite
        )

        logger.info(
            "Uploading '%s' (%.2f KB) -> %s (allow_overwrite=%s)",
            resolved_path.name,
            file_size / 1024,
            gcs_uri,
            should_overwrite,
        )

        start_time = time.perf_counter()

        try:
            client = self.get_client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            # Enforce generation precondition:
            # if_generation_match=0 ensures atomic creation only if the object does NOT exist.
            # if_generation_match=None allows unconditional overwrite.
            generation_precondition = None if should_overwrite else 0

            blob.upload_from_filename(
                filename=str(resolved_path),
                content_type=content_type,
                if_generation_match=generation_precondition,
            )

        except PreconditionFailed as err:
            logger.error(
                "GCS PreconditionFailed: Object '%s' already exists and overwrite is disabled.",
                gcs_uri,
            )
            raise GCSUploadError(
                f"Target object already exists at '{gcs_uri}' and overwrite is disabled (if_generation_match=0)."
            ) from err

        except GoogleCloudError as err:
            logger.error("GCS API error during upload of %s: %s", gcs_uri, err, exc_info=False)
            raise GCSUploadError(
                f"Google Cloud Storage error while uploading to '{gcs_uri}': {err}"
            ) from err

        except Exception as err:
            logger.error("Unexpected error during upload to %s: %s", gcs_uri, err, exc_info=False)
            raise GCSUploadError(
                f"Unexpected error while uploading to '{gcs_uri}': {err}"
            ) from err

        duration = time.perf_counter() - start_time
        logger.info("Successfully uploaded %s in %.2fs", gcs_uri, duration)

        return UploadResult(
            table_name=table_name,
            local_path=resolved_path,
            bucket_name=bucket_name,  # type: ignore[arg-type]
            blob_name=destination_blob_name,
            gcs_uri=gcs_uri,
            bytes_uploaded=file_size,
            duration_seconds=duration,
        )
