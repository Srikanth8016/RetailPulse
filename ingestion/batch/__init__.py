"""RetailPulse Batch Ingestion Package.

This package provides components to extract data from the PostgreSQL source OLTP
database and load it into Google Cloud Storage as partitioned raw CSV files.

Modules:
    config: Configuration loader, validators, and dataclasses.
    postgres_extractor: Streaming batch extractor using psycopg 3 named server-side cursors.
    gcs_uploader: Partitioned CSV uploader for Google Cloud Storage with overwrite protection.
    pipeline: Orchestrator integrating extraction and cloud landing.
"""

from ingestion.batch.config import (
    ALLOWED_SOURCE_TABLES,
    DEFAULT_SOURCE_TABLES,
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
from ingestion.batch.gcs_uploader import (
    GCSBatchUploader,
    GCSUploadError,
    UploadResult,
)
from ingestion.batch.pipeline import (
    BatchIngestionPipeline,
    PipelineRunSummary,
    TableIngestionMetrics,
    run_batch_ingestion_pipeline,
)
from ingestion.batch.postgres_extractor import (
    ExtractionResult,
    PostgresBatchExtractor,
    PostgresExtractionError,
)

__all__ = [
    "ALLOWED_SOURCE_TABLES",
    "BatchIngestionPipeline",
    "BatchPipelineConfig",
    "ConfigurationError",
    "DEFAULT_SOURCE_TABLES",
    "DatabaseConfig",
    "ExtractionResult",
    "GCSBatchUploader",
    "GCSConfig",
    "GCSUploadError",
    "PipelineRunSummary",
    "PostgresBatchExtractor",
    "PostgresExtractionError",
    "TableIngestionMetrics",
    "UploadResult",
    "load_batch_pipeline_config",
    "mask_secret",
    "normalize_gcs_prefix",
    "run_batch_ingestion_pipeline",
    "validate_ingestion_date",
    "validate_source_tables",
]
