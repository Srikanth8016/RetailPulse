"""RetailPulse Batch Ingestion Configuration Module.

This module provides strongly-typed configuration dataclasses, environment variable
loading via python-dotenv, source table allowlists, strict validation rules, and
secret-masking utilities for the PostgreSQL-to-GCS batch ingestion pipeline.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final, Sequence

from dotenv import load_dotenv

# Strict allowlist of supported source tables in the PostgreSQL transactional database
ALLOWED_SOURCE_TABLES: Final[tuple[str, ...]] = (
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
)
DEFAULT_SOURCE_TABLES: Final[tuple[str, ...]] = ALLOWED_SOURCE_TABLES

DEFAULT_STAGING_DIR: Final[str] = "data/staging"
DEFAULT_GCS_RAW_PREFIX: Final[str] = "raw"
DEFAULT_FETCH_CHUNK_SIZE: Final[int] = 5000


class ConfigurationError(Exception):
    """Raised when required configuration or environment variables are missing or invalid."""


def mask_secret(value: str | None, unmasked_suffix_length: int = 2) -> str:
    """Mask a sensitive string (e.g., password or token) for safe logging.

    Args:
        value: The sensitive string to mask.
        unmasked_suffix_length: Number of trailing characters to leave unmasked.

    Returns:
        Masked string representation, e.g., '******ab' or '***'.
    """
    if not value:
        return "***"
    if len(value) <= unmasked_suffix_length:
        return "***"
    return f"{'*' * (len(value) - unmasked_suffix_length)}{value[-unmasked_suffix_length:]}"


def normalize_gcs_prefix(prefix: str | None) -> str:
    """Normalize GCS blob prefix to prevent accidental leading, trailing, or double slashes.

    Args:
        prefix: Raw prefix string, e.g. '/raw//landing/'.

    Returns:
        Clean normalized prefix string, e.g. 'raw/landing', or 'raw' if empty.
    """
    if not prefix:
        return DEFAULT_GCS_RAW_PREFIX
    parts = [part.strip() for part in prefix.strip("/").split("/") if part.strip()]
    return "/".join(parts) if parts else DEFAULT_GCS_RAW_PREFIX


def validate_source_tables(tables: Sequence[str]) -> tuple[str, ...]:
    """Validate that requested tables are non-empty and strictly exist in the allowlist.

    Args:
        tables: Sequence of table names to validate.

    Returns:
        Tuple of validated table names.

    Raises:
        ConfigurationError: If tables is empty or contains unallowed table names.
    """
    if not tables:
        raise ConfigurationError("Source table list cannot be empty. Must specify at least one table.")

    cleaned_tables = [tbl.strip() for tbl in tables if tbl.strip()]
    if not cleaned_tables:
        raise ConfigurationError("Source table list cannot be empty. Must specify at least one table.")

    disallowed = [t for t in cleaned_tables if t not in ALLOWED_SOURCE_TABLES]
    if disallowed:
        raise ConfigurationError(
            f"Invalid source table(s): {disallowed}. "
            f"Only the following tables are permitted: {list(ALLOWED_SOURCE_TABLES)}"
        )
    return tuple(cleaned_tables)


def validate_ingestion_date(date_str: str) -> str:
    """Validate that the ingestion date is strictly formatted as YYYY-MM-DD.

    Args:
        date_str: Date string to validate.

    Returns:
        Validated date string.

    Raises:
        ConfigurationError: If format or calendar date is invalid.
    """
    if not isinstance(date_str, str) or len(date_str) != 10:
        raise ConfigurationError(
            f"Invalid ingestion_date '{date_str}'. Expected format is strictly YYYY-MM-DD."
        )
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
        if parsed.strftime("%Y-%m-%d") != date_str:
            raise ValueError
    except ValueError as err:
        raise ConfigurationError(
            f"Invalid ingestion_date '{date_str}'. Expected valid calendar date in YYYY-MM-DD format."
        ) from err
    return date_str


@dataclass(frozen=True)
class DatabaseConfig:
    """PostgreSQL source database configuration parameters."""

    host: str = "localhost"
    port: int = 5433
    dbname: str = "retailpulse"
    user: str = "retailpulse_user"
    password: str = field(default="", repr=False)
    db_schema: str = "public"
    connect_timeout: int = 10

    def __post_init__(self) -> None:
        """Validate database parameters upon instantiation."""
        if not self.host or not self.host.strip():
            raise ConfigurationError("Database host cannot be empty.")
        if not (1 <= self.port <= 65535):
            raise ConfigurationError(
                f"Invalid database port '{self.port}'. Must be between 1 and 65535."
            )
        if not self.dbname or not self.dbname.strip():
            raise ConfigurationError("Database name cannot be empty.")
        if not self.user or not self.user.strip():
            raise ConfigurationError("Database user cannot be empty.")
        if not self.password:
            raise ConfigurationError("Database password cannot be empty.")
        if not self.db_schema or not self.db_schema.strip():
            raise ConfigurationError("Database schema cannot be empty.")
        if self.connect_timeout <= 0:
            raise ConfigurationError("Database connect_timeout must be a positive integer.")

    def __repr__(self) -> str:
        """Safe string representation preventing password leakage in logs or traces."""
        return (
            f"DatabaseConfig(host={self.host!r}, port={self.port}, "
            f"dbname={self.dbname!r}, user={self.user!r}, "
            f"password=***, db_schema={self.db_schema!r}, "
            f"connect_timeout={self.connect_timeout})"
        )

    @property
    def connect_kwargs(self) -> dict[str, Any]:
        """Return safe connection keyword arguments for psycopg.connect().

        Avoids manual libpq connection string interpolation.
        """
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connect_timeout,
        }


@dataclass(frozen=True)
class GCSConfig:
    """Google Cloud Storage destination configuration parameters."""

    project_id: str | None = None
    bucket_name: str | None = None
    base_prefix: str = DEFAULT_GCS_RAW_PREFIX
    credentials_path: str | None = None
    allow_overwrite: bool = False

    def __post_init__(self) -> None:
        """Normalize prefix upon instantiation."""
        normalized = normalize_gcs_prefix(self.base_prefix)
        object.__setattr__(self, "base_prefix", normalized)

    def validate(self) -> None:
        """Validate that required GCS parameters are set before initiating cloud operations.

        Raises:
            ConfigurationError: If project_id or bucket_name are not configured.
        """
        if not self.project_id or not self.project_id.strip():
            raise ConfigurationError(
                "GCP_PROJECT_ID environment variable is required for Google Cloud Storage operations."
            )
        if not self.bucket_name or not self.bucket_name.strip():
            raise ConfigurationError(
                "GCS_BUCKET_RAW (or GCS_BUCKET_LANDING) environment variable is required for Google Cloud Storage operations."
            )

    def __repr__(self) -> str:
        """Safe string representation of GCS configuration without secrets."""
        creds_display = mask_secret(self.credentials_path, 8) if self.credentials_path else None
        return (
            f"GCSConfig(project_id={self.project_id!r}, "
            f"bucket_name={self.bucket_name!r}, "
            f"base_prefix={self.base_prefix!r}, "
            f"credentials_path={creds_display!r}, "
            f"allow_overwrite={self.allow_overwrite})"
        )


@dataclass(frozen=True)
class BatchPipelineConfig:
    """Complete batch ingestion pipeline configuration."""

    database: DatabaseConfig
    gcs: GCSConfig
    local_staging_dir: Path
    tables: tuple[str, ...] = DEFAULT_SOURCE_TABLES
    ingestion_date: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    chunk_size: int = DEFAULT_FETCH_CHUNK_SIZE

    def __post_init__(self) -> None:
        """Validate pipeline parameters upon instantiation."""
        object.__setattr__(self, "tables", validate_source_tables(self.tables))
        object.__setattr__(self, "ingestion_date", validate_ingestion_date(self.ingestion_date))
        if self.chunk_size <= 0:
            raise ConfigurationError(
                f"Invalid chunk_size '{self.chunk_size}'. Must be a positive integer (> 0)."
            )

    def get_staging_csv_path(self, table_name: str) -> Path:
        """Return the local staging CSV filepath for a given source table.

        Path format: data/staging/{table_name}.csv
        """
        if table_name not in ALLOWED_SOURCE_TABLES:
            raise ConfigurationError(
                f"Cannot generate staging path for invalid table '{table_name}'. "
                f"Must be one of {list(ALLOWED_SOURCE_TABLES)}"
            )
        return self.local_staging_dir / f"{table_name}.csv"

    def get_gcs_blob_name(self, table_name: str) -> str:
        """Generate the partitioned GCS destination object key.

        Format requirement:
            raw/{table_name}/ingestion_date=YYYY-MM-DD/{table_name}.csv
        """
        if table_name not in ALLOWED_SOURCE_TABLES:
            raise ConfigurationError(
                f"Cannot generate GCS blob name for invalid table '{table_name}'. "
                f"Must be one of {list(ALLOWED_SOURCE_TABLES)}"
            )
        return f"{self.gcs.base_prefix}/{table_name}/ingestion_date={self.ingestion_date}/{table_name}.csv"


def load_batch_pipeline_config(
    env_file_path: Path | str | None = None,
    ingestion_date: str | None = None,
    tables: Sequence[str] | None = None,
    chunk_size: int | None = None,
    require_gcs: bool = False,
    allow_overwrite: bool = False,
) -> BatchPipelineConfig:
    """Load, validate, and assemble configuration from environment variables and .env file.

    Args:
        env_file_path: Optional explicit path to .env file. If None, auto-discovers .env.
        ingestion_date: Optional explicit ingestion date (YYYY-MM-DD). Defaults to UTC today.
        tables: Optional subset of tables to extract. Defaults to all 5 source tables.
        chunk_size: Optional cursor fetch chunk size. Defaults to 5000 rows.
        require_gcs: If True, validates GCP_PROJECT_ID and GCS bucket immediately.
                     If False (default for dry-run), allows missing GCP settings.
        allow_overwrite: If True, enables overwriting existing GCS blobs.

    Returns:
        Validated BatchPipelineConfig instance.

    Raises:
        ConfigurationError: If required configuration is missing or invalid.
    """
    if env_file_path:
        load_dotenv(dotenv_path=env_file_path, override=False)
    else:
        load_dotenv(override=False)

    # 1. Database Configuration
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port_raw = os.getenv("POSTGRES_PORT", "5433")
    try:
        db_port = int(db_port_raw)
    except ValueError as err:
        raise ConfigurationError(
            f"Invalid POSTGRES_PORT '{db_port_raw}'. Must be an integer."
        ) from err

    db_name = os.getenv("POSTGRES_DB", "retailpulse")
    db_user = os.getenv("POSTGRES_USER", "retailpulse_user")
    db_password = os.getenv("POSTGRES_PASSWORD")
    if not db_password:
        raise ConfigurationError(
            "POSTGRES_PASSWORD environment variable is required but not set in the environment or .env."
        )
    db_schema = os.getenv("POSTGRES_SCHEMA", "public")

    database_config = DatabaseConfig(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
        db_schema=db_schema,
    )

    # 2. Google Cloud Storage Configuration
    gcp_project_id = os.getenv("GCP_PROJECT_ID")
    bucket_name = (
        os.getenv("GCS_BUCKET_RAW")
        or os.getenv("GCS_BUCKET_LANDING")
        or os.getenv("GCS_BUCKET_NAME")
    )
    gcs_prefix = normalize_gcs_prefix(os.getenv("GCS_RAW_PREFIX", DEFAULT_GCS_RAW_PREFIX))
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    gcs_config = GCSConfig(
        project_id=gcp_project_id,
        bucket_name=bucket_name,
        base_prefix=gcs_prefix,
        credentials_path=credentials_path,
        allow_overwrite=allow_overwrite,
    )

    if require_gcs:
        gcs_config.validate()

    # 3. Local Staging and Extraction Settings
    local_staging_raw = os.getenv("LOCAL_STAGING_DIR", DEFAULT_STAGING_DIR)
    local_staging_dir = Path(local_staging_raw)

    if tables is None:
        configured_tables_env = os.getenv("BATCH_INGESTION_TABLES")
        if configured_tables_env:
            raw_tables = tuple(
                tbl.strip() for tbl in configured_tables_env.split(",") if tbl.strip()
            )
        else:
            raw_tables = DEFAULT_SOURCE_TABLES
    else:
        raw_tables = tuple(tables)

    validated_tables = validate_source_tables(raw_tables)

    # Ingestion date validation
    if ingestion_date:
        active_date = validate_ingestion_date(ingestion_date)
    else:
        active_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    active_chunk_size = chunk_size or DEFAULT_FETCH_CHUNK_SIZE
    if active_chunk_size <= 0:
        raise ConfigurationError("chunk_size must be a positive integer (> 0).")

    return BatchPipelineConfig(
        database=database_config,
        gcs=gcs_config,
        local_staging_dir=local_staging_dir,
        tables=validated_tables,
        ingestion_date=active_date,
        chunk_size=active_chunk_size,
    )
