"""pydantic-settings configuration.

Reads ``STINGRAY_*`` environment variables (see ``.env.example``): manifest
store root, manifest S3 credentials, cache toggle and credentials, default
frame format, JPEG quality, and neighbors-endpoint window.

The cache_* and neighbor_window fields are parsed today for forward
compatibility but are not consulted by the M4 route layer — they land when
the cache (M5) and neighbors endpoint (M6) are wired in.
"""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="STINGRAY_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    store_root: str
    store_s3_endpoint: str | None = None
    store_s3_access_key: str | None = None
    store_s3_secret_key: str | None = None

    cache_enabled: bool = False
    cache_bucket: str | None = None
    cache_s3_endpoint: str | None = None
    cache_s3_access_key: str | None = None
    cache_s3_secret_key: str | None = None

    default_format: str = "png"
    jpeg_quality: int = 90
    neighbor_window: str = "1,5"
