"""pydantic-settings configuration.

Reads ``STINGRAY_*`` environment variables (see ``.env.example``): manifest
store root, manifest S3 credentials, cache toggle and credentials, default
frame format, JPEG quality, and neighbors-endpoint window.

When ``STINGRAY_CACHE_ENABLED=true``, the route layer uses the cache settings
to read/write encoded frame bytes through amplify-storage-utils' BucketStore.
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
