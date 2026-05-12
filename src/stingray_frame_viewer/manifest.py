"""amplify-db-utils wrapper for the manifest layer.

The manifest is the index of which videos exist, where they live on disk, and
how many frames each has (see DESIGN.md). At service startup the videos table is
read into an in-memory ``{video_id: Video}`` dict; per-request lookups are
plain dict access. There is no per-request database call.
"""
from __future__ import annotations

from datetime import datetime, timezone

from amplify_db_utils import DuckDBParquetConfig, DuckDBParquetStore

from .models import Frame, Video


def open_store(
    root: str,
    s3_endpoint: str | None = None,
    s3_access_key: str | None = None,
    s3_secret_key: str | None = None,
) -> DuckDBParquetStore:
    """Open the manifest store. Local path or ``s3://...`` URL for ``root``."""
    config = DuckDBParquetConfig(
        root=root,
        s3_endpoint=s3_endpoint,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
    )
    return DuckDBParquetStore(config)


def ensure_videos_table(store: DuckDBParquetStore) -> None:
    store.create_table("videos", Video, partition_by=["cruise", "camera"])


def ensure_frames_table(store: DuckDBParquetStore) -> None:
    store.create_table("frames", Frame, partition_by=["cruise"])


def load_manifest(store: DuckDBParquetStore) -> dict[str, Video]:
    """Materialize the full videos table into an in-memory dict keyed by ``video_id``.

    Normalizes ``media_time`` to UTC. DuckDB returns timestamp[utc] columns
    converted into whatever local TZ the OS is set to — same absolute moment,
    different ``tzinfo``. Anchoring on UTC here keeps the in-memory dict
    consistent across hosts.
    """
    table = store.bulk_read("videos")
    out: dict[str, Video] = {}
    for row in table.to_pylist():
        mt: datetime = row["media_time"]
        if mt.tzinfo is not None:
            row["media_time"] = mt.astimezone(timezone.utc)
        out[row["video_id"]] = Video(**row)
    return out


def lookup(manifest: dict[str, Video], video_id: str) -> Video | None:
    return manifest.get(video_id)
