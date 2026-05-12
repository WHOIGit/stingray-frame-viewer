"""Polars-based CSV aggregation for the ingest CLI.

Pure functions, no I/O against the manifest store. The CLI in ``__main__.py``
wires these into the store-write path. Splitting them out keeps the
aggregation directly unit-testable.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl


def parse_cruise_camera(media_path: str) -> tuple[str, str]:
    """Extract ``(cruise, camera)`` from a Stingray ``media_path``.

    Stingray paths follow ``/proj/nes-lter/Stingray/data/{cruise}/{camera}/{ts}/{file}.avi``
    (see DESIGN.md). We anchor on the literal ``"Stingray"`` segment so the parser
    works regardless of mount-point prefix.
    """
    parts = Path(media_path).parts
    try:
        anchor = parts.index("Stingray")
    except ValueError as exc:
        raise ValueError(f"media_path does not contain 'Stingray' segment: {media_path}") from exc
    if anchor + 3 >= len(parts):
        raise ValueError(f"media_path is too short to contain cruise/camera: {media_path}")
    # anchor + 1 is "data", anchor + 2 is cruise, anchor + 3 is camera
    return parts[anchor + 2], parts[anchor + 3]


def _scan(csv_paths: Iterable[str | Path]) -> pl.LazyFrame:
    return pl.scan_csv([str(p) for p in csv_paths])


def _scan_ok_frames(csv_paths: Iterable[str | Path]) -> pl.LazyFrame:
    """Scan only the per-frame rows; drop sentinel rows for unreadable videos.

    The Stingray CSV emits a single ``status='bad_file'`` row (with null
    ``frame``/``times``) per unreadable video instead of per-frame rows. Those
    videos have no frames to serve, so they don't belong in the manifest.
    """
    return _scan(csv_paths).filter(pl.col("frame").is_not_null())


def count_bad_file_videos(csv_paths: Iterable[str | Path]) -> int:
    """Count distinct media_path values flagged as unreadable in the CSV."""
    return int(
        _scan(csv_paths)
        .filter(pl.col("frame").is_null())
        .select(pl.col("media_path").n_unique())
        .collect()
        .item()
    )


def aggregate_videos(csv_paths: Iterable[str | Path]) -> pl.DataFrame:
    """One row per video; counts frames and pulls first ``media_time``.

    Returns a DataFrame with the ``Video`` schema columns:
    ``video_id, media_path, frame_count, media_time, cruise, camera``.
    """
    grouped = (
        _scan_ok_frames(csv_paths)
        .group_by("media_path")
        .agg(
            pl.col("media").first().alias("video_id"),
            pl.len().alias("frame_count"),
            pl.col("media_time").first().alias("media_time"),
        )
        .collect()
    )

    cruises = []
    cameras = []
    for p in grouped["media_path"].to_list():
        c, cam = parse_cruise_camera(p)
        cruises.append(c)
        cameras.append(cam)

    return grouped.with_columns(
        pl.Series("cruise", cruises),
        pl.Series("camera", cameras),
        pl.col("media_time").str.to_datetime(strict=False, time_zone="UTC"),
        pl.col("frame_count").cast(pl.Int64),
    ).select(["video_id", "media_path", "frame_count", "media_time", "cruise", "camera"])


def aggregate_frames(csv_paths: Iterable[str | Path]) -> pl.DataFrame:
    """One row per frame, sorted for parquet range pruning (see DESIGN.md).

    Bad-file sentinel rows are dropped (see ``_scan_ok_frames``).
    """
    scanned = _scan_ok_frames(csv_paths).collect()
    cruises = [parse_cruise_camera(p)[0] for p in scanned["media_path"].to_list()]
    return (
        scanned.with_columns(
            pl.Series("cruise", cruises),
            pl.col("media").alias("video_id"),
            pl.col("frame").cast(pl.Int64).alias("frame_index"),
            pl.col("times").str.to_datetime(strict=False, time_zone="UTC").alias("frame_time"),
        )
        .select(["video_id", "frame_index", "frame_time", "status", "cruise"])
        .sort(["cruise", "video_id", "frame_index"])
    )


def count_id_link_nonempty(csv_paths: Iterable[str | Path]) -> int:
    """How many rows have non-empty ``id`` or ``link``? Zero is the expected case.

    DESIGN.md says these columns are empty today; non-zero is a design-trigger
    warning. Returns 0 if the columns are absent (older CSVs).
    """
    schema_names = _scan(csv_paths).collect_schema().names()
    cond = None
    for col in ("id", "link"):
        if col not in schema_names:
            continue
        c = pl.col(col).is_not_null() & (pl.col(col).cast(pl.Utf8) != "")
        cond = c if cond is None else (cond | c)
    if cond is None:
        return 0
    return int(_scan(csv_paths).filter(cond).select(pl.len()).collect().item())


def distinct_cruise_camera(videos_df: pl.DataFrame) -> list[tuple[str, str]]:
    """Sorted unique ``(cruise, camera)`` pairs in the aggregated videos frame."""
    unique = videos_df.select(["cruise", "camera"]).unique().sort(["cruise", "camera"])
    return [(row["cruise"], row["camera"]) for row in unique.iter_rows(named=True)]
