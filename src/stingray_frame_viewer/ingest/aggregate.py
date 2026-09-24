"""Polars-based CSV aggregation for the ingest CLI.

Pure functions, no I/O against the manifest store. The CLI in ``__main__.py``
wires these into the store-write path. Splitting them out keeps the
aggregation directly unit-testable.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, NamedTuple

import polars as pl

# Run every full-corpus collect through Polars' streaming engine. The default
# in-memory engine materializes all referenced columns of every CSV row before
# it aggregates — on a cruise corpus (millions of frame rows) that alone
# exhausts RAM, even though the grouped/filtered output is tiny. The streaming
# engine processes the scan in batches and keeps only the running aggregate
# (group-by hash table / counter) resident. Results are identical; only the
# execution strategy differs.
_ENGINE = "streaming"

# Videos parked under a ``skip/`` directory are operator-marked "do not ingest".
# The marker also breaks cruise/camera parsing: the extra path segment shifts
# the trailing positions (``.../{cruise}/{camera}/skip/{ts}/{file}.avi``), so a
# skip video would otherwise land the camera in the cruise slot and ``skip`` in
# the camera slot. Excluding by path keeps them out of the manifest entirely.
# Case-sensitive, matched as a whole path segment (bounded by slashes).
_SKIP_MARKER = "/skip/"

# LRAUV deployments add a ``basler-videos`` segment between cruise and camera
# (``.../{cruise}/basler-videos/{camera}/{ts}/{file}.avi``). When this is the
# fourth segment from the end, the cruise sits one segment further back.
# Case-sensitive, matched as a whole path segment.
_LRAUV_MARKER = "basler-videos"

# Columns every CSV must carry, read by name so column order and extra columns
# (``media_size``, ``frame_count``, ...) don't matter. Optional columns are
# filled with nulls when absent so every file projects to the same schema.
_REQUIRED = ("media_path", "media", "media_time", "frame", "times", "status")
_OPTIONAL = ("id", "link")


def _is_skipped() -> pl.Expr:
    """True for media_path values that contain a ``skip/`` path segment."""
    return pl.col("media_path").str.contains(_SKIP_MARKER, literal=True)


class ExclusionCounts(NamedTuple):
    """Per-reason counts of videos deliberately left out of the manifest."""

    bad_file: int  # status='bad_file' sentinel: unreadable source, no frames
    skipped: int  # parked under a skip/ directory: operator-marked do-not-ingest


def _normalize(segment: str) -> str:
    """Partition-key-safe form of a path segment: spaces become underscores.

    Cruise and camera are Hive partition directory names; PyArrow would
    URI-encode spaces (``%20``), so normalize them away at ingest.
    """
    return segment.replace(" ", "_")


def parse_cruise_camera(media_path: str) -> tuple[str, str]:
    """Extract ``(cruise, camera)`` from a Stingray ``media_path``.

    Stingray paths end in ``.../{cruise}/{camera}/{timestamp}/{file}.avi`` (see
    DESIGN.md). Only that trailing structure is guaranteed — the prefix is just
    wherever the AVIs happen to be mounted (``/proj/nes-lter/Stingray/data``,
    ``/mnt/stingray_data``, etc.), so we index from the *end* rather than
    anchoring on any fixed segment. This keeps ingest working for CSVs that
    reference arbitrary mount roots.

    LRAUV paths insert a ``basler-videos`` segment after the cruise
    (``.../{cruise}/basler-videos/{camera}/{timestamp}/{file}.avi``; see
    ``_LRAUV_MARKER``). Both values are normalized with :func:`_normalize`.
    """
    parts = Path(media_path).parts
    # Need at least the final four segments: cruise/camera/timestamp/file.
    if len(parts) < 4:
        raise ValueError(
            f"media_path is too short to contain cruise/camera/timestamp/file: {media_path}"
        )
    if parts[-4] == _LRAUV_MARKER:
        # ..., cruise=-5, basler-videos=-4, camera=-3, timestamp=-2, file=-1
        if len(parts) < 5:
            raise ValueError(
                f"media_path is too short to contain cruise/{_LRAUV_MARKER}/camera/"
                f"timestamp/file: {media_path}"
            )
        return _normalize(parts[-5]), _normalize(parts[-3])
    # ..., cruise=-4, camera=-3, timestamp=-2, file=-1
    return _normalize(parts[-4]), _normalize(parts[-3])


def _scan_one(csv_path: str | Path) -> pl.LazyFrame:
    """Scan one CSV, projected by column name to ``_REQUIRED + _OPTIONAL``.

    Every column is read as String (``infer_schema=False``) so per-file type
    inference can't drift between files; callers cast what they need.
    """
    lf = pl.scan_csv(str(csv_path), infer_schema=False)
    names = lf.collect_schema().names()
    missing = [c for c in _REQUIRED if c not in names]
    if missing:
        raise ValueError(f"{csv_path}: missing required column(s): {', '.join(missing)}")
    return lf.select(
        *_REQUIRED,
        *(pl.col(c) if c in names else pl.lit(None, pl.Utf8).alias(c) for c in _OPTIONAL),
    )


def _scan(csv_paths: Iterable[str | Path]) -> pl.LazyFrame:
    """Scan many CSVs as one frame, tolerating differing headers.

    A single multi-file ``pl.scan_csv`` requires identical headers (``schema
    names differ``), which breaks manifests that mix CSV formats. Scanning each
    file by name first gives every file the same schema to concatenate.
    """
    return pl.concat([_scan_one(p) for p in csv_paths], how="vertical")


def _scan_ok_frames(csv_paths: Iterable[str | Path]) -> pl.LazyFrame:
    """Scan the per-frame rows that belong in the manifest.

    Drops two kinds of rows up front, so neither videos nor frames aggregation
    (and neither the cruise/camera parser) ever sees them:

    - ``status='bad_file'`` sentinels (null ``frame``): the Stingray CSV emits
      one per unreadable video instead of per-frame rows — no frames to serve.
    - ``skip/`` directory videos: operator-marked do-not-ingest (see
      ``_SKIP_MARKER``).
    """
    return _scan(csv_paths).filter(pl.col("frame").is_not_null() & ~_is_skipped())


def count_excluded_videos(csv_paths: Iterable[str | Path]) -> ExclusionCounts:
    """Distinct-video counts for each exclusion reason, in a single scan.

    Categories are disjoint: a video parked under ``skip/`` is counted only as
    ``skipped`` even if it also carries a bad-file sentinel.
    """
    res = (
        _scan(csv_paths)
        .select(
            pl.col("media_path")
            .filter(pl.col("frame").is_null() & ~_is_skipped())
            .n_unique()
            .alias("bad_file"),
            pl.col("media_path").filter(_is_skipped()).n_unique().alias("skipped"),
        )
        .collect(engine=_ENGINE)
    )
    return ExclusionCounts(int(res["bad_file"][0]), int(res["skipped"][0]))


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
        .collect(engine=_ENGINE)
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


# Polars expression mirroring ``parse_cruise_camera``'s cruise extraction:
# cruise is the fourth path segment from the end (``.../cruise/camera/ts/file``),
# or the fifth when an LRAUV ``basler-videos`` segment follows it. Anchored to
# end-of-string so it is independent of the mount-point prefix; regex search is
# leftmost, so on an LRAUV path the match starts at the cruise rather than at
# ``basler-videos``. Kept in lockstep with the Python parser
# (``test_cruise_expr_matches_parser``). Unlike the parser this returns null on
# a non-matching path rather than raising — safe here because
# ``aggregate_videos`` (always run first in the CLI) validates every path via
# ``parse_cruise_camera`` before the frames stage is reached.
_CRUISE_PATTERN = rf"([^/]+)/(?:{re.escape(_LRAUV_MARKER)}/)?[^/]+/[^/]+/[^/]+$"


def _cruise_expr() -> pl.Expr:
    """Cruise derived from ``media_path``, normalized like :func:`_normalize`."""
    return (
        pl.col("media_path")
        .str.extract(_CRUISE_PATTERN, 1)
        .str.replace_all(" ", "_", literal=True)
    )


def _frames_lazy(csv_paths: Iterable[str | Path]) -> pl.LazyFrame:
    """Per-frame rows projected to the ``Frame`` schema, with a derived cruise.

    Shared transform behind both :func:`aggregate_frames` (whole corpus) and
    :func:`iter_frame_chunks` (one cruise at a time). Not sorted — callers add
    the ordering they need. Bad-file sentinel rows are dropped (see
    ``_scan_ok_frames``).
    """
    return (
        _scan_ok_frames(csv_paths)
        .with_columns(
            _cruise_expr().alias("cruise"),
            pl.col("media").alias("video_id"),
            pl.col("frame").cast(pl.Int64).alias("frame_index"),
            pl.col("times").str.to_datetime(strict=False, time_zone="UTC").alias("frame_time"),
        )
        .select(["video_id", "frame_index", "frame_time", "status", "cruise"])
    )


def aggregate_frames(csv_paths: Iterable[str | Path]) -> pl.DataFrame:
    """One row per frame, sorted for parquet range pruning (see DESIGN.md).

    Materializes the whole corpus at once — fine for per-cruise smoke tests and
    unit tests. For full-corpus ingest use :func:`iter_frame_chunks`, which
    bounds peak memory to a single cruise.
    """
    return (
        _frames_lazy(csv_paths)
        .sort(["cruise", "video_id", "frame_index"])
        .collect(engine=_ENGINE)
    )


def iter_frame_chunks(
    csv_paths: Iterable[str | Path], cruises: Iterable[str]
):
    """Yield ``(cruise, frames_df)`` one cruise at a time, sorted within cruise.

    Resolves the ``frames`` table's TODO(M7): instead of materializing all
    frames in RAM before a single ``store.write``, this caps peak memory at one
    cruise's worth of rows. Each yielded DataFrame is sorted by
    ``(video_id, frame_index)`` so the per-cruise parquet keeps its
    range-pruning order (cruise is constant within a chunk, so the leading
    cruise sort key from :func:`aggregate_frames` is redundant here).

    Trade-off: the CSVs are lazily re-scanned once per cruise. That trades I/O
    for bounded memory — the right call for an offline backfill on storage that
    can't hold the whole frame corpus at once. ``cruises`` should come from the
    already-validated ``aggregate_videos`` output so every value is real.
    """
    lazy = _frames_lazy(csv_paths)
    for cruise in cruises:
        # Streaming scan+filter so only this cruise's rows are materialized for
        # the sort, not the whole frame corpus (see _ENGINE).
        chunk = (
            lazy.filter(pl.col("cruise") == cruise)
            .sort(["video_id", "frame_index"])
            .collect(engine=_ENGINE)
        )
        yield cruise, chunk


def count_id_link_nonempty(csv_paths: Iterable[str | Path]) -> int:
    """How many rows have non-empty ``id`` or ``link``? Zero is the expected case.

    DESIGN.md says these columns are empty today; non-zero is a design-trigger
    warning. CSVs without the columns contribute nulls (see ``_scan_one``).
    """
    cond = pl.any_horizontal(
        pl.col(c).is_not_null() & (pl.col(c) != "") for c in ("id", "link")
    )
    return int(_scan(csv_paths).filter(cond).select(pl.len()).collect(engine=_ENGINE).item())


def distinct_cruise_camera(videos_df: pl.DataFrame) -> list[tuple[str, str]]:
    """Sorted unique ``(cruise, camera)`` pairs in the aggregated videos frame."""
    unique = videos_df.select(["cruise", "camera"]).unique().sort(["cruise", "camera"])
    return [(row["cruise"], row["camera"]) for row in unique.iter_rows(named=True)]
