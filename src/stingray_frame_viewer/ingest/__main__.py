"""CSV ingest CLI entry point.

Aggregates per-frame CSVs into the manifest store's ``videos`` (and optionally
``frames``) tables. Append-only; refuses to write to ``(cruise, camera)``
partitions that already exist in the manifest. Re-ingesting an existing cruise
requires an out-of-band full reingest (no ``--overwrite`` flag by design).
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from pathlib import Path

from ..manifest import ensure_frames_table, ensure_videos_table, open_store
from .aggregate import (
    aggregate_frames,
    aggregate_videos,
    count_bad_file_videos,
    count_id_link_nonempty,
    distinct_cruise_camera,
)


def _expand_globs(patterns: list[str]) -> list[str]:
    paths: list[str] = []
    for pat in patterns:
        matches = sorted(glob.glob(pat))
        if not matches:
            print(f"WARNING: glob matched no files: {pat}", file=sys.stderr)
        paths.extend(matches)
    return paths


def _existing_partitions(store) -> set[tuple[str, str]]:
    rows = store.distinct_values("videos", ["cruise", "camera"])
    return {(r["cruise"], r["camera"]) for r in rows}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="stingray_frame_viewer.ingest",
        description="Ingest Stingray per-frame CSVs into the manifest store.",
    )
    parser.add_argument(
        "--csv",
        action="append",
        required=True,
        help="Glob matching one or more CSV files (may be repeated).",
    )
    parser.add_argument(
        "--store-root",
        default=os.environ.get("STINGRAY_STORE_ROOT"),
        help="Manifest store root (local path or s3://...). Falls back to STINGRAY_STORE_ROOT.",
    )
    parser.add_argument("--frames", action="store_true", help="Also build the per-frame table.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Aggregate and print a summary; do not write to the store.",
    )
    args = parser.parse_args(argv)

    if not args.store_root:
        parser.error("--store-root is required (or set STINGRAY_STORE_ROOT)")

    csv_paths = _expand_globs(args.csv)
    if not csv_paths:
        print("ERROR: no CSV files matched the supplied glob(s)", file=sys.stderr)
        return 1

    print(f"reading {len(csv_paths)} csv file(s)", file=sys.stderr)

    nonempty = count_id_link_nonempty(csv_paths)
    if nonempty:
        print(
            f"WARNING: {nonempty} rows have non-empty id/link "
            "(DESIGN.md says these are empty today — escalate)",
            file=sys.stderr,
        )

    bad_videos = count_bad_file_videos(csv_paths)
    if bad_videos:
        print(
            f"NOTE: skipping {bad_videos} video(s) flagged as unreadable in the CSV "
            "(status='bad_file', no frame rows) — these are absent from the manifest",
            file=sys.stderr,
        )

    videos_df = aggregate_videos(csv_paths)
    partitions = distinct_cruise_camera(videos_df)
    print(f"aggregated {videos_df.height} video(s) across {len(partitions)} partition(s)")

    store = open_store(args.store_root)
    ensure_videos_table(store)
    if args.frames:
        ensure_frames_table(store)

    existing = _existing_partitions(store)
    conflicts = sorted(set(partitions) & existing)
    if conflicts:
        print(
            "ERROR: refusing to write — these (cruise, camera) partitions already exist:",
            file=sys.stderr,
        )
        for cruise, camera in conflicts:
            print(f"  cruise={cruise} camera={camera}", file=sys.stderr)
        print(
            "Re-ingest requires an out-of-band full reingest (the CLI is append-only).",
            file=sys.stderr,
        )
        return 2

    if args.dry_run:
        for cruise, camera in partitions:
            subset = videos_df.filter(
                (videos_df["cruise"] == cruise) & (videos_df["camera"] == camera)
            )
            print(f"  [dry-run] cruise={cruise} camera={camera} videos={subset.height}")
        print(f"done [dry-run]: {len(partitions)} partition(s), {videos_df.height} video(s)")
        return 0

    videos_arrow = videos_df.to_arrow()
    store.write("videos", videos_arrow)

    frame_total = 0
    if args.frames:
        # TODO(M7): full-corpus runs need chunking — to_arrow() materializes
        # the whole frame in RAM. Fine for the per-cruise smoke test.
        frames_df = aggregate_frames(csv_paths)
        frame_total = frames_df.height
        store.write("frames", frames_df.to_arrow())

    for cruise, camera in partitions:
        subset = videos_df.filter(
            (videos_df["cruise"] == cruise) & (videos_df["camera"] == camera)
        )
        msg = f"writing cruise={cruise} camera={camera} videos={subset.height}"
        if args.frames:
            msg += f" frames={int(subset['frame_count'].sum())}"
        print(msg)

    summary = f"done: {len(partitions)} partition(s), {videos_df.height} video(s)"
    if args.frames:
        summary += f", {frame_total} frame(s)"
    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())


def _entrypoint() -> int:  # pragma: no cover
    return main()
