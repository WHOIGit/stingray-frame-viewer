"""CSV ingest CLI entry point.

Aggregates per-frame CSVs into the manifest store's ``videos`` (and optionally
``frames``) tables. Append-only; refuses to write to ``(cruise, camera)``
partitions that already exist in the manifest. Re-ingesting an existing cruise
requires an out-of-band full reingest (no ``--overwrite`` flag by design).
"""
from __future__ import annotations

import argparse
import fnmatch
import glob
import logging
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from ..manifest import ensure_frames_table, ensure_videos_table, open_store
from .aggregate import (
    aggregate_videos,
    count_bad_file_videos,
    count_id_link_nonempty,
    distinct_cruise_camera,
    iter_frame_chunks,
)

log = logging.getLogger("stingray_frame_viewer.ingest")


def _configure_logging(verbose: bool) -> None:
    """Attach a timestamped stderr handler to the ingest logger (idempotent).

    Configures only this package's logger, not the root, so embedding the CLI
    (or running it under pytest's caplog) doesn't clobber anyone else's logging.
    ``--verbose`` drops the threshold to DEBUG, which adds per-file and
    per-partition detail on top of the default per-phase INFO timeline.
    """
    log.setLevel(logging.DEBUG if verbose else logging.INFO)
    if not log.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")
        )
        log.addHandler(handler)
    log.propagate = False


@contextmanager
def _phase(label: str):
    """Log a phase boundary with wall-clock duration.

    Each Stingray ingest phase is a full pass over every CSV (4.7M+ rows on a
    cruise corpus), so emitting a start line and an elapsed-time end line is the
    difference between "looks stuck" and "knowing the bad-file scan took 40s".
    """
    log.info("→ %s", label)
    start = time.monotonic()
    try:
        yield
    finally:
        log.info("  %s (%.1fs)", label, time.monotonic() - start)


def _expand_globs(patterns: list[str]) -> list[str]:
    paths: list[str] = []
    for pat in patterns:
        matches = sorted(glob.glob(pat))
        if not matches:
            log.warning("glob matched no files: %s", pat)
        paths.extend(matches)
    return paths


def _apply_excludes(
    paths: list[str], patterns: list[str]
) -> tuple[list[str], list[str]]:
    """Split ``paths`` into (kept, dropped) by fnmatch against the full path.

    Patterns are matched with ``fnmatch.fnmatchcase`` against each complete
    path string — ``*`` spans ``/``, so ``*_fast.csv`` matches any file ending
    that way, and ``*/ISIIS2/*`` excludes a whole subdirectory.
    """
    if not patterns:
        return paths, []
    kept: list[str] = []
    dropped: list[str] = []
    for p in paths:
        if any(fnmatch.fnmatchcase(p, pat) for pat in patterns):
            dropped.append(p)
        else:
            kept.append(p)
    return kept, dropped


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
        "--exclude",
        action="append",
        default=[],
        metavar="PATTERN",
        help="fnmatch pattern tested against each CSV path; matching files are "
        "skipped (may be repeated). Example: --exclude '*_fast.csv'",
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
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose (DEBUG) logging: per-file and per-partition detail.",
    )
    args = parser.parse_args(argv)

    _configure_logging(args.verbose)
    run_start = time.monotonic()

    if not args.store_root:
        parser.error("--store-root is required (or set STINGRAY_STORE_ROOT)")

    csv_paths = _expand_globs(args.csv)
    if not csv_paths:
        log.error("no CSV files matched the supplied glob(s)")
        return 1

    csv_paths, excluded = _apply_excludes(csv_paths, args.exclude)
    if excluded:
        log.info("excluding %d file(s) matching --exclude", len(excluded))
        for p in excluded:
            log.debug("  excluded: %s", p)
    if not csv_paths:
        log.error("no CSV files left after applying --exclude")
        return 1

    log.info(
        "ingest start: %d csv file(s), store_root=%s, frames=%s, dry_run=%s",
        len(csv_paths),
        args.store_root,
        args.frames,
        args.dry_run,
    )
    for p in csv_paths:
        log.debug("  csv: %s", p)

    with _phase("scan: id/link sanity check"):
        nonempty = count_id_link_nonempty(csv_paths)
    if nonempty:
        log.warning(
            "%d rows have non-empty id/link (DESIGN.md says these are empty today — escalate)",
            nonempty,
        )

    with _phase("scan: bad-file sentinels"):
        bad_videos = count_bad_file_videos(csv_paths)
    if bad_videos:
        log.info(
            "skipping %d video(s) flagged unreadable in the CSV "
            "(status='bad_file', no frame rows) — absent from the manifest",
            bad_videos,
        )

    with _phase("aggregate: videos"):
        videos_df = aggregate_videos(csv_paths)
        partitions = distinct_cruise_camera(videos_df)
    log.info(
        "aggregated %d video(s) across %d partition(s)",
        videos_df.height,
        len(partitions),
    )
    for cruise, camera in partitions:
        log.debug("  partition: cruise=%s camera=%s", cruise, camera)

    with _phase("open store + ensure tables"):
        store = open_store(args.store_root)
        ensure_videos_table(store)
        if args.frames:
            ensure_frames_table(store)

    with _phase("check existing partitions"):
        existing = _existing_partitions(store)
    conflicts = sorted(set(partitions) & existing)
    if conflicts:
        log.error("refusing to write — these (cruise, camera) partitions already exist:")
        for cruise, camera in conflicts:
            log.error("  cruise=%s camera=%s", cruise, camera)
        log.error("Re-ingest requires an out-of-band full reingest (the CLI is append-only).")
        return 2

    if args.dry_run:
        for cruise, camera in partitions:
            subset = videos_df.filter(
                (videos_df["cruise"] == cruise) & (videos_df["camera"] == camera)
            )
            log.info("[dry-run] cruise=%s camera=%s videos=%d", cruise, camera, subset.height)
        log.info(
            "done [dry-run]: %d partition(s), %d video(s) in %.1fs",
            len(partitions),
            videos_df.height,
            time.monotonic() - run_start,
        )
        return 0

    with _phase(f"write: videos table ({videos_df.height} rows)"):
        store.write("videos", videos_df.to_arrow())

    frame_total = 0
    if args.frames:
        # Write one cruise at a time so peak RAM is bounded to a single
        # cruise's frames rather than the whole corpus (see iter_frame_chunks).
        # Each cruise is a fresh `frames` partition (conflicts rejected above),
        # so these appends never collide. This is the long pole on a full
        # corpus, so each cruise gets its own timed phase.
        frame_cruises = sorted({cruise for cruise, _ in partitions})
        log.info("writing frames for %d cruise(s)", len(frame_cruises))
        for cruise, chunk in iter_frame_chunks(csv_paths, frame_cruises):
            with _phase(f"write: frames cruise={cruise} ({chunk.height} rows)"):
                store.write("frames", chunk.to_arrow())
            frame_total += chunk.height

    summary = f"done: {len(partitions)} partition(s), {videos_df.height} video(s)"
    if args.frames:
        summary += f", {frame_total} frame(s)"
    summary += f" in {time.monotonic() - run_start:.1f}s"
    log.info(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())


def _entrypoint() -> int:  # pragma: no cover
    return main()
