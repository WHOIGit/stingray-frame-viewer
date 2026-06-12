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

import polars as pl

from ..manifest import ensure_frames_table, ensure_videos_table, open_store
from .aggregate import (
    aggregate_videos,
    count_excluded_videos,
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
        "--skip-existing",
        action="store_true",
        help="Write only partitions not already in the manifest and skip the rest, "
        "instead of refusing the whole run when any (cruise, camera) already exists. "
        "Makes adding a new cruise idempotent — point at the full corpus and only the "
        "new partitions are written.",
    )
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
        # Historically these columns were empty and a non-empty value was an
        # escalation trigger; in practice they are now routinely populated and
        # carry nothing the manifest uses, so this is DEBUG-level only.
        log.debug("%d rows have non-empty id/link (informational; columns are ignored)", nonempty)

    with _phase("scan: excluded videos (bad-file + skip)"):
        excluded = count_excluded_videos(csv_paths)
    if excluded.bad_file:
        log.info(
            "skipping %d video(s) flagged unreadable in the CSV "
            "(status='bad_file', no frame rows) — absent from the manifest",
            excluded.bad_file,
        )
    if excluded.skipped:
        log.info(
            "skipping %d video(s) parked under a 'skip/' directory "
            "(operator-marked — absent from the manifest)",
            excluded.skipped,
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
    # A dry run *previews* — it reports conflicts as information and still prints
    # the full plan. Only a real run acts on them, per --skip-existing below.
    if args.dry_run:
        existing_fate = "would be skipped" if args.skip_existing else "would be rejected"
        for cruise, camera in partitions:
            subset = videos_df.filter(
                (videos_df["cruise"] == cruise) & (videos_df["camera"] == camera)
            )
            status = f"EXISTS — {existing_fate}" if (cruise, camera) in existing else "new"
            log.info(
                "[dry-run] cruise=%s camera=%s videos=%d (%s)",
                cruise, camera, subset.height, status,
            )
        new_count = len(partitions) - len(conflicts)
        log.info(
            "done [dry-run]: %d partition(s) — %d new, %d already exist; %d video(s) in %.1fs",
            len(partitions),
            new_count,
            len(conflicts),
            videos_df.height,
            time.monotonic() - run_start,
        )
        return 0

    # Decide what to actually write. Default is all-or-nothing (refuse on any
    # conflict). --skip-existing instead drops the already-present partitions
    # and writes only the new ones, at (cruise, camera) granularity.
    if conflicts and not args.skip_existing:
        log.error("refusing to write — these (cruise, camera) partitions already exist:")
        for cruise, camera in conflicts:
            log.error("  cruise=%s camera=%s", cruise, camera)
        log.error(
            "Re-ingest requires an out-of-band full reingest, or pass --skip-existing "
            "to write only the new partitions (the CLI is append-only)."
        )
        return 2

    write_partitions = [p for p in partitions if p not in existing]
    if args.skip_existing and conflicts:
        for cruise, camera in conflicts:
            log.info(
                "skip-existing: cruise=%s camera=%s already in manifest — not rewriting",
                cruise, camera,
            )
    if not write_partitions:
        log.info(
            "nothing new to write — all %d partition(s) already exist (%.1fs)",
            len(partitions), time.monotonic() - run_start,
        )
        return 0

    # Restrict the videos rows (and the set of frame video_ids) to the partitions
    # we're actually writing. With no skipping this is a no-op pass-through.
    if len(write_partitions) == len(partitions):
        videos_to_write = videos_df
    else:
        wp_df = pl.DataFrame(
            {"cruise": [c for c, _ in write_partitions], "camera": [cam for _, cam in write_partitions]}
        )
        videos_to_write = videos_df.join(wp_df, on=["cruise", "camera"], how="semi")

    with _phase(f"write: videos table ({videos_to_write.height} rows)"):
        store.write("videos", videos_to_write.to_arrow())

    frame_total = 0
    if args.frames:
        # Write one cruise at a time so peak RAM is bounded to a single cruise's
        # frames rather than the whole corpus (see iter_frame_chunks). For a
        # cruise that only partially exists (a new camera on an existing cruise),
        # iter_frame_chunks yields every camera's frames for that cruise, so we
        # filter each chunk down to the videos we're actually writing before the
        # append — otherwise the existing camera's frames would be re-appended
        # into the cruise's (cruise-keyed) frame partition.
        writable_ids = set(videos_to_write["video_id"].to_list())
        frame_cruises = sorted({cruise for cruise, _ in write_partitions})
        log.info("writing frames for %d cruise(s)", len(frame_cruises))
        for cruise, chunk in iter_frame_chunks(csv_paths, frame_cruises):
            if len(write_partitions) != len(partitions):
                chunk = chunk.filter(pl.col("video_id").is_in(writable_ids))
            with _phase(f"write: frames cruise={cruise} ({chunk.height} rows)"):
                store.write("frames", chunk.to_arrow())
            frame_total += chunk.height

    summary = f"done: {len(write_partitions)} partition(s), {videos_to_write.height} video(s)"
    if conflicts and args.skip_existing:
        summary += f" ({len(conflicts)} existing partition(s) skipped)"
    if args.frames:
        summary += f", {frame_total} frame(s)"
    summary += f" in {time.monotonic() - run_start:.1f}s"
    log.info(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())


def _entrypoint() -> int:  # pragma: no cover
    return main()
