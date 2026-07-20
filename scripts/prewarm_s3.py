"""stingray-prewarm: batch pre-render frames into the VAST S3 cache.

Extracts every frame of every video found under a directory and writes the
encoded PNGs to S3 using the *same* keys the service looks up at request time.
A warmed video then serves entirely from the cache (one S3 GET) instead of
decoding on the fly, so first-touch latency for existing videos disappears.

Correctness hinges on one invariant: the cache key is
``cache_key(video_id, frame_index, fmt)`` where ``video_id`` is the MANIFEST
key — not the filename. So this tool loads the manifest for ``video_id`` and
the authoritative ``frame_count``, and matches on-disk AVIs to manifest rows by
filename. Reading happens from the on-disk file (which may live at a different
path than the manifest's ``media_path`` on this host); only the manifest's
``video_id`` is used for the key. A file with no manifest row is skipped — a
warmed key the service never requests would be wasted work.

Re-runs are cheap and safe: already-present frames are listed once per video
(one paginated LIST) and skipped, so the job is resumable. Nothing is ever
written to the source AVIs.

Usage:

  STINGRAY_STORE_ROOT=... \
  STINGRAY_CACHE_BUCKET=... STINGRAY_CACHE_S3_ENDPOINT=http://vast.whoi.edu \
  STINGRAY_CACHE_S3_ACCESS_KEY=... STINGRAY_CACHE_S3_SECRET_KEY=... \
    stingray-prewarm --avi-dir /srv/Stingray_data/data/NESLTER_AR99 [options]

Options of note: --dry-run (plan only), --overwrite (re-render existing),
--workers N (parallel uploads), --max-frames N (cap per video, for testing),
--video ID (restrict to one manifest video).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

from stingray_frame_viewer.cache import build_cache_store, cache_key, video_prefix
from stingray_frame_viewer.config import Settings
from stingray_frame_viewer.encoder import encode
from stingray_frame_viewer.manifest import load_manifest, open_store
from stingray_frame_viewer.models import Video

logger = logging.getLogger("stingray.prewarm")


# --------------------------------------------------------------------------- #
# manifest <-> disk reconciliation                                            #
# --------------------------------------------------------------------------- #
def index_by_basename(manifest: dict[str, Video]) -> dict[str, list[Video]]:
    """Map ``basename(media_path) -> [Video, ...]`` for on-disk matching."""
    idx: dict[str, list[Video]] = defaultdict(list)
    for v in manifest.values():
        idx[Path(v.media_path).name].append(v)
    return idx


def discover_avis(avi_dir: Path) -> list[Path]:
    """All ``*.avi`` under ``avi_dir`` (recursive), sorted for stable order."""
    return sorted(p for p in avi_dir.rglob("*.avi") if p.is_file())


def existing_indices(store, video_id: str, ext: str, prefix: str) -> set[int]:
    """Frame indices already present in S3 for this video (one LIST call).

    Lists under the SAME per-video prefix cache_key writes to (via
    video_prefix), so the skip-scan and the writer can't disagree.
    """
    list_prefix = video_prefix(video_id, prefix)
    suffix = f".{ext}"
    out: set[int] = set()
    for key in store.keys(list_prefix):
        if key.endswith(suffix):
            name = key[len(list_prefix):-len(suffix)]  # the "{frame_index}" segment
            if name.isdigit():
                out.add(int(name))
    return out


# --------------------------------------------------------------------------- #
# per-video warming                                                           #
# --------------------------------------------------------------------------- #
class Counts:
    __slots__ = ("uploaded", "skipped", "failed")

    def __init__(self) -> None:
        self.uploaded = self.skipped = self.failed = 0

    def add(self, other: "Counts") -> None:
        self.uploaded += other.uploaded
        self.skipped += other.skipped
        self.failed += other.failed


def _safe_put(store, key: str, body: bytes) -> bool:
    try:
        store.put(key, body)
        return True
    except Exception as exc:  # noqa: BLE001 - report, don't abort the batch
        logger.error("upload failed key=%s: %s: %s", key, type(exc).__name__, exc)
        return False


def warm_video(
    store,
    video: Video,
    disk_path: Path,
    *,
    fmt: str,
    n_frames: int,
    already: set[int],
    overwrite: bool,
    jpeg_quality: int,
    prefix: str,
    pool: ThreadPoolExecutor,
    inflight: threading.Semaphore,
) -> Counts:
    """Decode ``disk_path`` sequentially and upload the wanted frames."""
    import cv2  # local: keep module import cheap for --help / non-cv2 paths

    c = Counts()
    cap = cv2.VideoCapture(str(disk_path))
    if not cap.isOpened():
        logger.error("could not open %s — counting %d frames as failed", disk_path, n_frames)
        c.failed = n_frames
        return c

    futures: list[Future] = []
    ext = fmt if fmt == "png" else "jpg"
    try:
        for i in range(n_frames):
            if i in already and not overwrite:
                cap.grab()          # advance the decoder without decoding — cheap skip
                c.skipped += 1
                continue

            ok, frame = cap.read()  # grab + decode
            if not ok or frame is None:
                remaining = n_frames - i
                logger.warning(
                    "%s: decode stopped at frame %d (manifest says %d); "
                    "%d frames unavailable",
                    video.video_id, i, n_frames, remaining,
                )
                c.failed += remaining
                break

            if frame.ndim == 3:
                frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            body = encode(frame, fmt, jpeg_quality=jpeg_quality)
            key = cache_key(video.video_id, i, fmt, prefix=prefix)

            inflight.acquire()      # bound in-flight uploads (memory: ~body size each)
            fut = pool.submit(_safe_put, store, key, body)
            fut.add_done_callback(lambda f: inflight.release())
            futures.append(fut)
    finally:
        cap.release()

    for fut in futures:
        if fut.result():
            c.uploaded += 1
        else:
            c.failed += 1
    return c


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #
def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="stingray-prewarm",
        description="Pre-render video frames into the VAST S3 cache.",
    )
    ap.add_argument("--avi-dir", required=True, type=Path,
                    help="directory scanned recursively for *.avi files")
    ap.add_argument("--format", default="png", choices=("png", "jpeg"),
                    help="encoding format (default: png)")
    ap.add_argument("--workers", type=int, default=8,
                    help="parallel S3 uploads (default: 8)")
    ap.add_argument("--overwrite", action="store_true",
                    help="re-render and re-upload frames already in S3")
    ap.add_argument("--dry-run", action="store_true",
                    help="report the plan (matched videos, frames to upload); write nothing")
    ap.add_argument("--video", metavar="VIDEO_ID",
                    help="restrict to a single manifest video_id")
    ap.add_argument("--limit-videos", type=int,
                    help="process at most N videos (for testing)")
    ap.add_argument("--max-frames", type=int,
                    help="cap frames per video (for testing)")
    ap.add_argument("-v", "--verbose", action="store_true")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    if not args.avi_dir.is_dir():
        print(f"--avi-dir is not a directory: {args.avi_dir}", file=sys.stderr)
        return 2

    settings = Settings()
    if not settings.cache_bucket:
        print("STINGRAY_CACHE_BUCKET (and the other STINGRAY_CACHE_* vars) must be set.",
              file=sys.stderr)
        return 2
    if not settings.cache_enabled:
        logger.warning("STINGRAY_CACHE_ENABLED is false — warming S3 anyway, but the "
                       "service won't read the cache until you enable it there.")

    fmt = args.format
    ext = fmt if fmt == "png" else "jpg"

    # Load the manifest exactly like the service does — same source of truth.
    store_handle = open_store(
        settings.store_root,
        s3_endpoint=settings.store_s3_endpoint,
        s3_access_key=settings.store_s3_access_key,
        s3_secret_key=settings.store_s3_secret_key,
    )
    manifest = load_manifest(store_handle)
    by_name = index_by_basename(manifest)

    # Match on-disk AVIs to manifest rows.
    avis = discover_avis(args.avi_dir)
    if not avis:
        print(f"no .avi files found under {args.avi_dir}", file=sys.stderr)
        return 1

    worklist: list[tuple[Video, Path]] = []
    skipped_unmatched = skipped_ambiguous = 0
    seen_ids: set[str] = set()
    for path in avis:
        rows = by_name.get(path.name, [])
        if not rows:
            skipped_unmatched += 1
            logger.debug("no manifest row for %s — skipping", path.name)
            continue
        if len(rows) > 1:
            skipped_ambiguous += 1
            logger.warning("%s matches %d manifest rows — skipping (ambiguous)",
                           path.name, len(rows))
            continue
        video = rows[0]
        if args.video and video.video_id != args.video:
            continue
        if video.video_id in seen_ids:
            logger.warning("%s already matched — skipping duplicate on disk", path.name)
            continue
        seen_ids.add(video.video_id)
        worklist.append((video, path))

    if args.limit_videos:
        worklist = worklist[: args.limit_videos]

    print(f"matched {len(worklist)} video(s); "
          f"{skipped_unmatched} unmatched, {skipped_ambiguous} ambiguous "
          f"(of {len(avis)} .avi on disk)")
    if not worklist:
        return 1

    cache_store = build_cache_store(settings)

    # --- dry run: plan only ------------------------------------------------- #
    if args.dry_run:
        total_upload = total_present = 0
        for video, path in worklist:
            n = min(video.frame_count, args.max_frames) if args.max_frames else video.frame_count
            present = set() if args.overwrite else existing_indices(cache_store, video.video_id, ext, settings.cache_prefix)
            present_in_range = len({i for i in present if i < n})
            to_upload = n - present_in_range
            total_upload += to_upload
            total_present += present_in_range
            print(f"  {video.video_id}: {n} frames, {present_in_range} present, "
                  f"{to_upload} to upload  <- {path}")
        print(f"[dry-run] would upload {total_upload} frame(s); "
              f"{total_present} already present. Nothing written.")
        return 0

    # --- real run ----------------------------------------------------------- #
    totals = Counts()
    inflight = threading.Semaphore(max(2, args.workers * 2))
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for idx, (video, path) in enumerate(worklist, 1):
            n = min(video.frame_count, args.max_frames) if args.max_frames else video.frame_count
            already = set() if args.overwrite else existing_indices(cache_store, video.video_id, ext, settings.cache_prefix)
            t0 = time.monotonic()
            c = warm_video(
                cache_store, video, path,
                fmt=fmt, n_frames=n, already=already, overwrite=args.overwrite,
                jpeg_quality=settings.jpeg_quality, prefix=settings.cache_prefix, pool=pool, inflight=inflight,
            )
            totals.add(c)
            dt = time.monotonic() - t0
            print(f"[{idx}/{len(worklist)}] {video.video_id}: "
                  f"uploaded={c.uploaded} skipped={c.skipped} failed={c.failed} "
                  f"({dt:.1f}s)")

    elapsed = time.monotonic() - started
    print(f"\nDone in {elapsed:.1f}s — "
          f"uploaded={totals.uploaded} skipped={totals.skipped} failed={totals.failed}")
    return 1 if totals.failed else 0


if __name__ == "__main__":
    sys.exit(main())
