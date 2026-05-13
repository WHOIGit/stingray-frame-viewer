# Stingray Frame Viewer — Design Overview

A web service that serves individual frames of Stingray (towed shadowgraph imager) videos to browsers, as a prerequisite for downstream annotation workflows.

This document is a high-level overview. It splits into the architectural design, what currently exists in the repo, and what is planned but not yet built. It is intentionally light on implementation detail — we will revise it as the service grows.

---

## 1. Design

### Purpose and scope

The service is a **loosely coupled component** with a stable public URL contract. Clients (browsers, future annotation tools) talk to the service; the service knows how to find source videos on disk, extract a requested frame, encode it, and return the bytes. The bytes path is allowed to change underneath (currently a lazy VAST S3 cache, eventually substrate-backed) without breaking clients.

The service is prototype work. High availability is not a requirement; latency is not critical in early phases.

### Phased approach

Frames are not pre-extracted up front. The service grows in three phases:

1. **On-the-fly extraction.** The service opens the source AVI on demand, seeks to the requested frame, encodes it, and returns the bytes. No S3 writes.
2. **Lazy caching.** On cache miss, extract as in Phase 1, then write the encoded frame to VAST S3 under a stable cache key, then return. Subsequent requests serve from S3.
3. **Bulk backfill.** Pre-extract the rest of the corpus into VAST S3. The on-the-fly path remains as the cold fallback so new videos work the moment they are indexed.

### High-level architecture

```
                                    +-------------------------------+
                                    |  Source AVIs (read-only)      |
                                    |  /proj/.../Stingray/...       |
                                    +---------------+---------------+
                                                    |
                                                    | seek + read
                                                    v
   browser  ---->  FastAPI service  ---->  Frame extractor (OpenCV)
                      |    ^
                      |    |
                      v    | bytes
                  +--------+---------+
                  | VAST S3 cache    |  <--- amplify-storage-utils
                  | (Phase 2+)       |
                  +------------------+

   service startup:
      manifest store (amplify-db-utils, DuckDB+Parquet)
         --> bulk_read("videos") --> in-memory dict
```

### Public HTTP contract

URLs are stable; the storage backend behind them can change without breaking clients.

| Endpoint | Purpose |
|---|---|
| `GET /frames/{video_id}/{frame_index}?format=png\|jpeg` | Encoded frame bytes. Defaults to PNG (lossless). Sends `Cache-Control: public, max-age=31536000, immutable`. `404` if `video_id` unknown; `416` if frame index out of range. |
| `GET /videos/{video_id}` | Video-level JSON metadata for client-side display and scrubbing. The on-disk path is internal and not exposed. |
| `GET /frames/{video_id}/{frame_index}/neighbors` | Prefetch hints for snappy scrubbing (prev/next ±1, ±5). |
| `GET /health` | Liveness; `200 {"status": "ok"}` once the service is accepting requests. uvicorn's lifespan startup blocks the listen socket until the manifest is loaded, so no separate readiness gate is needed. |

PNG is the default and is always lossless. JPEG is permitted but only the viewer UI should use it; annotation pipelines always request PNG.

### Data model

The manifest lives in [`amplify-db-utils`](https://pypi.org/project/amplify-db-utils/) (DuckDB+Parquet today, VAST DB compatible later). Two tables:

**`videos`** — one row per video, partitioned by `(cruise, camera)`:

```python
class Video(BaseModel):
    video_id: str           # = `media` column from CSV (globally unique)
    media_path: str         # filesystem path to the AVI
    frame_count: int
    media_time: datetime    # tz-aware UTC
    cruise: str             # partition key
    camera: str             # partition key
```

**`frames`** — one row per frame, partitioned by `cruise` only. Built on demand; not required for HTTP routes.

```python
class Frame(BaseModel):
    video_id: str
    frame_index: int
    frame_time: datetime    # tz-aware UTC
    status: str
    cruise: str             # partition key
```

The `video_id` is the unmodified `media` column from the source CSV. The CSV is the source of truth — no derived identifiers, no hashing.

At service startup the `videos` table is read into an in-memory `{video_id: Video}` dict; per-request lookups are plain dict access.

### Source-data assumptions

- Stingray videos are uncompressed AVI. Every frame is independently decodable, so seek-and-decode is cheap and reliable. Confirmed via the inspection script: 2330×1750, 8-bit, grayscale broadcast across 3 channels.
- Source paths follow `/proj/.../Stingray/data/{cruise}/{camera}/{timestamp_dir}/{stem}.avi`. The ingest pipeline parses `cruise` and `camera` from the path.
- The per-frame CSV schema is `media_path, media, media_time, frame, times, status, id, link`.
- CSVs may include a sentinel `status='bad_file'` row (with null `frame` and `times`) marking a video as unreadable. The service excludes such videos from the manifest entirely — there are no frames to serve.
- Source CSV timestamps are naive but represent UTC (the filename `T...Z` suffix confirms intent). The ingest anchors them to UTC to avoid DuckDB's local-TZ round-trip drift.

### Storage and caching

- Manifest: `amplify-db-utils` (DuckDB+Parquet). Append-only writes. Partition keys are immutable.
- Cached encoded frames (Phase 2+): VAST S3 via `amplify-storage-utils`. VAST S3 has no signed-URL support, so cache reads are proxied through the service.
- Cache key shape: `{video_id}_{frame_index}.{ext}` — both PNG and JPEG variants live side by side under separate keys.

### Concurrency model

Synchronous FastAPI handlers (`def`, not `async def`), running in the framework threadpool. No `asyncio`, no singleflight, no streaming. This matches `amplify-storage-utils`' sync interface and the prototype-scale access pattern.

### Out of scope (v1)

Authentication (deferred to a reverse proxy); singleflight / coalesced extraction; nginx `X-Accel-Redirect`; async; chunked streaming; bounding-box overlays; annotation-tool integration.

---

## 2. Existing

What is currently implemented in this repo. Phase 1 (on-the-fly extraction) is end-to-end functional.

**Project scaffold.** [pyproject.toml](pyproject.toml), [.env.example](.env.example), [.gitignore](.gitignore), `pytest` setup. The `amplify-db-utils` and `amplify-storage-utils` dependencies are pinned to their git repositories (resolved by pip at install time); `tool.hatch.metadata.allow-direct-references = true` enables the direct URLs under hatchling.

**Manifest layer.** [src/stingray_frame_viewer/models.py](src/stingray_frame_viewer/models.py) defines the `Video` and `Frame` pydantic models with the schemas above. [src/stingray_frame_viewer/manifest.py](src/stingray_frame_viewer/manifest.py) wraps `amplify-db-utils`:

- `open_store(root, ...)` opens a DuckDB+Parquet store (local path or `s3://...`).
- `ensure_videos_table` / `ensure_frames_table` idempotently create the tables with their partitioning.
- `load_manifest(store)` materializes the full videos table into a `{video_id: Video}` dict, normalizing `media_time` to UTC on read.
- `lookup(manifest, video_id)` is plain dict access.

**CSV ingest CLI.** `python -m stingray_frame_viewer.ingest --csv "<glob>" --store-root <path> [--frames] [--dry-run]`. Implementation in [src/stingray_frame_viewer/ingest/](src/stingray_frame_viewer/ingest/):

- Streams CSVs with `polars.scan_csv`, parses `cruise`/`camera` from the path, aggregates per-video frame counts and start times, and writes a PyArrow table to the manifest store.
- **Append-only by design.** The CLI calls `store.distinct_values("videos", ["cruise", "camera"])` before writing and refuses (exit `2`) to write any partition that already exists. There is no `--overwrite` flag — re-ingesting an existing cruise requires a fresh store.
- Skips `status='bad_file'` videos (no frames to serve) and prints a count.
- Warns if `id` or `link` columns are unexpectedly non-empty (today they should always be empty; if that changes, the design needs to know).

**AVI inspection script.** [scripts/inspect_avi.py](scripts/inspect_avi.py) reads a sample AVI with OpenCV and prints dimensions, dtype, channel layout, and frame count. The measured ground truth is recorded as a comment block in [src/stingray_frame_viewer/extractor.py](src/stingray_frame_viewer/extractor.py) so future readers don't have to re-derive it.

**Frame extraction.** [src/stingray_frame_viewer/extractor.py](src/stingray_frame_viewer/extractor.py) — `extract_frame(media_path, frame_index) -> np.ndarray` returns a 2D uint8 grayscale array. Uses `cv2.VideoCapture` with `CAP_PROP_POS_FRAMES` (reliable on uncompressed AVI) and converts BGR-broadcast frames to single-channel grayscale. Raises `FrameExtractionError` for missing files, unopenable containers, and decode failures.

**Encoding.** [src/stingray_frame_viewer/encoder.py](src/stingray_frame_viewer/encoder.py) — `encode(frame, fmt, jpeg_quality=90) -> bytes` wraps `cv2.imencode`. PNG is lossless and the default; JPEG is opt-in. Unknown formats raise `ValueError` (mapped to 400 at the route layer); `cv2.imencode` failure raises `FrameExtractionError`.

**Domain errors.** [src/stingray_frame_viewer/errors.py](src/stingray_frame_viewer/errors.py) defines `VideoNotFoundError`, `FrameOutOfRangeError`, and `FrameExtractionError`. The exceptions carry useful identifiers (`video_id`, `frame_index`, `frame_count`) on their bodies. `install_handlers(app)` registers FastAPI exception handlers that map them to 404 / 416 / 500 JSON responses.

**Configuration.** [src/stingray_frame_viewer/config.py](src/stingray_frame_viewer/config.py) — pydantic-settings `Settings` class with `STINGRAY_` env prefix. Reads from environment or `.env` file. `extra="ignore"` keeps the schema forward-compatible.

**HTTP service.** [src/stingray_frame_viewer/app.py](src/stingray_frame_viewer/app.py) defines `create_app()`. The async lifespan loads the manifest and pins it on `app.state.manifest`. [src/stingray_frame_viewer/routes.py](src/stingray_frame_viewer/routes.py) implements three endpoints with sync `def` handlers:

- `GET /health` returns `{"status": "ok"}`.
- `GET /videos/{video_id}` returns `{video_id, media_basename, frame_count, last_frame_index, media_time, cruise, camera}`. The on-disk `media_path` stays internal.
- `GET /frames/{video_id}/{frame_index}?format=png|jpeg` extracts on the fly, encodes, and returns the bytes with `Cache-Control: public, max-age=31536000, immutable` and the matching `Content-Type`. 400 on bad format, 404 on unknown video, 416 on out-of-range frame, 500 on extraction failure.

The manifest, settings, and ready flag are exposed to handlers via tiny `Depends` shims so tests can override them with `app.dependency_overrides`.

Run with:

```bash
STINGRAY_STORE_ROOT=./.dev-store \
  uvicorn stingray_frame_viewer.app:create_app --factory
```

**Test suite.** [tests/](tests/) — 34 unit and integration tests covering: manifest round-trip; partition-conflict check; CSV path parser; polars aggregation including the `bad_file` skip; id/link warning logic; extractor seek/decode/error paths against a synthetic FFV1 AVI fixture; PNG byte-exact and JPEG approximate round-trips; all five route endpoints (status codes, headers, body byte-equality against the encoder output).

---

## 3. Future

What is planned but not yet built:

**S3 cache layer.** [src/stingray_frame_viewer/cache.py](src/stingray_frame_viewer/cache.py) — `probe / get / put` against `amplify-storage-utils` for VAST S3. Behind the `STINGRAY_CACHE_ENABLED` config flag so Phase 1 (no cache) and Phase 2 (lazy write-through) are selectable with a single switch. Response `Content-Type` is derived from the cache-key extension at serve time (the storage abstraction does not surface a content-type on `put`).

**Neighbor hints.** `GET /frames/{video_id}/{frame_index}/neighbors` — returns prefetch URLs for adjacent frames; useful for snappy scrubbing. The window is already configurable via `STINGRAY_NEIGHBOR_WINDOW`.

**Per-frame table.** Construction of the `frames` table is supported by the ingest CLI today but not yet consumed. It is the entry point for status-aware skipping and per-frame wall-clock timestamps if a future viewer feature needs them.

**Bulk backfill.** Phase-3 pre-extraction of the corpus into VAST S3. Likely separate operational tooling rather than service code. The on-the-fly path remains the cold fallback.

**Auth.** Bearer-token auth at a reverse proxy in front of the service.

**Substrate migration.** When a shared imaging-instrument data substrate is ready to host Stingray bytes, the public URL contract does not change. The cache module is the migration surface; designing it with a clean interface is the only thing that needs to happen now to keep that migration cheap.

**Structured logging.** One log line per request (video_id, frame_index, format, cache hit/miss, duration) — useful for understanding access patterns once the cache lands.

**Open-`VideoCapture` LRU.** Seek time on uncompressed AVI grows linearly with `frame_index` (≈3 ms / frame on the sample file). A bounded LRU of open `VideoCapture` handles keyed by `media_path` would amortize this for sequential scrubbing. Not needed at prototype scale.
