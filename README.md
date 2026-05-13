# Stingray Frame Viewer

Web service that serves individual frames of Stingray (towed shadowgraph imager) videos to browsers. Sits in front of source AVIs, encodes frames on demand, and (Phase 2+) caches encoded frames in VAST S3.

Design overview: [DESIGN.md](DESIGN.md).

## Status

Phase 1 (on-the-fly extraction) works end-to-end: manifest ingest, frame extraction, PNG/JPEG encoding, and the public HTTP contract are all implemented and tested. The Phase 2 VAST S3 cache, the `/neighbors` endpoint, and the bulk backfill are still to come — see [DESIGN.md](DESIGN.md) §3.

## Install (development)

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

The `amplify-db-utils` and `amplify-storage-utils` dependencies are pinned to their git repositories in [pyproject.toml](pyproject.toml); pip resolves them automatically. No separate editable installs of sibling checkouts are required.

## Test

```bash
.venv/bin/pytest
```

## Run the service

```bash
STINGRAY_STORE_ROOT=./.dev-store \
  .venv/bin/uvicorn stingray_frame_viewer.app:create_app --factory --port 8000
```

Then:

- `curl http://localhost:8000/health` → `{"status":"ok"}`
- `curl http://localhost:8000/videos/<video_id>` → JSON metadata
- `curl -o frame.png http://localhost:8000/frames/<video_id>/0` → PNG (default)
- `curl -o frame.jpg http://localhost:8000/frames/<video_id>/0?format=jpeg` → JPEG

## Inspect a sample AVI

```bash
.venv/bin/python scripts/inspect_avi.py /path/to/sample.avi
```

Prints dimensions, dtype, channel count, and total frame count. Used once during initial bring-up to confirm Stingray AVIs are 8-bit grayscale; the measured values are recorded as a comment block at the top of [src/stingray_frame_viewer/extractor.py](src/stingray_frame_viewer/extractor.py).

## Ingest a cruise's CSVs

```bash
.venv/bin/python -m stingray_frame_viewer.ingest \
    --csv "/path/to/cruise/*.csv" \
    --store-root ./.dev-store \
    [--frames]
```

The ingest CLI is append-only and refuses to write to a `(cruise, camera)` partition that already exists in the manifest. Re-ingesting an existing cruise requires manually clearing the store and starting over.

## Env vars

See [.env.example](.env.example). The ingest CLI honors `STINGRAY_STORE_ROOT` as a fallback for `--store-root`; the service requires it.
