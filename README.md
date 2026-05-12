# Stingray Frame Viewer

Web service that serves individual frames of Stingray (towed shadowgraph imager) videos to browsers. Sits in front of source AVIs, encodes frames on demand, and (Phase 2+) caches encoded frames in VAST S3.

Full design: [stingray-frame-viewer-DESIGN.md](stingray-frame-viewer-DESIGN.md). Library background: [stingray-frame-viewer-CONTEXT.md](stingray-frame-viewer-CONTEXT.md).

## Status

This repo is being built in milestones (DESIGN §16). M0–M2 are the foundation: scaffold, AVI inspection script, manifest models, and the CSV ingest CLI. HTTP routes (M3+) are not yet implemented.

## Install (development)

The service depends on two sibling libraries that are typically iterated alongside it. Install those editably first, then install this repo editably:

```bash
pip install -e ../amplify-db-utils
pip install -e "../amplify-storage-utils[s3]"
pip install -e ".[dev]"
```

Adjust the sibling paths to match your checkout layout.

## Test

```bash
pytest
```

## Inspect a sample AVI

```bash
python scripts/inspect_avi.py /path/to/sample.avi
```

Prints dimensions, dtype, channel count, and total frame count. Used once during M1 to confirm Stingray AVIs are 8-bit grayscale before extractor code is written.

## Ingest a cruise's CSVs

```bash
python -m stingray_frame_viewer.ingest \
    --csv "/path/to/cruise/*.csv" \
    --store-root ./.dev-store \
    [--frames]
```

The ingest CLI is append-only and refuses to write to a `(cruise, camera)` partition that already exists in the manifest. Re-ingesting an existing cruise requires manually clearing the store and starting over.

## Env vars

See [.env.example](.env.example). The ingest CLI honors `STINGRAY_STORE_ROOT` as a fallback for `--store-root`.
