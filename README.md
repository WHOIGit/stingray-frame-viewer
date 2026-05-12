# Stingray Frame Viewer

Web service that serves individual frames of Stingray (towed shadowgraph imager) videos to browsers. Sits in front of source AVIs, encodes frames on demand, and (Phase 2+) caches encoded frames in VAST S3.

Design overview: [DESIGN.md](DESIGN.md).

## Status

Foundations only: project scaffold, the AVI inspection script, the manifest models, and the CSV ingest CLI. Frame extraction, encoding, the HTTP routes, and the S3 cache are described in [DESIGN.md](DESIGN.md) §3 and are not yet implemented.

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
