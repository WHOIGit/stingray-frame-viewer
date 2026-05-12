"""pydantic-settings configuration.

Reads ``STINGRAY_*`` environment variables (see ``.env.example``): manifest
store root, manifest S3 credentials, cache toggle and credentials, default
frame format, JPEG quality, and neighbors-endpoint window.

Implemented in M4.
"""
from __future__ import annotations
