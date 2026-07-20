"""FastAPI application factory.

Builds the app, registers the routes from :mod:`routes`, and wires the
manifest dict onto ``app.state`` via a lifespan handler. uvicorn's lifespan
semantics keep the listen socket from accepting connections until the
manifest is loaded, so no separate readiness gate is needed.

Run with::

    uvicorn stingray_frame_viewer.app:create_app --factory
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .cache import FrameCache
from .config import Settings
from .errors import install_handlers
from .manifest import load_manifest, open_store
from .routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    app.state.settings = settings
    store = open_store(
        settings.store_root,
        s3_endpoint=settings.store_s3_endpoint,
        s3_access_key=settings.store_s3_access_key,
        s3_secret_key=settings.store_s3_secret_key,
    )
    app.state.manifest = load_manifest(store)
    # Phase 2: build the VAST S3 cache when enabled, else leave it None so the
    # route serves purely on-the-fly (Phase 1). Single switch, per DESIGN.
    app.state.cache = (
        FrameCache.from_settings(settings) if settings.cache_enabled else None
    )
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="Stingray Frame Viewer", lifespan=lifespan)
    app.include_router(router)
    install_handlers(app)
    return app
