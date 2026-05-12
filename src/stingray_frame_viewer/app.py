"""FastAPI application factory.

Builds the app, registers the routes from :mod:`routes`, and wires the manifest
dict onto ``app.state`` via a lifespan handler. ``/health`` is gated on
manifest readiness.

Implemented in M4.
"""
from __future__ import annotations
