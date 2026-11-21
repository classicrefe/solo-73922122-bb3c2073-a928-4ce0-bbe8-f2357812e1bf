"""ASGI entrypoint for the local chain source: ``uvicorn chain_source.main:app``."""

from .app import create_app

app = create_app()
