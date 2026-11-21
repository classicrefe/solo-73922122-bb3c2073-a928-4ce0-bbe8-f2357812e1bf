"""ASGI entrypoint for the ingestion API: ``uvicorn app.main:app``."""

from .api import create_app

app = create_app()
