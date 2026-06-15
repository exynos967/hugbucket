"""Tests for S3 app entrypoint startup wiring."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

from hugbucket.apps import s3 as s3_app


def test_s3_main_starts_without_token(monkeypatch) -> None:
    """App should start even without HF_TOKEN (user can add via admin panel)."""
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.setattr(s3_app.sys, "argv", ["hugbucket"])

    # Prevent actual server startup by mocking asyncio.run
    monkeypatch.setattr(asyncio, "run", lambda coro: None)

    s3_app.main()


def test_s3_main_starts(monkeypatch) -> None:
    seen = {}

    monkeypatch.setenv("HF_TOKEN", "hf_test")
    monkeypatch.setattr(s3_app.sys, "argv", ["hugbucket"])

    backend = MagicMock()
    monkeypatch.setattr(
        s3_app,
        "HFStorageBackend",
        lambda config, _token_pool=None: backend,
    )

    class _FakeApp(dict):
        def __init__(self):
            super().__init__()
            self.on_startup = [lambda app: None]  # aiohttp Signal is list-like
            self.on_shutdown = []

    def _create_s3_app(*, config, backend, max_upload_bytes):
        seen["config"] = config
        seen["backend"] = backend
        seen["max_upload_bytes"] = max_upload_bytes
        app = _FakeApp()
        app["config"] = config
        return app

    monkeypatch.setattr(s3_app, "create_s3_app", _create_s3_app)

    # Prevent actual server startup
    monkeypatch.setattr(asyncio, "run", lambda coro: None)

    # Prevent resolve_namespace from making real API calls
    async def _resolve():
        return "testuser"

    backend.resolve_namespace = _resolve

    s3_app.main()

    assert seen["backend"] is backend
    assert seen["max_upload_bytes"] == 1024 * 1024 * 1024
    assert seen["config"].host == "0.0.0.0"
    assert seen["config"].port == 9000
