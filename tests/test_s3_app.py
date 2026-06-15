"""Tests for S3 app entrypoint startup wiring."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

from hugbucket.apps import s3 as s3_app


def test_s3_main_starts_without_token(monkeypatch) -> None:
    """App should start without any pre-configured tokens (config via admin panel)."""
    monkeypatch.setattr(sys, "argv", ["hugbucket"])

    # Prevent actual server startup
    monkeypatch.setattr(s3_app.web, "run_app", lambda *a, **kw: None)

    s3_app.main()


def test_s3_main_starts(monkeypatch) -> None:
    seen = {}

    monkeypatch.setattr(sys, "argv", ["hugbucket"])

    backend = MagicMock()
    monkeypatch.setattr(
        s3_app,
        "HFStorageBackend",
        lambda config, _token_pool=None: backend,
    )

    class _FakeApp(dict):
        def __init__(self):
            super().__init__()
            self.on_startup = [lambda app: None]
            self.on_shutdown = []

    def _create_app(*, config, backend, token_pool, max_upload_bytes):
        seen["config"] = config
        seen["backend"] = backend
        seen["max_upload_bytes"] = max_upload_bytes
        app = _FakeApp()
        app["config"] = config
        return app

    monkeypatch.setattr(s3_app, "create_app", _create_app)
    monkeypatch.setattr(s3_app.web, "run_app", lambda *a, **kw: None)

    async def _resolve():
        return "testuser"

    backend.resolve_namespace = _resolve

    s3_app.main()

    assert seen["backend"] is backend
    assert seen["max_upload_bytes"] == 1024 * 1024 * 1024
    assert seen["config"].host == "0.0.0.0"
    assert seen["config"].port == 9000
