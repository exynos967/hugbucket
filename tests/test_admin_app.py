"""Tests for the unified admin panel routes."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from hugbucket.config import Config
from hugbucket.s3.app import create_app


class _FakePoolStatus:
    total = 1
    healthy = 1
    unhealthy = 0
    strategy = "round_robin"
    tokens = []


class _FakeTokenPool:
    async def load(self) -> None:
        return None

    @property
    def has_tokens(self) -> bool:
        return False

    def status(self) -> _FakePoolStatus:
        return _FakePoolStatus()


async def test_admin_login_cookie_unlocks_dashboard(aiohttp_client) -> None:
    """The admin login flow must not be intercepted by S3 auth."""
    backend = MagicMock()
    backend.close = AsyncMock()
    app = create_app(
        config=Config(
            admin_password="secret",
            s3_access_key="access",
            s3_secret_key="secret-key",
        ),
        backend=backend,
        token_pool=None,
    )

    client = await aiohttp_client(app)

    login_page = await client.get("/admin")
    assert login_page.status == 200
    assert "请输入管理密码" in await login_page.text()

    login = await client.post("/api/auth/login", json={"password": "secret"})
    assert login.status == 200
    assert "Set-Cookie" in login.headers

    cookie = login.headers["Set-Cookie"].split(";", 1)[0]
    dashboard = await client.get("/admin", headers={"Cookie": cookie})
    assert dashboard.status == 200
    assert "请输入管理密码" not in await dashboard.text()
    assert "HugBucket" in await dashboard.text()


async def test_admin_api_still_requires_admin_cookie(aiohttp_client) -> None:
    """Admin API routes bypass S3 auth but remain protected by admin auth."""
    backend = MagicMock()
    backend.close = AsyncMock()
    app = create_app(
        config=Config(
            admin_password="secret",
            s3_access_key="access",
            s3_secret_key="secret-key",
        ),
        backend=backend,
        token_pool=None,
    )

    client = await aiohttp_client(app)
    resp = await client.get("/api/status")
    assert resp.status == 401
    assert await resp.json() == {"error": "unauthorized"}


async def test_healthz_bypasses_admin_and_s3_auth(aiohttp_client) -> None:
    """Container health checks must not require an admin session or AWS auth."""
    backend = MagicMock()
    backend.close = AsyncMock()
    app = create_app(
        config=Config(
            admin_password="secret",
            s3_access_key="access",
            s3_secret_key="secret-key",
        ),
        backend=backend,
        token_pool=None,
    )

    client = await aiohttp_client(app)
    resp = await client.get("/healthz")

    assert resp.status == 200
    assert await resp.json() == {"ok": True}


async def test_logged_in_admin_api_bypasses_s3_auth(aiohttp_client) -> None:
    """Logged-in admin API calls should work without AWS SigV4 headers."""
    backend = MagicMock()
    backend.close = AsyncMock()
    app = create_app(
        config=Config(
            admin_password="secret",
            s3_access_key="access",
            s3_secret_key="secret-key",
        ),
        backend=backend,
        token_pool=_FakeTokenPool(),
    )

    client = await aiohttp_client(app)
    login = await client.post("/api/auth/login", json={"password": "secret"})
    cookie = login.headers["Set-Cookie"].split(";", 1)[0]

    resp = await client.get("/api/status", headers={"Cookie": cookie})
    body = await resp.json()

    assert resp.status == 200
    assert body["token_pool"]["total"] == 1
    assert body["token_pool"]["healthy"] == 1
