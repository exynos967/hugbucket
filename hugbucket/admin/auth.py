"""Admin panel authentication — HMAC-signed cookie."""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from collections.abc import Awaitable, Callable

from aiohttp import web

logger = logging.getLogger(__name__)

COOKIE_NAME = "hugbucket_admin_session"
COOKIE_MAX_AGE = 86400  # 24 hours
COOKIE_PATH = "/"


def _sign(secret: str, payload: str) -> str:
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def create_session_cookie(password: str) -> tuple[str, str]:
    """Return (cookie_value, set_cookie_header_value)."""
    expiry = int(time.time()) + COOKIE_MAX_AGE
    payload = f"{expiry}"
    sig = _sign(password, payload)
    value = f"{payload}.{sig}"
    return value, (
        f"{COOKIE_NAME}={value}; Path={COOKIE_PATH}; Max-Age={COOKIE_MAX_AGE}; "
        "HttpOnly; SameSite=Strict"
    )


def verify_session(request: web.Request, password: str) -> bool:
    """Check that the request carries a valid session cookie."""
    cookie = request.cookies.get(COOKIE_NAME)
    if not cookie:
        return False

    parts = cookie.split(".")
    if len(parts) != 2:
        return False

    payload, sig = parts
    expected = _sign(password, payload)

    if not hmac.compare_digest(expected, sig):
        return False

    try:
        expiry = int(payload)
    except ValueError:
        return False

    return expiry > time.time()


def logout_cookie() -> str:
    """Return a Set-Cookie header that clears the session."""
    return f"{COOKIE_NAME}=; Path={COOKIE_PATH}; Max-Age=0; HttpOnly; SameSite=Strict"


def admin_auth_middleware(password: str):
    """Build an aiohttp middleware that enforces admin authentication.

    Unauthenticated requests to ``/admin`` receive a redirect to the login
    page.  API calls under ``/api/`` receive a 401 JSON response.
    """

    _login_page_html: str | None = None

    @web.middleware
    async def _middleware(
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        # Only protect admin routes
        path = request.path
        is_admin = path == "/admin" or path.startswith("/admin/")
        is_api = path.startswith("/api/")

        if not is_admin and not is_api:
            return await handler(request)

        # Always allow login/logout flow
        if path in ("/admin/login", "/api/auth/login", "/api/auth/logout"):
            return await handler(request)

        # Verify session
        if verify_session(request, password):
            return await handler(request)

        # Serve login page or return 401
        if is_admin or path.startswith("/admin"):
            nonlocal _login_page_html
            if _login_page_html is None:
                from importlib.resources import files

                _login_page_html = (
                    files("hugbucket.admin")
                    .joinpath("login.html")
                    .read_text(encoding="utf-8")
                )
            return web.Response(
                text=_login_page_html,
                content_type="text/html",
                status=200,  # show login page, not 401
            )
        else:
            return web.json_response(
                {"error": "unauthorized"}, status=401
            )

    return _middleware
