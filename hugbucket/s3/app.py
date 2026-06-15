"""Unified app factory — S3 gateway + Admin panel on a single port."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from aiohttp import web

from hugbucket.bridge import HFStorageBackend
from hugbucket.config import Config
from hugbucket.s3.auth import s3_auth_middleware
from hugbucket.s3.server import S3Handler

logger = logging.getLogger(__name__)

_TPL_DIR = Path(__file__).resolve().parent.parent / "admin"


def create_app(
    *,
    config: Config,
    backend: HFStorageBackend,
    token_pool=None,
    max_upload_bytes: int = 1024 * 1024 * 1024,
) -> web.Application:
    """Create the unified HugBucket app (S3 gateway + Admin panel)."""

    handler = S3Handler(
        backend,
        multipart_upload_ttl=config.multipart_upload_ttl,
    )
    app = web.Application(
        client_max_size=max_upload_bytes,
        middlewares=[s3_auth_middleware],
    )
    app["config"] = config
    app["bridge"] = backend
    if token_pool is not None:
        app["token_pool"] = token_pool

    # ── Route registration (order = priority) ───────────────────────────
    # 1. Admin API routes (explicit paths, highest priority)
    _register_admin_routes(app)

    # 2. Admin dashboard at /admin
    app.router.add_get("/admin", _dashboard_handler)

    # 3. S3 routes (GET / for ListBuckets + catch-all)
    handler.setup_routes(app)

    # 4. Multipart upload lifecycle
    app.on_startup.append(handler._start_cleanup)
    app.on_shutdown.append(handler._stop_cleanup)

    # ── Lifecycle ───────────────────────────────────────────────────────
    async def on_startup(app: web.Application) -> None:
        pool = app.get("token_pool")
        if pool is not None:
            await pool.load()
            if pool.has_tokens:
                resolved = 0
                for entry in list(pool._config.tokens):
                    if entry.healthy and not entry.namespace:
                        try:
                            await pool.resolve_namespace(entry, backend.hub.whoami)
                            resolved += 1
                        except Exception as e:
                            logger.warning(
                                "Failed to resolve namespace for token %s: %s",
                                entry.label or entry.token[:10],
                                e,
                            )
                if resolved:
                    logger.info("Resolved %d token namespace(s)", resolved)
                for t in pool._config.tokens:
                    if t.healthy and t.namespace:
                        config.hf_namespace = t.namespace
                        break

        if not config.hf_namespace:
            try:
                config.hf_namespace = await backend.resolve_namespace()
                logger.info("  Resolved HF namespace: %s", config.hf_namespace)
            except Exception as exc:
                logger.error("Failed to resolve HF namespace: %s", exc)
                sys.exit(1)

        logger.info("=" * 60)
        logger.info("  HugBucket: http://%s:%s", config.host, config.port)
        logger.info("  HF endpoint: %s", config.hf_endpoint)
        logger.info("  HF namespace: %s", config.hf_namespace)
        logger.info("")
        logger.info("  S3 CLI:   aws --endpoint-url http://localhost:%s s3 ls", config.port)
        logger.info("  Admin UI: http://localhost:%s/admin", config.port)
        logger.info("=" * 60)

    async def on_shutdown(app: web.Application) -> None:
        await backend.close()

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app


# ── Route helpers ───────────────────────────────────────────────────────

_DASHBOARD_HTML = (_TPL_DIR / "dashboard.html").read_text(encoding="utf-8")


async def _dashboard_handler(_request: web.Request) -> web.Response:
    return web.Response(text=_DASHBOARD_HTML, content_type="text/html; charset=utf-8")


def _register_admin_routes(app: web.Application) -> None:
    from hugbucket.admin.app import (
        handle_status,
        handle_list_tokens,
        handle_add_token,
        handle_remove_token,
        handle_resolve_token,
        handle_list_buckets,
        handle_bucket_detail,
    )

    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/tokens", handle_list_tokens)
    app.router.add_post("/api/tokens", handle_add_token)
    app.router.add_delete("/api/tokens/{index}", handle_remove_token)
    app.router.add_post("/api/tokens/{index}/resolve", handle_resolve_token)
    app.router.add_get("/api/buckets", handle_list_buckets)
    app.router.add_get("/api/buckets/{namespace}/{name}", handle_bucket_detail)
