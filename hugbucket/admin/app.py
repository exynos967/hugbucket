"""Admin web panel for HugBucket — token management + bucket usage dashboard."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from aiohttp import web

logger = logging.getLogger(__name__)

_TPL_DIR = Path(__file__).resolve().parent


def create_admin_app(
    bridge,
    token_pool,
    config,
) -> web.Application:
    """Create the admin management aiohttp application."""

    app = web.Application()
    app["bridge"] = bridge
    app["token_pool"] = token_pool
    app["config"] = config

    # -- API routes ----------------------------------------------------------
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/tokens", handle_list_tokens)
    app.router.add_post("/api/tokens", handle_add_token)
    app.router.add_delete("/api/tokens/{index}", handle_remove_token)
    app.router.add_post("/api/tokens/{index}/resolve", handle_resolve_token)
    app.router.add_get("/api/buckets", handle_list_buckets)
    app.router.add_get("/api/buckets/{namespace}/{name}", handle_bucket_detail)

    # -- Static UI -----------------------------------------------------------
    # Serve the SPA dashboard at /
    async def index(_request: web.Request) -> web.Response:
        html = (_TPL_DIR / "dashboard.html").read_text(encoding="utf-8")
        return web.Response(text=html, content_type="text/html; charset=utf-8")

    app.router.add_get("/", index)

    logger.info("Admin panel routes registered")
    return app


# -- helpers ----------------------------------------------------------------


def _json(data, status: int = 200) -> web.Response:
    return web.json_response(data, status=status, dumps=lambda o: json.dumps(o, ensure_ascii=False))


def _error(message: str, status: int = 400) -> web.Response:
    return _json({"error": message}, status=status)


# -- handlers ----------------------------------------------------------------


async def handle_status(request: web.Request) -> web.Response:
    """GET /api/status — overall system status."""
    pool = request.app["token_pool"]
    config = request.app["config"]

    pool_status = pool.status()
    return _json(
        {
            "server": {
                "s3_port": config.port,
                "admin_port": pool.admin_port,
                "hf_endpoint": config.hf_endpoint,
            },
            "token_pool": {
                "total": pool_status.total,
                "healthy": pool_status.healthy,
                "unhealthy": pool_status.unhealthy,
                "strategy": pool_status.strategy,
                "tokens": pool_status.tokens,
            },
        }
    )


async def handle_list_tokens(request: web.Request) -> web.Response:
    """GET /api/tokens — list all configured tokens."""
    pool = request.app["token_pool"]
    return _json(pool.status())


async def handle_add_token(request: web.Request) -> web.Response:
    """POST /api/tokens — add a new HF token.

    Body: {"token": "hf_xxx", "label": "my-token"}
    """
    pool = request.app["token_pool"]
    bridge = request.app["bridge"]
    config = request.app["config"]

    try:
        body = await request.json()
    except Exception:
        return _error("请求体不是合法的 JSON")

    token = (body.get("token") or "").strip()
    label = (body.get("label") or "").strip()

    if not token:
        return _error("Token 不能为空")
    if not token.startswith("hf_"):
        return _error("无效的 HF Token — 必须以 hf_ 开头")

    try:
        entry = await pool.add_token(token, label)
    except ValueError as e:
        return _error(str(e), status=409)

    # Resolve namespace asynchronously in the background
    async def _resolve():
        try:
            ns = await pool.resolve_namespace(entry, bridge.hub.whoami)
            logger.info("Resolved namespace for new token: %s -> %s", label, ns)
        except Exception as e:
            logger.warning("Failed to resolve namespace for new token: %s", e)

    import asyncio
    asyncio.create_task(_resolve())

    return _json(
        {
            "ok": True,
            "index": len(pool._config.tokens) - 1,
            "label": entry.label,
            "token_preview": _mask(entry.token),
        },
        status=201,
    )


async def handle_remove_token(request: web.Request) -> web.Response:
    """DELETE /api/tokens/{index} — remove a token."""
    pool = request.app["token_pool"]

    try:
        index = int(request.match_info["index"])
    except ValueError:
        return _error("无效的索引")

    try:
        await pool.remove_token(index)
    except IndexError:
        return _error("Token 索引不存在", status=404)

    return _json({"ok": True})


async def handle_resolve_token(request: web.Request) -> web.Response:
    """POST /api/tokens/{index}/resolve — re-resolve namespace."""
    pool = request.app["token_pool"]
    bridge = request.app["bridge"]

    try:
        index = int(request.match_info["index"])
    except ValueError:
        return _error("无效的索引")

    await pool.load()
    if index < 0 or index >= len(pool._config.tokens):
        return _error("Token 索引不存在", status=404)

    entry = pool._config.tokens[index]
    try:
        ns = await pool.resolve_namespace(entry, bridge.hub.whoami)
        return _json({"ok": True, "namespace": ns})
    except Exception as e:
        return _error(f"解析命名空间失败: {e}", status=502)


async def handle_list_buckets(request: web.Request) -> web.Response:
    """GET /api/buckets — list all buckets across all namespaces."""
    bridge = request.app["bridge"]
    pool = request.app["token_pool"]
    config = request.app["config"]

    all_buckets: list[dict] = []

    # Collect across all healthy token namespaces
    namespaces = pool.all_namespaces

    if not namespaces:
        # No token pool configured — try single-token mode
        if config.hf_token:
            try:
                ns = config.hf_namespace or await bridge.hub.whoami(token=config.hf_token)
                buckets = await bridge.hub.list_buckets(ns, token=config.hf_token)
                for b in buckets:
                    all_buckets.append(
                        {
                            "id": b.id,
                            "name": b.id.split("/")[-1] if "/" in b.id else b.id,
                            "namespace": ns,
                            "private": b.private,
                            "created_at": b.created_at,
                            "size": b.size,
                            "total_files": b.total_files,
                        }
                    )
            except Exception as e:
                logger.warning("Failed to list buckets for single token: %s", e)
    else:
        for ns in namespaces:
            try:
                entry = await pool.get_token_for_namespace(ns)
                if entry is None:
                    continue
                buckets = await bridge.hub.list_buckets(ns, token=entry.token)
                for b in buckets:
                    all_buckets.append(
                        {
                            "id": b.id,
                            "name": b.id.split("/")[-1] if "/" in b.id else b.id,
                            "namespace": ns,
                            "private": b.private,
                            "created_at": b.created_at,
                            "size": b.size,
                            "total_files": b.total_files,
                        }
                    )
            except Exception as e:
                logger.warning("Failed to list buckets for namespace %s: %s", ns, e)

    # Sort by size descending
    all_buckets.sort(key=lambda b: b["size"], reverse=True)

    return _json(
        {
            "buckets": all_buckets,
            "total": len(all_buckets),
            "total_size": sum(b["size"] for b in all_buckets),
            "total_files": sum(b["total_files"] for b in all_buckets),
        }
    )


async def handle_bucket_detail(request: web.Request) -> web.Response:
    """GET /api/buckets/{namespace}/{name} — get detailed bucket info."""
    bridge = request.app["bridge"]
    pool = request.app["token_pool"]

    namespace = request.match_info["namespace"]
    name = request.match_info["name"]
    bucket_id = f"{namespace}/{name}"

    # Find the right token for this namespace
    entry = await pool.get_token_for_namespace(namespace)
    if entry is None:
        return _error(f"命名空间 {namespace} 没有可用的 Token", status=404)

    try:
        info = await bridge.hub.get_bucket_info(bucket_id, token=entry.token)
        return _json(
            {
                "id": info.id,
                "name": name,
                "namespace": namespace,
                "private": info.private,
                "created_at": info.created_at,
                "size": info.size,
                "total_files": info.total_files,
            }
        )
    except Exception as e:
        return _error(str(e), status=502)


def _mask(token: str) -> str:
    if len(token) <= 8:
        return token[:2] + "****"
    return token[:4] + "****" + token[-4:]
