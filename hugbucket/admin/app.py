"""Admin API handlers — auth, token management, bucket usage dashboard."""

from __future__ import annotations

import json
import logging

from aiohttp import web

from hugbucket.admin.auth import create_session_cookie, logout_cookie

logger = logging.getLogger(__name__)


# -- helpers ----------------------------------------------------------------


def _json(data, status: int = 200) -> web.Response:
    return web.json_response(data, status=status, dumps=lambda o: json.dumps(o, ensure_ascii=False))


def _error(message: str, status: int = 400) -> web.Response:
    return _json({"error": message}, status=status)


# -- auth handlers ---------------------------------------------------------


async def handle_login(request: web.Request) -> web.Response:
    """POST /api/auth/login — validate password, set session cookie."""
    config = request.app["config"]
    password = config.admin_password

    if not password:
        return _error("管理员密码未配置", status=500)

    try:
        body = await request.json()
    except Exception:
        return _error("请求体不是合法的 JSON")

    provided = (body.get("password") or "").strip()

    if provided != password:
        return _error("密码错误", status=401)

    secure = request.url.scheme == "https"
    _, cookie = create_session_cookie(password, secure=secure)
    return web.json_response({"ok": True}, headers={"Set-Cookie": cookie})


async def handle_logout(request: web.Request) -> web.Response:
    """POST /api/auth/logout — clear session cookie."""
    return web.json_response({"ok": True}, headers={"Set-Cookie": logout_cookie()})


# -- handlers ----------------------------------------------------------------


async def handle_status(request: web.Request) -> web.Response:
    """GET /api/status — overall system status."""
    pool = request.app["token_pool"]
    cfg = request.app["config"]

    pool_status = pool.status()
    return _json(
        {
            "server": {
                "port": cfg.port,
                "hf_endpoint": cfg.hf_endpoint,
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
    s = pool.status()
    return _json({"total": s.total, "healthy": s.healthy, "unhealthy": s.unhealthy, "strategy": s.strategy, "tokens": s.tokens})


async def handle_add_token(request: web.Request) -> web.Response:
    """POST /api/tokens — add a new HF token.

    Body: {"token": "hf_xxx", "label": "my-token"}
    """
    pool = request.app["token_pool"]
    bridge = request.app["bridge"]

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
    """GET /api/buckets — list all buckets (unified across namespaces)."""
    bridge = request.app["bridge"]

    buckets = await bridge.list_buckets()
    all_buckets = [
        {
            "id": b.id,
            "name": b.id.split("/")[-1] if "/" in b.id else b.id,
            "namespace": b.id.split("/")[0] if "/" in b.id else "",
            "private": b.private,
            "created_at": b.created_at,
            "size": b.size,
            "total_files": b.total_files,
        }
        for b in buckets
    ]
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

    namespace = request.match_info["namespace"]
    name = request.match_info["name"]

    info = await bridge.head_bucket(name)
    if info is None or info.id != f"{namespace}/{name}":
        return _error("存储桶不存在", status=404)

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


# -- bucket creation handlers -----------------------------------------------


async def handle_create_bucket(request: web.Request) -> web.Response:
    """POST /api/buckets/create — create a bucket under a specific token."""
    bridge = request.app["bridge"]
    pool = request.app["token_pool"]

    try:
        body = await request.json()
    except Exception:
        return _error("请求体不是合法的 JSON")

    name = (body.get("name") or "").strip()
    private = bool(body.get("private", False))
    token_index = body.get("token_index")

    if not name:
        return _error("桶名不能为空")

    # Use specific token if specified, otherwise pool acquire
    await pool.load()
    if token_index is not None:
        try:
            token_index = int(token_index)
        except ValueError:
            return _error("无效的 token_index")
        if token_index < 0 or token_index >= len(pool._config.tokens):
            return _error("Token 索引不存在", status=404)
        entry = pool._config.tokens[token_index]
        if not entry.healthy:
            return _error("所选 Token 不健康", status=400)
        if not entry.namespace:
            return _error("所选 Token 尚未解析 namespace", status=400)
        config = request.app["config"]
        config.hf_namespace = entry.namespace
        try:
            bridge.config.hf_namespace = entry.namespace
            url = await bridge.hub.create_bucket(name)
            bridge._bucket_ns_cache[name] = entry.namespace
            return _json({"ok": True, "url": url, "namespace": entry.namespace}, status=201)
        except Exception as e:
            return _error(f"创建桶失败: {e}", status=502)
    else:
        if not pool.has_tokens:
            return _error("没有可用的 Token — 请先在 Token 管理页面添加", status=503)
        url = await bridge.create_bucket(name, private=private)
        return _json({"ok": True, "url": url}, status=201)


async def handle_ensure_buckets(request: web.Request) -> web.Response:
    """POST /api/buckets/ensure — 确保每个 Token 的 namespace 下至少有一个桶。

    遍历所有健康 Token，检查其 namespace 是否已有桶。
    没有桶的 namespace → 创建一个随机 8 位桶名。
    """
    import secrets
    import string

    bridge = request.app["bridge"]
    pool = request.app["token_pool"]

    try:
        body = await request.json()
    except Exception:
        return _error("请求体不是合法的 JSON")

    private = bool(body.get("private", False))
    await pool.load()

    if not pool.has_tokens:
        return _error("没有可用的 Token", status=503)

    alphabet = string.ascii_lowercase + string.digits
    result: list[dict] = []

    for entry in pool._config.tokens:
        if not entry.healthy or not entry.namespace:
            continue

        # Check if this namespace already has buckets
        try:
            existing = await bridge.hub.list_buckets(entry.namespace, token=entry.token)
        except Exception:
            result.append({"namespace": entry.namespace, "label": entry.label, "status": "error", "reason": "list failed"})
            continue

        if existing:
            result.append({"namespace": entry.namespace, "label": entry.label, "status": "skipped", "existing_buckets": len(existing)})
            continue

        # No buckets — create one
        random_name = "".join(secrets.choice(alphabet) for _ in range(8))
        try:
            bridge.config.hf_namespace = entry.namespace
            await bridge.hub.create_bucket(random_name, private=private)
            bridge._bucket_ns_cache[random_name] = entry.namespace
            result.append({"namespace": entry.namespace, "label": entry.label, "status": "created", "bucket": random_name})
        except Exception as e:
            result.append({"namespace": entry.namespace, "label": entry.label, "status": "error", "reason": str(e)})

    created = sum(1 for r in result if r["status"] == "created")
    skipped = sum(1 for r in result if r["status"] == "skipped")
    return _json({"ok": True, "created": created, "skipped": skipped, "details": result})


# -- bucket mutation handlers -----------------------------------------------


async def handle_delete_bucket(request: web.Request) -> web.Response:
    """DELETE /api/buckets/{namespace}/{name} — delete a bucket."""
    bridge = request.app["bridge"]
    namespace = request.match_info["namespace"]
    name = request.match_info["name"]

    try:
        await bridge.hub.delete_bucket(f"{namespace}/{name}")
        bridge._bucket_ns_cache.pop(name, None)
        return _json({"ok": True})
    except Exception as e:
        return _error(f"删除失败: {e}", status=502)


async def handle_rename_bucket(request: web.Request) -> web.Response:
    """POST /api/buckets/rename — rename a bucket {old_name, new_name, namespace}."""
    bridge = request.app["bridge"]

    try:
        body = await request.json()
    except Exception:
        return _error("请求体不是合法的 JSON")

    old_name = (body.get("old_name") or "").strip()
    new_name = (body.get("new_name") or "").strip()
    namespace = (body.get("namespace") or "").strip()

    if not old_name or not new_name:
        return _error("old_name 和 new_name 不能为空")

    # Resolve namespace if not provided
    if not namespace:
        namespace = bridge._bucket_ns_cache.get(old_name)
    if not namespace:
        return _error("无法确定桶的 namespace", status=400)

    bucket_id = f"{namespace}/{old_name}"
    new_bucket_id = f"{namespace}/{new_name}"

    try:
        await bridge.hub._send_raw_request(
            "POST", "/api/repos/move",
            json={"fromRepo": bucket_id, "toRepo": new_bucket_id, "type": "bucket"},
        )
        bridge._bucket_ns_cache.pop(old_name, None)
        bridge._bucket_ns_cache[new_name] = namespace
        return _json({"ok": True, "namespace": namespace})
    except Exception as e:
        return _error(f"重命名失败: {e}", status=502)


# -- token edit handler ------------------------------------------------------


async def handle_edit_token(request: web.Request) -> web.Response:
    """PUT /api/tokens/{index} — edit token label."""
    pool = request.app["token_pool"]

    try:
        index = int(request.match_info["index"])
    except ValueError:
        return _error("无效的索引")

    try:
        body = await request.json()
    except Exception:
        return _error("请求体不是合法的 JSON")

    label = body.get("label")
    if label is not None:
        label = label.strip()

    try:
        entry = await pool.update_token(index, label=label)
        return _json({"ok": True, "label": entry.label})
    except IndexError:
        return _error("Token 索引不存在", status=404)


def _mask(token: str) -> str:
    if len(token) <= 8:
        return token[:2] + "****"
    return token[:4] + "****" + token[-4:]
