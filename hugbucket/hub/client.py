"""Async HTTP client for HF Hub Bucket API endpoints."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import quote

import aiohttp

from hugbucket.config import Config
from hugbucket.core.models import BucketFile, BucketInfo

logger = logging.getLogger(__name__)

# Batch sizes matching huggingface_hub
BATCH_ADD_CHUNK_SIZE = 100
BATCH_DELETE_CHUNK_SIZE = 1000
PATHS_INFO_BATCH_SIZE = 1000


@dataclass
class XetConnectionInfo:
    cas_url: str
    access_token: str
    token_expiration: int  # unix epoch


@dataclass
class HubClient:
    """Async client for HF Hub Bucket API.

    Supports dynamic token switching via *token_getter* — a callable that
    returns the HF token string to use for the next request.  The companion
    *token_releaser* callable is invoked after each request to track
    in-flight counts and detect 429 rate-limiting.

    When *token_getter* is None, no Authorization header is sent.
    """

    config: Config
    _session: aiohttp.ClientSession | None = field(default=None, repr=False)
    _token_getter: Callable[[], str] | None = field(default=None, repr=False)
    _token_releaser: Callable[[bool], None] | None = field(default=None, repr=False)
    _token_override: str | None = field(default=None, repr=False)

    def _get_token(self) -> str:
        if self._token_getter is not None:
            token = self._token_getter()
            if token:
                return token
        return ""

    def release_token(self, exc: BaseException | None = None) -> None:
        """Notify the token pool that this request is done.

        Call after every HF API request (success or failure).
        If *exc* is a 429 ``ClientResponseError`` the token is marked
        rate-limited.
        """
        if self._token_releaser is None:
            return
        rate_limited = False
        if exc is not None:
            # aiohttp may not be importable at type-check time
            rate_limited = (
                hasattr(exc, "status") and getattr(exc, "status", 0) == 429
            )
        self._token_releaser(rate_limited)

    def _base_headers(self) -> dict[str, str]:
        return {"User-Agent": "hugbucket/0.1.0"}

    def _auth_headers(self, token: str | None = None) -> dict[str, str]:
        if token is not None:
            return {"Authorization": f"Bearer {token}"}
        if self._token_override is not None:
            return {"Authorization": f"Bearer {self._token_override}"}
        token = self._get_token()
        if token:
            return {"Authorization": f"Bearer {token}"}
        return {}

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.config.http_pool_size,
                enable_cleanup_closed=True,
            )
            # Authorization is passed per-request so token_getter can
            # return different tokens for each request.
            self._session = aiohttp.ClientSession(
                connector=connector,
                headers=self._base_headers(),
                raise_for_status=False,
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def _api_url(self, path: str) -> str:
        return f"{self.config.hf_endpoint}{path}"

    # ---- Auth / identity ----

    async def whoami(self, *, token: str | None = None) -> str:
        """Get username associated with the HF token via /api/whoami-v2."""
        session = await self._ensure_session()
        url = self._api_url("/api/whoami-v2")
        headers = {"Authorization": f"Bearer {token}"} if token else self._auth_headers()
        async with session.get(url, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data["name"]

    # ---- Bucket CRUD ----

    async def create_bucket(
        self,
        name: str,
        *,
        private: bool = False,
        exist_ok: bool = True,
        namespace: str | None = None,
        token: str | None = None,
    ) -> str:
        """Create a bucket. Returns the bucket URL."""
        session = await self._ensure_session()
        ns = namespace or self.config.hf_namespace
        url = self._api_url(f"/api/buckets/{ns}/{name}")
        body: dict = {}
        if private:
            body["private"] = True

        async with session.post(
            url,
            json=body,
            headers=self._auth_headers(token),
        ) as resp:
            if resp.status == 409 and exist_ok:
                return f"{self.config.hf_endpoint}/buckets/{ns}/{name}"
            resp.raise_for_status()
            data = await resp.json()
            return data.get("url", "")

    async def get_bucket_info(
        self, bucket_id: str, *, token: str | None = None
    ) -> BucketInfo:
        """Get bucket info. bucket_id = 'namespace/name'."""
        session = await self._ensure_session()
        url = self._api_url(f"/api/buckets/{bucket_id}")
        headers = (
            {"Authorization": f"Bearer {token}"}
            if token
            else self._auth_headers()
        )
        async with session.get(url, headers=headers) as resp:
            resp.raise_for_status()
            d = await resp.json()
            return BucketInfo(
                id=d["id"],
                private=d["private"],
                created_at=d.get("createdAt", ""),
                size=d.get("size", 0),
                total_files=d.get("totalFiles", 0),
            )

    async def list_buckets(
        self, namespace: str | None = None, *, token: str | None = None
    ) -> list[BucketInfo]:
        """List all buckets for the namespace."""
        session = await self._ensure_session()
        ns = namespace or self.config.hf_namespace
        url = self._api_url(f"/api/buckets/{ns}")
        buckets: list[BucketInfo] = []
        headers = (
            {"Authorization": f"Bearer {token}"}
            if token
            else self._auth_headers()
        )

        while url:
            async with session.get(url, headers=headers) as resp:
                resp.raise_for_status()
                items = await resp.json()
                for d in items:
                    buckets.append(
                        BucketInfo(
                            id=d["id"],
                            private=d["private"],
                            created_at=d.get("createdAt", ""),
                            size=d.get("size", 0),
                            total_files=d.get("totalFiles", 0),
                        )
                    )
                url = self._next_link(resp)

        return buckets

    async def delete_bucket(self, bucket_id: str, *, missing_ok: bool = True) -> None:
        """Delete a bucket."""
        session = await self._ensure_session()
        url = self._api_url(f"/api/buckets/{bucket_id}")
        async with session.delete(url, headers=self._auth_headers()) as resp:
            if resp.status == 404 and missing_ok:
                return
            resp.raise_for_status()

    # ---- File listing ----

    async def list_bucket_tree(
        self,
        bucket_id: str,
        prefix: str = "",
        recursive: bool = False,
        *,
        token: str | None = None,
    ) -> list[BucketFile]:
        """List files/dirs in a bucket."""
        session = await self._ensure_session()

        path = f"/api/buckets/{bucket_id}/tree"
        if prefix:
            path += f"/{quote(prefix, safe='')}"

        url = self._api_url(path)
        params = {}
        if recursive:
            params["recursive"] = "true"

        req_headers = (
            {"Authorization": f"Bearer {token}"}
            if token
            else self._auth_headers()
        )

        files: list[BucketFile] = []
        while url:
            async with session.get(url, params=params, headers=req_headers) as resp:
                resp.raise_for_status()
                items = await resp.json()
                for d in items:
                    files.append(
                        BucketFile(
                            type=d["type"],
                            path=d["path"],
                            size=d.get("size", 0),
                            xet_hash=d.get("xetHash", ""),
                            mtime=d.get("mtime", ""),
                            uploaded_at=d.get("uploadedAt", ""),
                        )
                    )
                url = self._next_link(resp)
                params = {}  # params already in the next URL

        return files

    async def get_paths_info(
        self,
        bucket_id: str,
        paths: list[str],
        *,
        token: str | None = None,
    ) -> list[BucketFile]:
        """Batch get file info for specific paths."""
        session = await self._ensure_session()
        url = self._api_url(f"/api/buckets/{bucket_id}/paths-info")
        all_files: list[BucketFile] = []

        for i in range(0, len(paths), PATHS_INFO_BATCH_SIZE):
            batch = paths[i : i + PATHS_INFO_BATCH_SIZE]
            async with session.post(
                url,
                json={"paths": batch},
                headers=self._auth_headers(token),
            ) as resp:
                resp.raise_for_status()
                items = await resp.json()
                for d in items:
                    all_files.append(
                        BucketFile(
                            type=d["type"],
                            path=d["path"],
                            size=d.get("size", 0),
                            xet_hash=d.get("xetHash", ""),
                            mtime=d.get("mtime", ""),
                            uploaded_at=d.get("uploadedAt", ""),
                        )
                    )

        return all_files

    # ---- Batch add/delete ----

    async def batch_files(
        self,
        bucket_id: str,
        add: list[dict] | None = None,
        delete: list[str] | None = None,
        token: str | None = None,
    ) -> None:
        """Batch add/delete files via NDJSON.

        add: list of {"path": ..., "xetHash": ..., "mtime": epoch_ms, "contentType": ...}
        delete: list of paths to delete
        """
        session = await self._ensure_session()
        url = self._api_url(f"/api/buckets/{bucket_id}/batch")

        # Process adds in chunks of 100
        if add:
            for i in range(0, len(add), BATCH_ADD_CHUNK_SIZE):
                batch = add[i : i + BATCH_ADD_CHUNK_SIZE]
                await self._send_ndjson_batch(session, url, batch, [], token=token)

        # Process deletes in chunks of 1000
        if delete:
            for i in range(0, len(delete), BATCH_DELETE_CHUNK_SIZE):
                batch = delete[i : i + BATCH_DELETE_CHUNK_SIZE]
                await self._send_ndjson_batch(session, url, [], batch, token=token)

    async def _send_ndjson_batch(
        self,
        session: aiohttp.ClientSession,
        url: str,
        adds: list[dict],
        deletes: list[str],
        token: str | None = None,
    ) -> None:
        import json

        lines: list[str] = []
        for a in adds:
            lines.append(
                json.dumps(
                    {
                        "type": "addFile",
                        "path": a["path"],
                        "xetHash": a["xetHash"],
                        "mtime": a["mtime"],
                        "contentType": a.get("contentType", "application/octet-stream"),
                    }
                )
            )
        for d in deletes:
            lines.append(json.dumps({"type": "deleteFile", "path": d}))

        body = "\n".join(lines)
        req_headers = {
            "Content-Type": "application/x-ndjson",
            **self._auth_headers(token),
        }
        async with session.post(url, data=body.encode(), headers=req_headers) as resp:
            if resp.status >= 400:
                error_body = await resp.text()
                logger.error(f"Batch API error {resp.status}: {error_body}")
            resp.raise_for_status()

    # ---- Xet tokens ----

    async def get_xet_write_token(
        self,
        bucket_id: str,
        *,
        token: str | None = None,
    ) -> XetConnectionInfo:
        """Get Xet CAS write credentials."""
        return await self._get_xet_token(bucket_id, "write", token=token)

    async def get_xet_read_token(
        self,
        bucket_id: str,
        *,
        token: str | None = None,
    ) -> XetConnectionInfo:
        """Get Xet CAS read credentials."""
        return await self._get_xet_token(bucket_id, "read", token=token)

    async def _get_xet_token(
        self,
        bucket_id: str,
        token_type: str,
        *,
        token: str | None = None,
    ) -> XetConnectionInfo:
        session = await self._ensure_session()
        url = self._api_url(f"/api/buckets/{bucket_id}/xet-{token_type}-token")
        async with session.get(url, headers=self._auth_headers(token)) as resp:
            resp.raise_for_status()
            try:
                return XetConnectionInfo(
                    cas_url=resp.headers["X-Xet-Cas-Url"],
                    access_token=resp.headers["X-Xet-Access-Token"],
                    token_expiration=int(resp.headers["X-Xet-Token-Expiration"]),
                )
            except KeyError as e:
                raise aiohttp.ClientResponseError(
                    resp.request_info, resp.history, status=502,
                    message=f"Missing Xet header: {e}", headers=resp.headers,
                )

    # ---- File metadata (HEAD) ----

    async def head_file(self, bucket_id: str, path: str) -> BucketFile | None:
        """Get single file metadata via HEAD request."""
        session = await self._ensure_session()
        encoded = quote(path, safe="")
        url = self._api_url(f"/buckets/{bucket_id}/resolve/{encoded}")
        async with session.head(url, allow_redirects=False, headers=self._auth_headers()) as resp:
            if resp.status == 404:
                return None
            # Follow relative redirects only
            if resp.status in (301, 302, 307, 308):
                location = resp.headers.get("Location", "")
                if location.startswith("/"):
                    return await self._head_follow(session, location)
            resp.raise_for_status()
            return BucketFile(
                type="file",
                path=path,
                size=int(resp.headers.get("Content-Length", 0)),
                xet_hash=resp.headers.get("X-Xet-Hash", ""),
            )

    async def _head_follow(
        self, session: aiohttp.ClientSession, path: str
    ) -> BucketFile | None:
        url = self._api_url(path)
        async with session.head(url, allow_redirects=False, headers=self._auth_headers()) as resp:
            if resp.status == 404:
                return None
            resp.raise_for_status()
            return BucketFile(
                type="file",
                path=path.split("/resolve/")[-1] if "/resolve/" in path else path,
                size=int(resp.headers.get("Content-Length", 0)),
                xet_hash=resp.headers.get("X-Xet-Hash", ""),
            )

    # ---- Raw request helper (for operations not yet wrapped) ----------------

    async def _send_raw_request(
        self, method: str, path: str, *, json: dict | None = None
    ) -> dict:
        """Send an arbitrary request to the HF API and return JSON body."""
        session = await self._ensure_session()
        url = self._api_url(path)
        headers = {"Content-Type": "application/json", **self._auth_headers()}
        async with session.request(method, url, json=json, headers=headers) as resp:
            resp.raise_for_status()
            return await resp.json()

    # ---- Helpers ----

    @staticmethod
    def _next_link(resp: aiohttp.ClientResponse) -> str | None:
        """Parse GitHub-style Link header for pagination."""
        link = resp.headers.get("Link", "")
        if not link:
            return None
        for part in link.split(","):
            part = part.strip()
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
                return url
        return None
