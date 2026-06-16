"""Bridge: orchestrates S3 operations via HF Hub + Xet CAS.

This is the core translation layer that maps S3 operations
(put object, get object) to the multi-step HF/Xet protocol.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections import OrderedDict
from collections.abc import AsyncIterator
import logging
import mimetypes
import time
from dataclasses import dataclass, field, replace

from hugbucket.config import Config
from hugbucket.hub.client import HubClient, BucketInfo, BucketFile, XetConnectionInfo
from hugbucket.xet.cas_client import CASClient, Reconstruction, ReconstructionTerm
from hugbucket.xet.chunker import chunk_data
from hugbucket.xet.hasher import (
    chunk_hash,
    file_hash,
    hash_to_hex,
    verification_hash,
    xorb_hash,
)
from hugbucket.xet.xorb import (
    serialize_xorb,
    deserialize_xorb,
    ChunkEntry,
    XORB_MAX_BYTES,
)
from hugbucket.xet.shard import (
    FileInfo,
    FileDataTerm,
    XorbInfo,
    CASChunkInfo,
    build_shard,
)

logger = logging.getLogger(__name__)

# Max chunks per xorb (approx, to stay within 64 MiB serialized)
MAX_CHUNKS_PER_XORB = 1024

# Hidden placeholder file stored inside "empty" directories so they appear
# in listings.  S3 clients create folders by PUTting a zero-byte object with
# a trailing slash; HF Storage Buckets use virtual directories (inferred from
# file paths), so we materialise the folder by storing this tiny sentinel.
DIR_MARKER_FILENAME = ".hugbucket_keep"
DIR_MARKER_CONTENT = b"\n"  # must be non-empty so the full Xet upload runs


@dataclass
class _XorbBatch:
    """Pre-computed xorb ready for upload."""

    xorb_bytes: bytes
    xorb_hash_hex: str
    xorb_info: XorbInfo
    file_term: FileDataTerm
    verification_hash: bytes


@dataclass
class _PreparedUpload:
    """All CPU-bound results needed to complete an upload."""

    file_hash_hex: str
    xorb_batches: list[_XorbBatch]
    file_info: FileInfo
    etag: str


def _prepare_upload(data: bytes) -> _PreparedUpload:
    """CPU-bound upload preparation (chunking, hashing, compression).

    This runs in a thread to avoid blocking the async event loop.
    """
    # Step 1: CDC chunk
    chunks = chunk_data(data)
    logger.info(f"prepare_upload: {len(data)} bytes -> {len(chunks)} chunks")

    # Step 2: Hash all chunks
    c_hashes: list[bytes] = []
    c_sizes: list[int] = []
    for c in chunks:
        c_hashes.append(chunk_hash(c.data))
        c_sizes.append(len(c.data))

    # Step 3: Compute file hash
    f_hash = file_hash(c_hashes, c_sizes)
    f_hash_hex = hash_to_hex(f_hash)

    # Step 4: Group chunks into xorbs, serialize each
    xorb_batches: list[_XorbBatch] = []
    file_terms: list[FileDataTerm] = []
    term_verification_hashes: list[bytes] = []

    xorb_chunks: list[bytes] = []
    xorb_c_hashes: list[bytes] = []
    xorb_c_sizes: list[int] = []
    chunk_start_in_xorb = 0

    def _flush_xorb() -> None:
        nonlocal xorb_chunks, xorb_c_hashes, xorb_c_sizes
        nonlocal chunk_start_in_xorb

        if not xorb_chunks:
            return

        # Serialize (LZ4 compression)
        xorb_bytes, xorb_offsets = serialize_xorb(xorb_chunks)
        x_hash = xorb_hash(xorb_c_hashes, xorb_c_sizes)
        x_hash_hex = hash_to_hex(x_hash)

        # Build CAS info using cumulative uncompressed byte offsets
        cas_chunks: list[CASChunkInfo] = []
        uncompressed_offset = 0
        for i, (ch, cs) in enumerate(zip(xorb_c_hashes, xorb_c_sizes)):
            cas_chunks.append(
                CASChunkInfo(
                    chunk_hash=ch,
                    byte_range_start=uncompressed_offset,
                    unpacked_bytes=cs,
                )
            )
            uncompressed_offset += cs

        xi = XorbInfo(
            xorb_hash=x_hash,
            cas_flags=0,
            chunks=cas_chunks,
            total_bytes_in_xorb=sum(xorb_c_sizes),
            total_bytes_on_disk=len(xorb_bytes),
        )

        ft = FileDataTerm(
            xorb_hash=x_hash,
            cas_flags=0,
            unpacked_bytes=sum(xorb_c_sizes),
            chunk_start=chunk_start_in_xorb,
            chunk_end=chunk_start_in_xorb + len(xorb_chunks),
        )

        v_hash = verification_hash(xorb_c_hashes)

        xorb_batches.append(
            _XorbBatch(
                xorb_bytes=xorb_bytes,
                xorb_hash_hex=x_hash_hex,
                xorb_info=xi,
                file_term=ft,
                verification_hash=v_hash,
            )
        )

        file_terms.append(ft)
        term_verification_hashes.append(v_hash)

        # Reset
        xorb_chunks = []
        xorb_c_hashes = []
        xorb_c_sizes = []
        chunk_start_in_xorb = 0

    for i, c in enumerate(chunks):
        xorb_chunks.append(c.data)
        xorb_c_hashes.append(c_hashes[i])
        xorb_c_sizes.append(c_sizes[i])

        if (
            len(xorb_chunks) >= MAX_CHUNKS_PER_XORB
            or sum(len(d) for d in xorb_chunks) >= XORB_MAX_BYTES // 2
        ):
            _flush_xorb()
            chunk_start_in_xorb = 0

    _flush_xorb()

    # Step 5: Build shard
    fi = FileInfo(
        file_hash=f_hash,
        terms=file_terms,
        verification_hashes=term_verification_hashes,
    )
    # NOTE: shard_bytes is built later after uploads, but since build_shard
    # is also CPU-bound, we do it here too.

    # Step 6: MD5 for ETag
    etag = hashlib.md5(data).hexdigest()

    return _PreparedUpload(
        file_hash_hex=f_hash_hex,
        xorb_batches=xorb_batches,
        file_info=fi,
        etag=etag,
    )


@dataclass
class _XorbCacheEntry:
    """Cached decompressed xorb chunks."""

    chunks: list[ChunkEntry]
    size: int  # total uncompressed bytes


class _XorbCache:
    """LRU cache for decompressed xorb data, bounded by total memory."""

    def __init__(self, max_bytes: int) -> None:
        self._cache: OrderedDict[str, _XorbCacheEntry] = OrderedDict()
        self._total: int = 0
        self._max: int = max_bytes

    def get(self, key: str) -> list[ChunkEntry] | None:
        entry = self._cache.get(key)
        if entry is not None:
            self._cache.move_to_end(key)
            return entry.chunks
        return None

    def put(self, key: str, chunks: list[ChunkEntry]) -> None:
        if key in self._cache:
            return
        size = sum(len(c.uncompressed_data) for c in chunks)
        if size > self._max:
            return  # single xorb larger than entire cache
        while self._total + size > self._max and self._cache:
            _, evicted = self._cache.popitem(last=False)
            self._total -= evicted.size
        self._cache[key] = _XorbCacheEntry(chunks=chunks, size=size)
        self._total += size


@dataclass
class HFStorageBackend:
    """Orchestrates S3 <-> HF Bucket operations.

    When *token_pool* is provided, each outbound HF API request picks
    the next healthy token (round-robin), spreading load across all
    configured credentials.
    """

    config: Config
    hub: HubClient = field(init=False)
    cas: CASClient = field(init=False)
    _token_pool: object | None = field(default=None, repr=False)  # TokenPool (circular import)
    _token_cache: dict[str, XetConnectionInfo] = field(
        default_factory=dict, init=False, repr=False
    )
    _recon_cache: OrderedDict[str, tuple[float, Reconstruction]] = field(
        default_factory=OrderedDict, init=False, repr=False
    )
    _xorb_cache: _XorbCache = field(init=False, repr=False)
    _file_info_cache: OrderedDict[str, tuple[float, BucketFile]] = field(
        default_factory=OrderedDict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        token_getter = None
        token_releaser = None
        if self._token_pool is not None:
            token_getter = self._make_token_getter()
            token_releaser = self._make_token_releaser()

        self.hub = HubClient(
            config=self.config,
            _token_getter=token_getter,
            _token_releaser=token_releaser,
        )
        self.cas = CASClient(
            pool_size=self.config.http_pool_size,
            upload_timeout=self.config.cas_upload_timeout,
            max_retries=self.config.cas_upload_retries,
            retry_base_delay=self.config.cas_retry_base_delay,
        )
        self._xorb_cache = _XorbCache(max_bytes=self.config.xorb_cache_max_bytes)

    def _make_token_getter(self):
        """Least-in-flight acquire: pick healthiest, least-busy token."""
        pool = self._token_pool
        _current = [""]  # mutable cell shared with releaser

        def _get() -> str:
            try:
                entry = pool.acquire_sync()
                if entry is not None:
                    _current[0] = entry.token
                    return entry.token
            except Exception:
                logger.warning("Token pool acquire failed", exc_info=True)
            _current[0] = ""
            return ""

        self.__current_token_cell = _current
        return _get

    def _make_token_releaser(self):
        """Release token back to pool with optional 429 marking."""
        pool = self._token_pool
        _current = getattr(self, "__current_token_cell", [""])

        def _release(rate_limited: bool) -> None:
            token = _current[0]
            if not token:
                return
            try:
                pool.release_sync(token, rate_limited=rate_limited)
            except Exception:
                logger.warning("Token pool release failed", exc_info=True)

        return _release

    async def close(self) -> None:
        await self.hub.close()
        await self.cas.close()

    async def resolve_namespace(self) -> str:
        """Resolve namespace from the token pool."""
        if self._token_pool is not None:
            from hugbucket.admin.token_pool import TokenPool

            pool: TokenPool = self._token_pool
            await pool.load()
            if pool.has_tokens:
                entry = await pool.get_next()
                if entry is not None:
                    return await pool.resolve_namespace(entry, self.hub.whoami)
        return ""

    @property
    def token_pool(self):
        return self._token_pool

    # ---- Virtual pool bucket (single-bucket mode) ---------------------------

    # key → real_bucket_name cache for the virtual pool bucket
    _pool_file_cache: dict[str, str] = field(default_factory=dict, init=False, repr=False)

    # Cache of _pool_all_buckets result (short TTL for freshness)
    _pool_buckets_cache: tuple[float, list[dict]] = field(
        default_factory=lambda: (0, []), init=False, repr=False
    )

    # Full pool file listing cache — all files across all buckets, pre-prefixed.
    # pool_list_objects filters this in-memory for sub-millisecond responses.
    _pool_listing_cache: tuple[float, list[BucketFile]] = field(
        default_factory=lambda: (0, []), init=False, repr=False
    )
    _pool_listing_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    async def _pool_all_buckets(self) -> list[dict]:
        """Return [(name, namespace, token), ...] for all real buckets.

        Cached for 30 seconds to avoid hitting HF API on every list_objects.
        """
        now = time.monotonic()
        if self._pool_buckets_cache[1] and (now - self._pool_buckets_cache[0]) < 30:
            return self._pool_buckets_cache[1]

        result: list[dict] = []
        if self._token_pool is None:
            return result
        pool = self._token_pool
        await pool.load()
        for ns in pool.all_namespaces:
            entry = await pool.get_token_for_namespace(ns)
            if entry is None:
                continue
            try:
                buckets = await self.hub.list_buckets(ns, token=entry.token)
                for b in buckets:
                    name = b.id.split("/")[-1] if "/" in b.id else b.id
                    self._bucket_ns_cache[name] = ns
                    result.append({"name": name, "namespace": ns, "token": entry.token})
            except Exception:
                logger.warning("Failed to list buckets for ns %s", ns, exc_info=True)

        self._pool_buckets_cache = (now, result)
        return result

    def _invalidate_pool_cache(self) -> None:
        """Clear pool bucket cache (call after creating/deleting a bucket)."""
        self._pool_buckets_cache = (0, [])
        self._invalidate_pool_listing_cache()

    def _invalidate_pool_listing_cache(self) -> None:
        """Clear cached pool file listing after object mutations."""
        self._pool_listing_cache = (0, [])

    async def _refresh_pool_listing(self) -> None:
        """Fetch FULL file listing from all buckets and cache it.

        Must be called with _pool_listing_lock held or when no concurrent
        callers exist (e.g. startup).
        """
        all_buckets = await self._pool_all_buckets()

        async def _fetch_one(b: dict) -> list[tuple[str, BucketFile]]:
            result: list[tuple[str, BucketFile]] = []
            bucket_id = f"{b['namespace']}/{b['name']}"
            try:
                files = await self.hub.list_bucket_tree(
                    bucket_id, recursive=True, token=b["token"]
                )
                for f in files:
                    if f.type != "file":
                        continue
                    result.append((b["name"], replace(f)))
            except Exception:
                logger.warning("Failed to list %s", bucket_id, exc_info=True)
            return result

        tasks = [_fetch_one(b) for b in all_buckets]
        results = await asyncio.gather(*tasks)
        all_contents: list[BucketFile] = []
        seen_keys: set[str] = set()
        for r in results:
            for bucket_name, f in r:
                if f.path in seen_keys:
                    logger.warning(
                        "Duplicate pool key %s found in bucket %s; keeping first",
                        f.path,
                        bucket_name,
                    )
                    continue
                seen_keys.add(f.path)
                self._pool_file_cache[f.path] = bucket_name
                all_contents.append(f)

        all_contents.sort(key=lambda f: f.path)
        self._pool_listing_cache = (time.monotonic(), all_contents)
        logger.info(
            "Pool listing refreshed: %d files across %d buckets",
            len(all_contents), len(all_buckets),
        )

    async def pool_list_objects(self, prefix: str = "", delimiter: str = "",
                                 max_keys: int = 1000,
                                 continuation_token: str = "") -> dict:
        """Aggregate objects from all real buckets, keyed by bucket name.

        Returns from an in-memory cache of the full file listing.
        Triggers a background refresh when the cache is stale (>60 s).
        """
        now = time.monotonic()
        cache_ts = self._pool_listing_cache[0]
        cache_initialized = cache_ts > 0
        cache_age = now - cache_ts if cache_initialized else float("inf")
        need_refresh = cache_age > 60

        if not cache_initialized:
            # Cold cache — must do a blocking fetch (first request).
            # A warmed cache can legitimately contain zero files.
            async with self._pool_listing_lock:
                if self._pool_listing_cache[0] <= 0:
                    await self._refresh_pool_listing()
                    need_refresh = False
        elif need_refresh and not self._pool_listing_lock.locked():
            # Stale cache, no refresh in progress — trigger background refresh
            asyncio.ensure_future(self._bg_refresh_listing())

        all_contents = self._pool_listing_cache[1]

        # Apply S3-style prefix/delimiter filtering (pure in-memory)
        filtered = [f for f in all_contents if f.path.startswith(prefix)]
        contents: list[BucketFile] = []
        common_prefixes: set[str] = set()

        if delimiter:
            for f in filtered:
                rest = f.path[len(prefix):]
                delim_pos = rest.find(delimiter)
                if delim_pos >= 0:
                    common_prefixes.add(prefix + rest[:delim_pos + len(delimiter)])
                else:
                    contents.append(f)
        else:
            contents = filtered

        sorted_prefixes = sorted(common_prefixes)

        # Pagination
        start_idx = 0
        if continuation_token:
            for i, c in enumerate(contents):
                if c.path > continuation_token:
                    start_idx = i
                    break

        truncated = len(contents) > start_idx + max_keys
        page = contents[start_idx:start_idx + max_keys]
        next_token = page[-1].path if truncated and page else None

        return {
            "contents": page,
            "common_prefixes": sorted_prefixes,
            "is_truncated": truncated,
            "next_continuation_token": next_token,
        }

    async def _bg_refresh_listing(self) -> None:
        """Background refresh with lock guard."""
        async with self._pool_listing_lock:
            # Re-check staleness under lock — another refresh may have finished
            if (time.monotonic() - self._pool_listing_cache[0]) <= 60:
                return
            try:
                await self._refresh_pool_listing()
            except Exception:
                logger.warning("Background pool listing refresh failed", exc_info=True)

    async def pool_put_object(self, key: str, data: bytes) -> dict:
        """Write to the least-in-flight real bucket, recording mapping."""
        all_buckets = await self._pool_all_buckets()
        if not all_buckets:
            raise RuntimeError("No buckets available in pool")

        # Pick least-in-flight — acquire a token
        if self._token_pool is not None:
            entry = await self._token_pool.acquire()
            if entry is None:
                raise RuntimeError("No healthy token available")
            try:
                ns = entry.namespace or await self._token_pool.resolve_namespace(entry, self.hub.whoami)
                # Find or create a bucket for this token
                my_buckets = [b for b in all_buckets if b["namespace"] == ns]
                if my_buckets:
                    real_bucket = my_buckets[0]["name"]
                else:
                    import secrets
                    import string

                    real_bucket = "".join(
                        secrets.choice(string.ascii_lowercase + string.digits)
                        for _ in range(8)
                    )
                    await self.hub.create_bucket(
                        real_bucket,
                        namespace=ns,
                        token=entry.token,
                    )
                    self._bucket_ns_cache[real_bucket] = ns
                    self._invalidate_pool_cache()

                bucket_id = f"{ns}/{real_bucket}"
                result = await self.put_object(
                    real_bucket,
                    key,
                    data,
                    bucket_id=bucket_id,
                    token=entry.token,
                )
                self._bucket_ns_cache[real_bucket] = ns
                self._pool_file_cache[key] = real_bucket
                self._invalidate_pool_listing_cache()
                return result
            finally:
                await self._token_pool.release(entry.token)

        # Fallback without pool
        first = all_buckets[0]
        bucket_id = f"{first['namespace']}/{first['name']}"
        result = await self.put_object(
            first["name"],
            key,
            data,
            bucket_id=bucket_id,
            token=first.get("token"),
        )
        self._bucket_ns_cache[first["name"]] = first["namespace"]
        self._pool_file_cache[key] = first["name"]
        self._invalidate_pool_listing_cache()
        return result

    async def _pool_find_bucket(self, key: str) -> str | None:
        """Find which real bucket contains *key*. Returns bucket name or None."""
        if key in self._pool_file_cache:
            return self._pool_file_cache[key]

        all_buckets = await self._pool_all_buckets()
        for b in all_buckets:
            bucket_id = f"{b['namespace']}/{b['name']}"
            try:
                info = await self.hub.get_paths_info(
                    bucket_id,
                    [key],
                    token=b.get("token"),
                )
                if info and info[0].type == "file":
                    self._pool_file_cache[key] = b["name"]
                    self._bucket_ns_cache[b["name"]] = b["namespace"]
                    return b["name"]
            except Exception:
                continue
        return None

    async def _pool_route_for_key(self, key: str) -> tuple[str, str, str | None] | None:
        """Return (bucket_name, bucket_id, token) for a virtual pool key."""
        bucket_name = await self._pool_find_bucket(key)
        if bucket_name is None:
            return None

        namespace = self._bucket_ns_cache.get(bucket_name)
        if namespace is None:
            namespace = await self._resolve_bucket_ns(bucket_name)
        if namespace is None:
            return None

        token = None
        if self._token_pool is not None:
            entry = await self._token_pool.get_token_for_namespace(namespace)
            if entry is not None:
                token = entry.token

        return bucket_name, f"{namespace}/{bucket_name}", token

    async def pool_get_object(self, key: str) -> bytes | None:
        """Get object from pool — probes real buckets until found."""
        route = await self._pool_route_for_key(key)
        if route is None:
            return None
        bucket_name, bucket_id, token = route
        return await self.get_object(bucket_name, key, bucket_id=bucket_id, token=token)

    async def pool_get_object_stream(self, key: str, file_info=None,
                                      byte_range=None):
        """Stream object from pool."""
        route = await self._pool_route_for_key(key)
        if route is None:
            return None
        bucket_name, bucket_id, token = route
        return await self.get_object_stream(
            bucket_name,
            key,
            file_info=file_info,
            byte_range=byte_range,
            bucket_id=bucket_id,
            token=token,
        )

    async def pool_head_object(self, key: str) -> BucketFile | None:
        """Head object in pool."""
        route = await self._pool_route_for_key(key)
        if route is None:
            return None
        bucket_name, bucket_id, token = route
        return await self.head_object(bucket_name, key, bucket_id=bucket_id, token=token)

    async def pool_head_directory(self, prefix: str) -> bool:
        """Check if a virtual pool directory exists."""
        marker = await self.pool_head_object(prefix + DIR_MARKER_FILENAME)
        if marker is not None:
            return True

        result = await self.pool_list_objects(prefix=prefix, max_keys=1)
        return bool(result["contents"] or result["common_prefixes"])

    async def pool_delete_object(self, key: str) -> None:
        """Delete object from pool."""
        route = await self._pool_route_for_key(key)
        if route is None:
            raise FileNotFoundError(key)
        bucket_name, bucket_id, token = route
        await self.delete_object(bucket_name, key, bucket_id=bucket_id, token=token)
        self._pool_file_cache.pop(key, None)
        self._invalidate_pool_listing_cache()

    async def pool_delete_objects(self, keys: list[str]) -> tuple[list[str], list[dict]]:
        """Delete multiple virtual pool objects."""
        deleted: list[str] = []
        errors: list[dict] = []
        for key in keys:
            try:
                await self.pool_delete_object(key)
                deleted.append(key)
            except FileNotFoundError:
                deleted.append(key)
            except Exception as exc:
                logger.exception("pool_delete_objects failed for %s", key)
                errors.append(
                    {"key": key, "code": "InternalError", "message": str(exc)}
                )
        return deleted, errors

    async def pool_copy_object(self, src_key: str, dst_key: str) -> dict:
        """Copy a virtual pool object within the same underlying real bucket."""
        route = await self._pool_route_for_key(src_key)
        if route is None:
            raise FileNotFoundError(src_key)
        bucket_name, bucket_id, token = route
        result = await self.copy_object(
            bucket_name,
            src_key,
            bucket_name,
            dst_key,
            src_bucket_id=bucket_id,
            dst_bucket_id=bucket_id,
            token=token,
        )
        self._pool_file_cache[dst_key] = bucket_name
        self._invalidate_pool_listing_cache()
        return result

    # ---- Unified bucket pool (all namespaces merged) ------------------------

    # bucket_name → namespace cache (populated by list_buckets, lazy fallback)
    _bucket_ns_cache: dict[str, str] = field(default_factory=dict, init=False, repr=False)

    async def _resolve_bucket_ns(self, bucket_name: str) -> str | None:
        """Find which namespace owns *bucket_name*.

        Checks all healthy tokens' namespaces.  Caches the result so
        subsequent lookups are O(1).
        """
        if bucket_name in self._bucket_ns_cache:
            return self._bucket_ns_cache[bucket_name]

        if self._token_pool is None:
            return self.config.hf_namespace or None

        pool = self._token_pool
        await pool.load()
        for ns in pool.all_namespaces:
            try:
                entry = await pool.get_token_for_namespace(ns)
                if entry is None:
                    continue
                info = await self.hub.get_bucket_info(
                    f"{ns}/{bucket_name}", token=entry.token
                )
                if info is not None:
                    self._bucket_ns_cache[bucket_name] = ns
                    return ns
            except Exception:
                continue
        return None

    def _bucket_id(self, bucket_name: str) -> str:
        """Convert S3 bucket name to HF bucket_id (namespace/name).

        Uses cached namespace mapping populated by ``list_buckets`` or
        ``_resolve_bucket_ns``.  Falls back to configured namespace.
        """
        if "/" in bucket_name:
            return bucket_name
        ns = self._bucket_ns_cache.get(bucket_name)
        if ns:
            return f"{ns}/{bucket_name}"
        return f"{self.config.hf_namespace}/{bucket_name}"

    async def _get_token_for_bucket(self, bucket_name: str) -> str | None:
        """Return the token string for the namespace owning *bucket_name*."""
        ns = self._bucket_ns_cache.get(bucket_name)
        if ns and self._token_pool is not None:
            entry = await self._token_pool.get_token_for_namespace(ns)
            if entry:
                return entry.token
        return None

    # ---- Cached helpers ----

    async def _get_read_token(
        self,
        bucket_id: str,
        *,
        token: str | None = None,
    ) -> XetConnectionInfo:
        """Return a cached read token, refreshing when close to expiry."""
        cached = self._token_cache.get(bucket_id)
        if cached and cached.token_expiration > time.time() + 60:
            return cached
        token_kwargs = {"token": token} if token is not None else {}
        conn = await self.hub.get_xet_read_token(bucket_id, **token_kwargs)
        self._token_cache[bucket_id] = conn
        return conn

    async def _get_reconstruction(
        self, conn: XetConnectionInfo, file_hash: str
    ) -> Reconstruction:
        """Return a cached reconstruction plan, fetching if stale/missing."""
        cached = self._recon_cache.get(file_hash)
        if cached:
            ts, recon = cached
            if time.time() - ts < self.config.recon_cache_ttl:
                self._recon_cache.move_to_end(file_hash)
                return recon
            del self._recon_cache[file_hash]
        recon = await self.cas.get_reconstruction(conn, file_hash)
        while len(self._recon_cache) >= self.config.recon_cache_max_entries:
            self._recon_cache.popitem(last=False)
        self._recon_cache[file_hash] = (time.time(), recon)
        return recon

    async def _get_file_info_cached(
        self,
        bucket_id: str,
        key: str,
        *,
        token: str | None = None,
    ) -> BucketFile | None:
        """Return cached file metadata, fetching from Hub if stale/missing."""
        cache_key = f"{bucket_id}:{key}"
        cached = self._file_info_cache.get(cache_key)
        if cached is not None:
            ts, file_info = cached
            if time.time() - ts < self.config.file_info_cache_ttl:
                self._file_info_cache.move_to_end(cache_key)
                return file_info
            del self._file_info_cache[cache_key]
        token_kwargs = {"token": token} if token is not None else {}
        files = await self.hub.get_paths_info(bucket_id, [key], **token_kwargs)
        if not files:
            return None
        file_info = files[0]
        while len(self._file_info_cache) >= self.config.file_info_cache_max_entries:
            self._file_info_cache.popitem(last=False)
        self._file_info_cache[cache_key] = (time.time(), file_info)
        return file_info

    def _invalidate_file_info(self, bucket_id: str, key: str) -> None:
        """Remove a file_info entry from the cache after a mutation."""
        cache_key = f"{bucket_id}:{key}"
        self._file_info_cache.pop(cache_key, None)

    async def _fetch_xorb_chunks(
        self, term: ReconstructionTerm, recon: Reconstruction
    ) -> tuple[list[ChunkEntry], int] | None:
        """Fetch and decompress xorb chunks, using cache. Returns (chunks, fetch_range_start)."""
        fetches = recon.fetch_info.get(term.hash, [])
        for fetch in fetches:
            if fetch.range_start > term.range_end or fetch.range_end < term.range_start:
                continue
            cache_key = f"{term.hash}:{fetch.range_start}:{fetch.range_end}"
            chunks = self._xorb_cache.get(cache_key)
            if chunks is None:
                xorb_bytes = await self.cas.fetch_xorb_range(fetch)
                chunks = await asyncio.to_thread(deserialize_xorb, xorb_bytes)
                self._xorb_cache.put(cache_key, chunks)
            return chunks, fetch.range_start
        return None

    # ---- Bucket operations ----

    async def list_buckets(self) -> list[BucketInfo]:
        """List buckets from all namespaces (unified pool)."""
        if self._token_pool is None:
            return await self.hub.list_buckets()

        pool = self._token_pool
        await pool.load()
        all_buckets: list[BucketInfo] = []
        for ns in pool.all_namespaces:
            entry = await pool.get_token_for_namespace(ns)
            if entry is None:
                continue
            try:
                buckets = await self.hub.list_buckets(ns, token=entry.token)
                for b in buckets:
                    name = b.id.split("/")[-1] if "/" in b.id else b.id
                    self._bucket_ns_cache[name] = ns
                all_buckets.extend(buckets)
            except Exception:
                logger.warning("Failed to list buckets for namespace %s", ns, exc_info=True)
        return all_buckets

    async def create_bucket(self, name: str, private: bool = False) -> str:
        """Create a bucket in the least-busy token's namespace."""
        if self._token_pool is not None:
            pool = self._token_pool
            await pool.load()
            entry = await pool.acquire()
            if entry and entry.namespace:
                try:
                    url = await self.hub.create_bucket(
                        name,
                        private=private,
                        namespace=entry.namespace,
                        token=entry.token,
                    )
                    self._bucket_ns_cache[name] = entry.namespace
                    return url
                finally:
                    await pool.release(entry.token)
        return await self.hub.create_bucket(name, private=private)

    async def delete_bucket(self, name: str) -> None:
        await self.hub.delete_bucket(self._bucket_id(name))
        self._bucket_ns_cache.pop(name, None)

    async def head_bucket(self, name: str) -> BucketInfo | None:
        ns = self._bucket_ns_cache.get(name)
        if ns and self._token_pool is not None:
            entry = await self._token_pool.get_token_for_namespace(ns)
            if entry:
                try:
                    return await self.hub.get_bucket_info(
                        f"{ns}/{name}", token=entry.token
                    )
                except Exception:
                    pass
        # Fallback: probe config namespace
        try:
            return await self.hub.get_bucket_info(self._bucket_id(name))
        except Exception:
            return None

    # ---- Object operations ----

    async def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        bucket_id: str | None = None,
        token: str | None = None,
    ) -> dict:
        """Upload an object. Full Xet protocol:
        1. CDC chunk the data
        2. Hash all chunks
        3. Build xorbs from chunks
        4. Upload xorbs to CAS
        5. Build + upload shard
        6. Register file via Hub batch API

        CPU-bound work (chunking, hashing, compression, MD5) is offloaded
        to a thread so the event loop stays responsive during uploads.
        """
        bucket_id = bucket_id or self._bucket_id(bucket)
        requested_size = len(data)

        # S3 clients create "folders" by PUTting a zero-byte object with a
        # trailing slash (e.g. "my-folder/").  HF Storage Buckets use virtual
        # directories — they are inferred from file paths, not created
        # explicitly — so the batch API rejects addFile for such paths (422).
        # Store a hidden placeholder file inside the directory so it shows up
        # in listings.  The content must be non-empty because the batch API
        # rejects files whose xetHash has not been uploaded to Xet CAS, and
        # _put_empty_file skips the CAS upload step.
        if key.endswith("/") and len(data) == 0:
            logger.info(f"PUT {key}: directory marker -> {key}{DIR_MARKER_FILENAME}")
            key = key + DIR_MARKER_FILENAME
            data = DIR_MARKER_CONTENT
            # Fall through to the normal upload path below

        # Handle empty files
        if len(data) == 0:
            return await self._put_empty_file(bucket_id, key, token=token)

        # Run all CPU-bound work in a thread (chunking, hashing,
        # LZ4 compression, shard building, MD5)
        prepared = await asyncio.to_thread(_prepare_upload, data)
        logger.info(
            f"PUT {key}: {len(data)} bytes -> {len(prepared.xorb_batches)} xorb(s)"
        )

        # Get write token (network I/O, stays on event loop)
        token_kwargs = {"token": token} if token is not None else {}
        conn = await self.hub.get_xet_write_token(bucket_id, **token_kwargs)

        # Upload xorbs to CAS concurrently (network I/O)
        await asyncio.gather(
            *(
                self.cas.upload_xorb(conn, batch.xorb_hash_hex, batch.xorb_bytes)
                for batch in prepared.xorb_batches
            )
        )

        # Build shard (CPU-bound, offload to thread)
        xorb_infos = [b.xorb_info for b in prepared.xorb_batches]
        shard_bytes = await asyncio.to_thread(
            build_shard, [prepared.file_info], xorb_infos
        )
        await self.cas.upload_shard(conn, shard_bytes)

        # Register file with Hub (network I/O)
        content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
        mtime_ms = int(time.time() * 1000)

        await self.hub.batch_files(
            bucket_id,
            add=[
                {
                    "path": key,
                    "xetHash": prepared.file_hash_hex,
                    "mtime": mtime_ms,
                    "contentType": content_type,
                }
            ],
            **token_kwargs,
        )

        self._invalidate_file_info(bucket_id, key)
        return {"ETag": f'"{prepared.etag}"', "size": requested_size}

    async def _put_empty_file(
        self,
        bucket_id: str,
        key: str,
        *,
        token: str | None = None,
    ) -> dict:
        """Handle zero-byte file (no Xet upload needed)."""
        # Empty file still needs a file hash
        c_hash = chunk_hash(b"")
        f_hash = file_hash([c_hash], [0])
        f_hash_hex = hash_to_hex(f_hash)

        content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
        mtime_ms = int(time.time() * 1000)

        token_kwargs = {"token": token} if token is not None else {}
        await self.hub.batch_files(
            bucket_id,
            add=[
                {
                    "path": key,
                    "xetHash": f_hash_hex,
                    "mtime": mtime_ms,
                    "contentType": content_type,
                }
            ],
            **token_kwargs,
        )
        self._invalidate_file_info(bucket_id, key)
        etag = hashlib.md5(b"").hexdigest()
        return {"ETag": f'"{etag}"', "size": 0}

    async def get_object(
        self,
        bucket: str,
        key: str,
        *,
        bucket_id: str | None = None,
        token: str | None = None,
    ) -> bytes | None:
        """Download an object. Full Xet protocol:
        1. Get file metadata (xetHash, size)
        2. Get read token
        3. Get reconstruction from CAS
        4. Fetch xorb ranges from CDN
        5. Decompress + reassemble
        """
        bucket_id = bucket_id or self._bucket_id(bucket)
        token_kwargs = {"token": token} if token is not None else {}

        # Step 1: Get file info
        files = await self.hub.get_paths_info(bucket_id, [key], **token_kwargs)
        if not files:
            return None

        file_info = files[0]
        if file_info.size == 0:
            return b""

        # Step 2: Get read token
        conn = await self._get_read_token(bucket_id, **token_kwargs)

        # Step 3: Get reconstruction
        recon = await self._get_reconstruction(conn, file_info.xet_hash)

        # Step 4+5: Fetch and reassemble
        result_parts: list[bytes] = []
        first_term = True

        for term in recon.terms:
            result = await self._fetch_xorb_chunks(term, recon)
            if result is None:
                continue
            xorb_chunks, fetch_range_start = result

            for ci in range(term.range_start, term.range_end):
                local_idx = ci - fetch_range_start
                if 0 <= local_idx < len(xorb_chunks):
                    chunk_bytes = xorb_chunks[local_idx].uncompressed_data

                    if first_term and recon.offset_into_first_range > 0:
                        chunk_bytes = chunk_bytes[recon.offset_into_first_range :]
                        first_term = False
                    else:
                        first_term = False

                    result_parts.append(chunk_bytes)

        return b"".join(result_parts)

    async def get_object_stream(
        self,
        bucket: str,
        key: str,
        file_info: BucketFile | None = None,
        byte_range: tuple[int, int] | None = None,
        *,
        bucket_id: str | None = None,
        token: str | None = None,
    ) -> AsyncIterator[bytes] | None:
        """Stream an object chunk by chunk instead of buffering the entire file.

        Returns an async iterator that yields decompressed chunks, or None
        if the object does not exist.  When *file_info* is supplied the
        initial ``get_paths_info`` round-trip is skipped.

        When *byte_range* ``(start, end)`` is given (inclusive on both
        ends), only the bytes in that window are yielded — terms whose
        data falls entirely outside the range are never fetched from
        the CDN, making random-access seeks O(relevant terms) instead
        of O(all terms).
        """
        bucket_id = bucket_id or self._bucket_id(bucket)
        token_kwargs = {"token": token} if token is not None else {}

        if file_info is None:
            files = await self.hub.get_paths_info(bucket_id, [key], **token_kwargs)
            if not files:
                return None
            file_info = files[0]

        if file_info.size == 0:

            async def _empty() -> AsyncIterator[bytes]:
                yield b""

            return _empty()

        conn = await self._get_read_token(bucket_id, **token_kwargs)
        recon = await self._get_reconstruction(conn, file_info.xet_hash)

        # Pre-compute cumulative byte boundaries per term so we can
        # skip terms that fall outside the requested byte_range.
        term_bounds: list[tuple[int, int]] = []  # (start_byte, end_byte) inclusive
        cum = 0
        for i, term in enumerate(recon.terms):
            usable = term.unpacked_length
            if i == 0 and recon.offset_into_first_range > 0:
                usable -= recon.offset_into_first_range
            term_bounds.append((cum, cum + usable - 1))
            cum += usable

        async def _stream() -> AsyncIterator[bytes]:
            for i, term in enumerate(recon.terms):
                t_start, t_end = term_bounds[i]

                # ── range-aware term skipping ──
                if byte_range is not None:
                    req_start, req_end = byte_range
                    if t_end < req_start:
                        continue  # entire term before range
                    if t_start > req_end:
                        break  # past the range — done

                result = await self._fetch_xorb_chunks(term, recon)
                if result is None:
                    continue
                xorb_chunks, fetch_range_start = result

                # Track byte position within the file for each chunk
                chunk_file_pos = t_start

                for ci in range(term.range_start, term.range_end):
                    local_idx = ci - fetch_range_start
                    if not (0 <= local_idx < len(xorb_chunks)):
                        continue

                    chunk_bytes = xorb_chunks[local_idx].uncompressed_data

                    # Trim leading offset for the very first chunk of the file
                    if (
                        i == 0
                        and ci == term.range_start
                        and recon.offset_into_first_range > 0
                    ):
                        chunk_bytes = chunk_bytes[recon.offset_into_first_range :]

                    chunk_start = chunk_file_pos
                    chunk_end = chunk_file_pos + len(chunk_bytes) - 1
                    chunk_file_pos += len(chunk_bytes)

                    if byte_range is not None:
                        req_start, req_end = byte_range
                        # Skip chunks before the range
                        if chunk_end < req_start:
                            continue
                        # Stop after the range
                        if chunk_start > req_end:
                            return
                        # Trim first/last chunk to the exact byte window
                        left = max(0, req_start - chunk_start)
                        right = min(len(chunk_bytes), req_end - chunk_start + 1)
                        chunk_bytes = chunk_bytes[left:right]

                    if chunk_bytes:
                        yield chunk_bytes

        return _stream()

    async def delete_object(
        self,
        bucket: str,
        key: str,
        *,
        bucket_id: str | None = None,
        token: str | None = None,
    ) -> None:
        """Delete an object."""
        bucket_id = bucket_id or self._bucket_id(bucket)
        token_kwargs = {"token": token} if token is not None else {}
        keys_to_delete = [key]
        # Directory marker PUTs store a hidden placeholder; delete it too.
        if key.endswith("/"):
            keys_to_delete.append(key + DIR_MARKER_FILENAME)
        await self.hub.batch_files(bucket_id, delete=keys_to_delete, **token_kwargs)
        self._invalidate_file_info(bucket_id, key)

    async def delete_objects(
        self,
        bucket: str,
        keys: list[str],
        *,
        bucket_id: str | None = None,
        token: str | None = None,
    ) -> tuple[list[str], list[dict]]:
        """Delete multiple objects in a single batch call.

        Returns (deleted_keys, errors) where errors is a list of
        {"key": ..., "code": ..., "message": ...} dicts.
        """
        bucket_id = bucket_id or self._bucket_id(bucket)
        token_kwargs = {"token": token} if token is not None else {}
        # Expand directory keys to also delete the marker placeholder
        all_keys = list(keys)
        for key in keys:
            if key.endswith("/"):
                all_keys.append(key + DIR_MARKER_FILENAME)
        deleted: list[str] = []
        errors: list[dict] = []
        try:
            await self.hub.batch_files(bucket_id, delete=all_keys, **token_kwargs)
            deleted = list(keys)  # report original keys only
            for key in keys:
                self._invalidate_file_info(bucket_id, key)
        except Exception as exc:
            logger.exception("delete_objects batch failed")
            # Report every key as failed so the caller can build a proper
            # DeleteResult response.
            for key in keys:
                errors.append(
                    {"key": key, "code": "InternalError", "message": str(exc)}
                )
        return deleted, errors

    async def head_object(
        self,
        bucket: str,
        key: str,
        *,
        bucket_id: str | None = None,
        token: str | None = None,
    ) -> BucketFile | None:
        """Get object metadata (cached)."""
        bucket_id = bucket_id or self._bucket_id(bucket)
        return await self._get_file_info_cached(bucket_id, key, token=token)

    async def head_directory(
        self,
        bucket: str,
        prefix: str,
        *,
        bucket_id: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Check if a directory prefix exists.

        A directory is considered to exist if:
        1. A .hugbucket_keep marker file exists (explicitly created folder), OR
        2. Any objects exist under the prefix (implicit folder).

        This supports S3 clients (e.g. S3 Browser) that send HEAD requests
        on folder keys (trailing slash) to verify folder existence.  In real
        AWS S3 the console creates a 0-byte object for folders; HugBucket
        stores a hidden marker instead, so we need this fallback.
        """
        bucket_id = bucket_id or self._bucket_id(bucket)
        token_kwargs = {"token": token} if token is not None else {}

        # Fast path: check for the explicit directory marker
        marker = await self._get_file_info_cached(
            bucket_id,
            prefix + DIR_MARKER_FILENAME,
            **token_kwargs,
        )
        if marker is not None:
            return True

        # Slow path: check if any objects exist under this prefix
        all_files = await self.hub.list_bucket_tree(
            bucket_id,
            prefix=prefix,
            recursive=True,
            **token_kwargs,
        )
        return len(all_files) > 0

    async def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
        *,
        src_bucket_id: str | None = None,
        dst_bucket_id: str | None = None,
        token: str | None = None,
    ) -> dict:
        """Copy an object by registering the destination path with the same xetHash.

        Because Xet uses content-addressable storage, we don't need to
        re-download and re-upload the data — just register a new path
        pointing to the same content hash.

        Returns {"ETag": ..., "LastModified": ...}.
        """
        src_bucket_id = src_bucket_id or self._bucket_id(src_bucket)
        dst_bucket_id = dst_bucket_id or self._bucket_id(dst_bucket)
        token_kwargs = {"token": token} if token is not None else {}

        # Get source file metadata (using cache)
        src_file = await self._get_file_info_cached(
            src_bucket_id,
            src_key,
            **token_kwargs,
        )
        if not src_file:
            raise FileNotFoundError(f"Source object not found: {src_bucket}/{src_key}")

        # Register the new path with the same content hash
        content_type = mimetypes.guess_type(dst_key)[0] or "application/octet-stream"
        mtime_ms = int(time.time() * 1000)

        await self.hub.batch_files(
            dst_bucket_id,
            add=[
                {
                    "path": dst_key,
                    "xetHash": src_file.xet_hash,
                    "mtime": mtime_ms,
                    "contentType": content_type,
                }
            ],
            **token_kwargs,
        )

        self._invalidate_file_info(dst_bucket_id, dst_key)
        etag = f'"{src_file.xet_hash[:32]}"'
        last_modified = src_file.mtime or src_file.uploaded_at or ""
        logger.info(f"COPY {src_bucket}/{src_key} -> {dst_bucket}/{dst_key}")
        return {"ETag": etag, "LastModified": last_modified}

    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        continuation_token: str = "",
    ) -> dict:
        """List objects with S3-style prefix/delimiter support.

        Returns dict with keys:
            contents: list of file objects
            common_prefixes: list of prefix strings (when delimiter is used)
            is_truncated: bool
            next_continuation_token: str or None
        """
        bucket_id = self._bucket_id(bucket)

        # Get all files (recursive for prefix filtering)
        all_files = await self.hub.list_bucket_tree(
            bucket_id, prefix=prefix, recursive=True
        )

        # Filter by prefix (Hub should already do this, but be safe)
        filtered = [f for f in all_files if f.path.startswith(prefix)]

        contents: list[BucketFile] = []
        common_prefixes: set[str] = set()

        if delimiter:
            for f in filtered:
                # Get the part after the prefix
                rest = f.path[len(prefix) :]
                delim_pos = rest.find(delimiter)
                if delim_pos >= 0:
                    # This is a "directory" — add as common prefix
                    cp = prefix + rest[: delim_pos + len(delimiter)]
                    common_prefixes.add(cp)
                else:
                    if f.type == "file":
                        contents.append(f)
        else:
            contents = [f for f in filtered if f.type == "file"]

        # Hide directory-marker placeholder files from contents
        # (they must stay in the filtered list above so that empty folders
        # still contribute to common_prefixes)
        contents = [
            f for f in contents if not f.path.endswith("/" + DIR_MARKER_FILENAME)
        ]

        # Sort by key
        contents.sort(key=lambda f: f.path)
        sorted_prefixes = sorted(common_prefixes)

        # Pagination
        start_idx = 0
        if continuation_token:
            for i, c in enumerate(contents):
                if c.path > continuation_token:
                    start_idx = i
                    break

        truncated = len(contents) > start_idx + max_keys
        page = contents[start_idx : start_idx + max_keys]
        next_token = page[-1].path if truncated and page else None

        return {
            "contents": page,
            "common_prefixes": sorted_prefixes,
            "is_truncated": truncated,
            "next_continuation_token": next_token,
        }


# Backward-compatible name kept for existing imports/tests.
Bridge = HFStorageBackend
