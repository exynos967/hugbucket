"""Token pool with round-robin load balancing and health checking."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import dataclass

from hugbucket.admin.store import ConfigStore, AppConfig, TokenConfig

logger = logging.getLogger(__name__)


@dataclass
class PoolStatus:
    total: int
    healthy: int
    unhealthy: int
    strategy: str
    tokens: list[dict]


# 429 backoff duration (seconds) — matches HF rate-limit window.
_RATE_LIMIT_BACKOFF = 60

# How long a token stays "cooling" after 429 before retry.
_COOLDOWN_SECONDS = 30


class TokenPool:
    """Least-in-flight token pool with 429-aware backoff.

    - **acquire**: returns the healthy token with fewest in-flight
      requests, excluding tokens cooling after a 429.
    - **release**: decrements in-flight; if *rate_limited* is True the
      token enters a 30-second cooldown.

    Thread-safe for synchronous access from HubClient's token getter.
    """

    def __init__(self, store: ConfigStore) -> None:
        self._store = store
        self._config: AppConfig = AppConfig()
        self._index: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()
        self._sync_lock: threading.Lock = threading.Lock()
        self._loaded: bool = False
        # Runtime tracking — keyed by token string (not index — survives reorder)
        self._in_flight: dict[str, int] = {}
        self._cooldown_until: dict[str, float] = {}

    # -- public API ---------------------------------------------------------

    async def load(self) -> None:
        """Load tokens from store. Safe to call multiple times."""
        if self._loaded:
            return
        self._config = self._store.load()
        self._loaded = True
        logger.info("Token pool loaded: %d tokens", len(self._config.tokens))

    async def reload(self) -> None:
        """Force reload from disk."""
        self._config = self._store.load()
        logger.info("Token pool reloaded: %d tokens", len(self._config.tokens))

    # -- acquire / release ---------------------------------------------------

    def _candidates(self) -> list[str]:
        """Return token strings of healthy tokens not in cooldown."""
        now = time.time()
        result: list[str] = []
        for t in self._config.tokens:
            if not t.healthy:
                continue
            if self._cooldown_until.get(t.token, 0) > now:
                continue
            result.append(t.token)
        return result

    def acquire_sync(self) -> TokenConfig | None:
        """Pick the healthy token with fewest in-flight requests (sync).

        Returns None when no token is available.
        """
        with self._sync_lock:
            candidates = self._candidates()
            if not candidates:
                return None
            # Least in-flight — tie-break by token for stability
            best_token = min(candidates, key=lambda tk: (self._in_flight.get(tk, 0), tk))
            self._in_flight[best_token] = self._in_flight.get(best_token, 0) + 1
            for t in self._config.tokens:
                if t.token == best_token:
                    return t
            return None

    def release_sync(self, token: str, *, rate_limited: bool = False) -> None:
        """Release a token previously acquired via ``acquire_sync``."""
        with self._sync_lock:
            cnt = self._in_flight.get(token, 0)
            if cnt > 0:
                self._in_flight[token] = cnt - 1
            if rate_limited:
                self._cooldown_until[token] = time.time() + _COOLDOWN_SECONDS
                logger.warning("Token rate-limited, cooldown %ds", _COOLDOWN_SECONDS)

    async def acquire(self) -> TokenConfig | None:
        """Async version of ``acquire_sync``."""
        await self.load()
        async with self._lock:
            candidates = self._candidates()
            if not candidates:
                return None
            best_token = min(candidates, key=lambda tk: (self._in_flight.get(tk, 0), tk))
            self._in_flight[best_token] = self._in_flight.get(best_token, 0) + 1
            for t in self._config.tokens:
                if t.token == best_token:
                    return t
            return None

    async def release(self, token: str, *, rate_limited: bool = False) -> None:
        """Async version of ``release_sync``."""
        async with self._lock:
            cnt = self._in_flight.get(token, 0)
            if cnt > 0:
                self._in_flight[token] = cnt - 1
            if rate_limited:
                self._cooldown_until[token] = time.time() + _COOLDOWN_SECONDS
                logger.warning("Token rate-limited, cooldown %ds", _COOLDOWN_SECONDS)

    async def get_next(self) -> TokenConfig | None:
        """Deprecated alias — prefer ``acquire`` + ``release``."""
        return await self.acquire()

    async def get_namespace(self, token: TokenConfig) -> str:
        """Return the cached namespace for *token*."""
        return token.namespace

    async def add_token(
        self,
        token: str,
        label: str = "",
        *,
        namespace: str = "",
    ) -> TokenConfig:
        """Add a token and persist.

        If *namespace* is empty it will be resolved lazily.
        """
        await self.load()

        # Deduplicate
        for existing in self._config.tokens:
            if existing.token == token:
                raise ValueError("该 Token 已存在")

        entry = TokenConfig(
            token=token,
            label=label,
            namespace=namespace,
            healthy=True,
            last_checked=time.time(),
        )
        self._config.tokens.append(entry)
        self._store.save(self._config)
        logger.info("Added token: label=%s namespace=%s", label, namespace)
        return entry

    async def remove_token(self, index: int) -> None:
        """Remove token at *index* and persist."""
        await self.load()
        if index < 0 or index >= len(self._config.tokens):
            raise IndexError(f"Token index {index} out of range")
        removed = self._config.tokens.pop(index)
        self._store.save(self._config)
        logger.info("Removed token: label=%s", removed.label)

    async def update_token(
        self,
        index: int,
        *,
        token: str | None = None,
        label: str | None = None,
        namespace: str | None = None,
        healthy: bool | None = None,
    ) -> TokenConfig:
        """Update a token's fields."""
        await self.load()
        if index < 0 or index >= len(self._config.tokens):
            raise IndexError(f"Token index {index} out of range")
        entry = self._config.tokens[index]
        if token is not None:
            entry.token = token
        if label is not None:
            entry.label = label
        if namespace is not None:
            entry.namespace = namespace
        if healthy is not None:
            entry.healthy = healthy
            entry.last_checked = time.time()
        self._store.save(self._config)
        return entry

    async def mark_unhealthy(self, token_str: str) -> None:
        """Mark a token as unhealthy."""
        for i, t in enumerate(self._config.tokens):
            if t.token == token_str:
                await self.update_token(i, healthy=False)
                return

    async def resolve_namespace(
        self,
        entry: TokenConfig,
        whoami_fn,
    ) -> str:
        """Resolve namespace for *entry* via the HF whoami API.

        Caches the result in memory and persists to disk.
        """
        if entry.namespace:
            return entry.namespace

        try:
            name = await whoami_fn(token=entry.token)
            # Find the entry and update
            for i, t in enumerate(self._config.tokens):
                if t.token == entry.token:
                    await self.update_token(
                        i, namespace=name, healthy=True
                    )
                    break
            return name
        except Exception as e:
            logger.error("Failed to resolve namespace for token: %s", e)
            await self.mark_unhealthy(entry.token)
            raise

    def status(self) -> PoolStatus:
        """Return pool status for the admin API."""
        tokens = self._config.tokens
        healthy = sum(1 for t in tokens if t.healthy)
        return PoolStatus(
            total=len(tokens),
            healthy=healthy,
            unhealthy=len(tokens) - healthy,
            strategy=self._config.load_balance_strategy,
            tokens=[
                {
                    "index": i,
                    "label": t.label,
                    "namespace": t.namespace or "(未解析)",
                    "healthy": t.healthy,
                    "token_preview": _mask_token(t.token),
                    "last_checked": t.last_checked,
                }
                for i, t in enumerate(tokens)
            ],
        )

    async def get_token_for_namespace(self, namespace: str) -> TokenConfig | None:
        """Return a healthy token that matches *namespace*."""
        await self.load()
        for t in self._config.tokens:
            if t.namespace == namespace and t.healthy:
                return t
        return None

    @property
    def all_namespaces(self) -> list[str]:
        """Return all unique resolved namespaces across healthy tokens."""
        seen: set[str] = set()
        result: list[str] = []
        for t in self._config.tokens:
            if t.healthy and t.namespace and t.namespace not in seen:
                seen.add(t.namespace)
                result.append(t.namespace)
        return result

    @property
    def has_tokens(self) -> bool:
        return len(self._config.tokens) > 0 and any(
            t.healthy for t in self._config.tokens
        )



def _mask_token(token: str) -> str:
    if len(token) <= 8:
        return token[:2] + "****"
    return token[:4] + "****" + token[-4:]
