"""JSON file persistence for token configuration."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_TOKENS_FILE = "tokens.json"


@dataclass
class TokenConfig:
    token: str
    label: str = ""
    namespace: str = ""
    healthy: bool = True
    last_checked: float = 0.0


@dataclass
class AppConfig:
    tokens: list[TokenConfig] = field(default_factory=list)
    load_balance_strategy: str = "round_robin"


class ConfigStore:
    """JSON file-backed configuration store for admin settings."""

    def __init__(self, file_path: str | None = None) -> None:
        if file_path is None:
            file_path = os.environ.get(
                "HUGBUCKET_TOKENS_FILE",
                os.path.join(os.getcwd(), DEFAULT_TOKENS_FILE),
            )
        self._path = Path(file_path)

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> AppConfig:
        """Load config from JSON file. Returns defaults if file doesn't exist."""
        if not self._path.exists():
            logger.info("Tokens file not found at %s, using defaults", self._path)
            return AppConfig()

        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            tokens = [
                TokenConfig(
                    token=t.get("token", ""),
                    label=t.get("label", ""),
                    namespace=t.get("namespace", ""),
                    healthy=t.get("healthy", True),
                    last_checked=t.get("last_checked", 0.0),
                )
                for t in data.get("tokens", [])
            ]
            return AppConfig(
                tokens=tokens,
                load_balance_strategy=data.get(
                    "load_balance_strategy", "round_robin"
                ),
            )
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.error("Failed to parse tokens file: %s", e)
            return AppConfig()

    def save(self, config: AppConfig) -> None:
        """Persist config to JSON file atomically."""
        data = {
            "tokens": [
                {
                    "token": t.token,
                    "label": t.label,
                    "namespace": t.namespace,
                    "healthy": t.healthy,
                    "last_checked": t.last_checked,
                }
                for t in config.tokens
            ],
            "load_balance_strategy": config.load_balance_strategy,
        }

        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(self._path)
        logger.info("Saved %d tokens to %s", len(config.tokens), self._path)

    def exists(self) -> bool:
        return self._path.exists()
