"""HugBucket entrypoint — S3 gateway + Admin panel on a single port."""

from __future__ import annotations

import argparse
import logging

from aiohttp import web

from hugbucket.bridge import HFStorageBackend
from hugbucket.config import Config
from hugbucket.s3.app import create_app

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="HugBucket: S3-compatible gateway for HF Storage Buckets"
    )
    parser.add_argument(
        "--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", type=int, default=9000, help="Bind port (default: 9000)"
    )
    parser.add_argument(
        "--tokens-file",
        default=None,
        help="Path to tokens.json (default: HUGBUCKET_TOKENS_FILE or ./tokens.json)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = Config(
        host=args.host,
        port=args.port,
        tokens_file=args.tokens_file or Config().tokens_file,
    )

    # -- Token pool -------------------------------------------------------
    from hugbucket.admin.store import ConfigStore
    from hugbucket.admin.token_pool import TokenPool

    store = ConfigStore(config.tokens_file)
    token_pool = TokenPool(store)

    backend = HFStorageBackend(
        config=config,
        _token_pool=token_pool,
    )

    # -- Validate ---------------------------------------------------------
    if not store.exists():
        logger.warning("=" * 60)
        logger.warning("  No HF_TOKEN env var and no tokens.json found.")
        logger.warning("  The gateway will start, but you must configure")
        logger.warning("  at least one HF token via the admin panel:")
        logger.warning(f"  → http://localhost:{config.port}/admin")
        logger.warning("=" * 60)

    if not config.s3_access_key and not config.s3_secret_key:
        logger.warning(
            "S3 authentication is disabled (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY empty)."
        )

    # -- Build & run ------------------------------------------------------
    app = create_app(
        config=config,
        backend=backend,
        token_pool=token_pool,
        max_upload_bytes=1024 * 1024 * 1024,
    )

    web.run_app(app, host=config.host, port=config.port, print=None)


if __name__ == "__main__":
    main()
