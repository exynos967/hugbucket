"""S3 gateway + Admin panel entrypoint."""

from __future__ import annotations

import argparse
import logging
import sys

from aiohttp import web

from hugbucket.bridge import HFStorageBackend
from hugbucket.config import Config
from hugbucket.s3.app import create_s3_app

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="HugBucket: S3-compatible gateway for HF Storage Buckets"
    )
    parser.add_argument(
        "--host", default="0.0.0.0", help="S3 bind host (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", type=int, default=9000, help="S3 bind port (default: 9000)"
    )
    parser.add_argument(
        "--admin-host",
        default=None,
        help="Admin panel bind host (default: same as --host)",
    )
    parser.add_argument(
        "--admin-port",
        type=int,
        default=None,
        help="Admin panel port (default: 9001, or from tokens.json)",
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

    # -- Token pool setup -------------------------------------------------
    from hugbucket.admin.store import ConfigStore
    from hugbucket.admin.token_pool import TokenPool

    store = ConfigStore(config.tokens_file)
    token_pool = TokenPool(store)

    # Pre-load tokens (async — will be done in startup)
    backend = HFStorageBackend(
        config=config,
        _token_pool=token_pool,
    )

    # Determine admin port
    admin_port = args.admin_port
    if admin_port is None:
        # Check tokens.json for admin_port
        app_cfg = store.load()
        admin_port = app_cfg.admin_port
    admin_host = args.admin_host or args.host

    # -- Validate token config --------------------------------------------
    if not config.hf_token and not store.exists():
        logger.warning("=" * 60)
        logger.warning("  No HF_TOKEN env var and no tokens.json found.")
        logger.warning("  The S3 gateway will start, but you must configure")
        logger.warning("  at least one HF token via the admin panel:")
        logger.warning(f"  → http://localhost:{admin_port}")
        logger.warning("=" * 60)

    if not config.s3_access_key and not config.s3_secret_key:
        logger.warning(
            "S3 authentication is disabled (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY empty)."
        )

    # -- Build apps -------------------------------------------------------
    s3_app = create_s3_app(
        config=config,
        backend=backend,
        max_upload_bytes=1024 * 1024 * 1024,
    )

    from hugbucket.admin.app import create_admin_app

    admin_app = create_admin_app(
        bridge=backend,
        token_pool=token_pool,
        config=config,
    )

    # Store token_pool so the S3 on_startup handler can resolve namespaces
    s3_app["token_pool"] = token_pool

    # -- Patch S3 startup to load tokens first ----------------------------
    # We need to resolve namespaces from token pool before the S3 gateway
    # is ready.  The original on_startup handler does this for single-token
    # mode; here we extend it for multi-token mode.
    _original_startup = s3_app.on_startup[0]

    async def patched_startup(app: web.Application) -> None:
        # Load token pool and resolve namespaces for all tokens
        await token_pool.load()
        if token_pool.has_tokens:
            resolved = 0
            for token_entry in list(token_pool._config.tokens):
                if token_entry.healthy and not token_entry.namespace:
                    try:
                        await token_pool.resolve_namespace(
                            token_entry, backend.hub.whoami
                        )
                        resolved += 1
                    except Exception as e:
                        logger.warning(
                            "Failed to resolve namespace for token %s: %s",
                            token_entry.label or token_entry.token[:10],
                            e,
                        )
            if resolved:
                logger.info("Resolved %d token namespace(s)", resolved)

            # Set primary namespace from first resolved token
            for t in token_pool._config.tokens:
                if t.healthy and t.namespace:
                    config.hf_namespace = t.namespace
                    break

        await _original_startup(app)

    s3_app.on_startup.clear()
    s3_app.on_startup.append(patched_startup)

    # -- Start both servers -----------------------------------------------
    async def serve_both() -> None:
        s3_runner = web.AppRunner(s3_app)
        admin_runner = web.AppRunner(admin_app)
        await s3_runner.setup()
        await admin_runner.setup()

        s3_site = web.TCPSite(s3_runner, config.host, config.port)
        admin_site = web.TCPSite(admin_runner, admin_host, admin_port)

        await s3_site.start()
        await admin_site.start()

        logger.info("=" * 60)
        logger.info(
            "  S3 Gateway:  http://%s:%d", config.host, config.port
        )
        logger.info(
            "  Admin Panel: http://%s:%d", admin_host, admin_port
        )
        logger.info("=" * 60)

        # Keep running
        import asyncio

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        finally:
            await s3_runner.cleanup()
            await admin_runner.cleanup()

    try:
        import asyncio

        asyncio.run(serve_both())
    except KeyboardInterrupt:
        logger.info("Shutting down…")


if __name__ == "__main__":
    main()
