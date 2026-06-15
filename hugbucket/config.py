"""Configuration for HugBucket."""

import os
from dataclasses import dataclass, field


@dataclass
class Config:
    # S3 gateway settings
    host: str = "0.0.0.0"
    port: int = 9000
    region: str = "us-east-1"

    # HF Hub settings
    hf_endpoint: str = "https://huggingface.co"

    # Admin panel password (required)
    admin_password: str = field(
        default_factory=lambda: os.environ.get("ADMIN_PASSWORD", "")
    )

    # Admin / token-pool settings
    tokens_file: str = field(
        default_factory=lambda: os.environ.get(
            "HUGBUCKET_TOKENS_FILE",
            os.path.join(os.getcwd(), "tokens.json"),
        )
    )

    # S3 auth — maps to HF token
    s3_access_key: str = field(
        default_factory=lambda: os.environ.get("AWS_ACCESS_KEY_ID", "")
    )
    s3_secret_key: str = field(
        default_factory=lambda: os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    )

    # HF namespace (user or org that owns the buckets)
    # Resolved automatically from HF token via /api/whoami-v2 at startup.
    # When a token pool is active this is set from the first resolved token.
    hf_namespace: str = ""

    # Xet CDC settings
    xet_chunk_target: int = 65536  # 64 KiB
    xet_chunk_min: int = 8192  # 8 KiB
    xet_chunk_max: int = 131072  # 128 KiB
    xet_xorb_max_bytes: int = 67108864  # 64 MiB

    # Concurrency / connection-pool settings
    # Total outbound connections shared across all concurrent downloads.
    # 0 = unlimited (no cap on simultaneous outbound connections).
    http_pool_size: int = 0

    # Upload settings
    cas_upload_timeout: int = 300  # 5 minutes per CAS request
    cas_upload_retries: int = 3  # retry count for CAS xorb/shard uploads
    cas_retry_base_delay: float = 1.0  # base delay (seconds) for exponential backoff
    multipart_upload_ttl: int = 86400  # 24 hours before stale multipart cleanup

    # Cache settings
    xorb_cache_max_bytes: int = 512 * 1024 * 1024  # 512 MiB
    recon_cache_max_entries: int = 1024
    recon_cache_ttl: int = 300  # 5 minutes
    file_info_cache_max_entries: int = 256
    file_info_cache_ttl: int = 30  # 30 seconds — short enough for consistency
