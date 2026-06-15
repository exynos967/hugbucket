"""End-to-end integration tests via boto3 against a live S3 gateway.

Starts the real hugbucket S3 server backed by HF Hub, then exercises the
full S3 API through boto3 — the same path a real user would take.

Requires HF_TOKEN env var.
Run with:  HF_TOKEN=hf_xxx uv run pytest -m integration tests/test_boto3.py -v
Skip with: uv run pytest -m 'not integration'
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import time
import urllib.request
import urllib.error

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from aiohttp import web
from aiohttp.test_utils import TestServer

from hugbucket.config import Config
from hugbucket.bridge import Bridge
from hugbucket.s3.auth import s3_auth_middleware
from hugbucket.s3.server import S3Handler


# ── fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def s3_server(hf_token: str):
    """Start a single hugbucket S3 gateway for the whole test session.

    Runs the aiohttp server in a background thread with its own event loop
    so that synchronous boto3 calls from tests don't deadlock.
    """
    import threading

    loop = asyncio.new_event_loop()
    server = None
    bridge = None
    started = threading.Event()

    async def _run():
        nonlocal server, bridge
        config = Config()
        bridge = Bridge(config=config)
        config.hf_namespace = await bridge.hub.whoami(token=hf_token)

        handler = S3Handler(bridge)
        app = web.Application(
            client_max_size=256 * 1024 * 1024,
            middlewares=[s3_auth_middleware],
        )
        app["config"] = config
        handler.setup_routes(app)

        server = TestServer(app)
        await server.start_server()
        started.set()

        # Keep loop alive until stop is requested
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            await server.close()
            await bridge.close()

    task = loop.create_task(_run())
    thread = threading.Thread(target=loop.run_until_complete, args=(task,), daemon=True)
    thread.start()
    started.wait(timeout=30)

    yield server

    loop.call_soon_threadsafe(task.cancel)
    thread.join(timeout=10)
    loop.close()


@pytest.fixture(scope="session")
def s3(s3_server):
    """boto3 S3 client pointed at the test gateway."""
    url = f"http://{s3_server.host}:{s3_server.port}"
    return boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id="hugbucket",
        aws_secret_access_key="hugbucket",
        region_name="us-east-1",
        config=BotoConfig(
            signature_version="s3v4",
            retries={"max_attempts": 1},
        ),
    )


@pytest.fixture(scope="session")
def bucket(s3, s3_server):
    """Create a temporary bucket, yield its name, delete on teardown.

    Teardown lists every object inside the bucket and deletes them
    individually before deleting the bucket — so leftover files from
    failed tests never prevent bucket removal.
    """
    name = f"pytest-b3-{int(time.time()) % 100000}"
    s3.create_bucket(Bucket=name)
    yield name
    _drain_and_delete_bucket(s3, name)


# ── cleanup helpers ──────────────────────────────────────────────────────


def _drain_and_delete_bucket(s3, bucket_name: str) -> None:
    """Delete every object inside *bucket_name*, then delete the bucket.

    Uses list_objects_v2 to discover all keys and deletes them one-by-one.
    Silently ignores errors so that a partial cleanup never masks the
    original test failure.
    """
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get("Contents", []):
                try:
                    s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
                except Exception:
                    pass
    except Exception:
        pass

    # Also abort any in-progress multipart uploads
    try:
        resp = s3.list_multipart_uploads(Bucket=bucket_name)
        for upload in resp.get("Uploads", []):
            try:
                s3.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=upload["Key"],
                    UploadId=upload["UploadId"],
                )
            except Exception:
                pass
    except Exception:
        pass

    # Now delete the (hopefully empty) bucket
    try:
        s3.delete_bucket(Bucket=bucket_name)
    except Exception:
        pass


# ── helpers ──────────────────────────────────────────────────────────────


def _put(s3, **kw):
    return s3.put_object(**kw)


def _get_body(s3, **kw) -> bytes:
    """get_object and read full body."""
    resp = s3.get_object(**kw)
    return resp["Body"].read()


def _get_response(s3, **kw) -> dict:
    """get_object returning full response (body pre-read)."""
    resp = s3.get_object(**kw)
    resp["_Body"] = resp["Body"].read()
    return resp


def _head(s3, **kw):
    return s3.head_object(**kw)


def _delete(s3, **kw):
    return s3.delete_object(**kw)


def _list(s3, **kw):
    return s3.list_objects_v2(**kw)


def _list_buckets(s3):
    return s3.list_buckets()


# ── tests ────────────────────────────────────────────────────────────────


@pytest.mark.integration
class TestBoto3Buckets:
    """Bucket-level operations via boto3."""

    def test_list_buckets(self, s3) -> None:
        resp = _list_buckets(s3)
        assert "Buckets" in resp

    def test_create_and_delete_bucket(self, s3) -> None:
        name = f"pytest-cd-{int(time.time()) % 100000}"
        s3.create_bucket(Bucket=name)
        s3.delete_bucket(Bucket=name)

    def test_head_bucket(self, s3, bucket) -> None:
        resp = s3.head_bucket(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.integration
class TestBoto3UploadDownload:
    """Upload -> download round-trip via boto3, verifying byte integrity."""

    def test_small_file_roundtrip(self, s3, bucket) -> None:
        """1 KB file -- single CDC chunk."""
        data = os.urandom(1024)
        key = "small.bin"

        _put(s3, Bucket=bucket, Key=key, Body=data)
        downloaded = _get_body(s3, Bucket=bucket, Key=key)

        assert downloaded == data
        _delete(s3, Bucket=bucket, Key=key)

    def test_multi_chunk_roundtrip(self, s3, bucket) -> None:
        """256 KB file -- multiple CDC chunks, exercises LZ4 frame path."""
        data = os.urandom(256 * 1024)
        key = "big.bin"

        _put(s3, Bucket=bucket, Key=key, Body=data)
        downloaded = _get_body(s3, Bucket=bucket, Key=key)

        assert (
            hashlib.sha256(downloaded).hexdigest() == hashlib.sha256(data).hexdigest()
        )
        assert len(downloaded) == len(data)
        _delete(s3, Bucket=bucket, Key=key)

    def test_compressible_data_roundtrip(self, s3, bucket) -> None:
        """Highly compressible text -- guarantees LZ4 compression is used."""
        data = b"The quick brown fox jumps over the lazy dog. " * 500
        key = "text.txt"

        _put(s3, Bucket=bucket, Key=key, Body=data)
        downloaded = _get_body(s3, Bucket=bucket, Key=key)

        assert downloaded == data
        _delete(s3, Bucket=bucket, Key=key)


@pytest.mark.integration
class TestBoto3ObjectOps:
    """Head, list, delete, and error handling via boto3."""

    def test_head_object(self, s3, bucket) -> None:
        data = os.urandom(5 * 1024)
        key = "head-test.bin"
        _put(s3, Bucket=bucket, Key=key, Body=data)

        resp = _head(s3, Bucket=bucket, Key=key)
        assert resp["ContentLength"] == len(data)
        _delete(s3, Bucket=bucket, Key=key)

    def test_head_object_not_found(self, s3, bucket) -> None:
        """head_object on missing key should raise 404 ClientError."""
        with pytest.raises(ClientError) as exc_info:
            _head(s3, Bucket=bucket, Key="does-not-exist.bin")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    def test_get_object_not_found(self, s3, bucket) -> None:
        """get_object on missing key should raise ClientError."""
        with pytest.raises(ClientError):
            _get_body(s3, Bucket=bucket, Key="does-not-exist.bin")

    def test_list_objects_with_prefix(self, s3, bucket) -> None:
        _put(s3, Bucket=bucket, Key="dir/a.txt", Body=b"aaa")
        _put(s3, Bucket=bucket, Key="dir/b.txt", Body=b"bbb")
        _put(s3, Bucket=bucket, Key="other.txt", Body=b"other")

        resp = _list(s3, Bucket=bucket, Prefix="dir/")
        keys = {obj["Key"] for obj in resp.get("Contents", [])}
        assert "dir/a.txt" in keys
        assert "dir/b.txt" in keys

        _delete(s3, Bucket=bucket, Key="dir/a.txt")
        _delete(s3, Bucket=bucket, Key="dir/b.txt")
        _delete(s3, Bucket=bucket, Key="other.txt")

    def test_list_objects_with_delimiter(self, s3, bucket) -> None:
        """Delimiter groups keys into CommonPrefixes."""
        _put(s3, Bucket=bucket, Key="photos/2024/a.jpg", Body=b"a")
        _put(s3, Bucket=bucket, Key="photos/2025/b.jpg", Body=b"b")
        _put(s3, Bucket=bucket, Key="photos/c.jpg", Body=b"c")

        resp = _list(s3, Bucket=bucket, Prefix="photos/", Delimiter="/")
        prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        assert "photos/2024/" in prefixes
        assert "photos/2025/" in prefixes
        # c.jpg is directly under photos/ so it should be in Contents
        keys = {obj["Key"] for obj in resp.get("Contents", [])}
        assert "photos/c.jpg" in keys

        _delete(s3, Bucket=bucket, Key="photos/2024/a.jpg")
        _delete(s3, Bucket=bucket, Key="photos/2025/b.jpg")
        _delete(s3, Bucket=bucket, Key="photos/c.jpg")

    def test_delete_object(self, s3, bucket) -> None:
        key = "to-delete.bin"
        _put(s3, Bucket=bucket, Key=key, Body=b"bye")
        _delete(s3, Bucket=bucket, Key=key)

    def test_etag_returned(self, s3, bucket) -> None:
        """put_object returns ETag, head_object returns the same."""
        data = b"etag-test-data"
        key = "etag.bin"
        put_resp = _put(s3, Bucket=bucket, Key=key, Body=data)
        assert "ETag" in put_resp

        head_resp = _head(s3, Bucket=bucket, Key=key)
        assert "ETag" in head_resp
        _delete(s3, Bucket=bucket, Key=key)

    def test_content_type_detection(self, s3, bucket) -> None:
        """Server should return correct Content-Type based on key extension."""
        _put(s3, Bucket=bucket, Key="doc.txt", Body=b"hello")
        resp = _get_response(s3, Bucket=bucket, Key="doc.txt")
        assert "text/plain" in resp["ContentType"]
        _delete(s3, Bucket=bucket, Key="doc.txt")

        _put(s3, Bucket=bucket, Key="data.json", Body=b'{"a":1}')
        resp = _get_response(s3, Bucket=bucket, Key="data.json")
        assert "json" in resp["ContentType"]
        _delete(s3, Bucket=bucket, Key="data.json")


@pytest.mark.integration
class TestBoto3RangeRequests:
    """HTTP Range requests via boto3."""

    def test_byte_range_middle(self, s3, bucket) -> None:
        """bytes=100-199 -- a slice from the middle."""
        data = os.urandom(256 * 1024)
        key = "range-test.bin"
        _put(s3, Bucket=bucket, Key=key, Body=data)

        chunk = _get_body(s3, Bucket=bucket, Key=key, Range="bytes=100-199")
        assert len(chunk) == 100
        assert chunk == data[100:200]
        _delete(s3, Bucket=bucket, Key=key)

    def test_byte_range_suffix(self, s3, bucket) -> None:
        """bytes=1000- -- from offset to end of file."""
        data = os.urandom(5 * 1024)
        key = "range-suffix.bin"
        _put(s3, Bucket=bucket, Key=key, Body=data)

        chunk = _get_body(s3, Bucket=bucket, Key=key, Range="bytes=1000-")
        assert chunk == data[1000:]
        _delete(s3, Bucket=bucket, Key=key)

    def test_byte_range_first_n(self, s3, bucket) -> None:
        """bytes=0-49 -- first 50 bytes."""
        data = os.urandom(5 * 1024)
        key = "range-first.bin"
        _put(s3, Bucket=bucket, Key=key, Body=data)

        chunk = _get_body(s3, Bucket=bucket, Key=key, Range="bytes=0-49")
        assert len(chunk) == 50
        assert chunk == data[:50]
        _delete(s3, Bucket=bucket, Key=key)


@pytest.mark.integration
class TestBoto3MultipartUpload:
    """Multipart upload via boto3 -- used for large files."""

    def test_multipart_upload(self, s3, bucket) -> None:
        """Initiate -> upload 2 parts -> complete -> download verify."""
        key = "multipart.bin"
        part1 = os.urandom(5 * 1024)
        part2 = os.urandom(3 * 1024)
        expected = part1 + part2

        # Initiate
        resp = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = resp["UploadId"]

        # Upload parts
        p1 = s3.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part1,
        )
        p2 = s3.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=2,
            Body=part2,
        )

        # Complete
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": p1["ETag"]},
                    {"PartNumber": 2, "ETag": p2["ETag"]},
                ]
            },
        )

        downloaded = _get_body(s3, Bucket=bucket, Key=key)
        assert downloaded == expected
        _delete(s3, Bucket=bucket, Key=key)

    def test_abort_multipart_upload(self, s3, bucket) -> None:
        """Initiate -> abort -- upload should be cleaned up."""
        key = "abort-me.bin"
        resp = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = resp["UploadId"]
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)


@pytest.mark.integration
class TestBoto3PresignedURL:
    """Presigned URL generation and download -- browser-style access."""

    def test_presigned_url_full_download(self, s3, s3_server, bucket) -> None:
        """Generate presigned URL, fetch via plain HTTP (no AWS auth)."""
        data = os.urandom(256 * 1024)
        key = "presigned-test.bin"
        _put(s3, Bucket=bucket, Key=key, Body=data)

        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=3600,
        )
        assert url.startswith(f"http://{s3_server.host}:{s3_server.port}/")

        downloaded = urllib.request.urlopen(url).read()
        assert (
            hashlib.sha256(downloaded).hexdigest() == hashlib.sha256(data).hexdigest()
        )
        _delete(s3, Bucket=bucket, Key=key)

    def test_presigned_url_with_range(self, s3, s3_server, bucket) -> None:
        """Presigned URL + Range header for partial downloads."""
        data = os.urandom(256 * 1024)
        key = "presigned-range.bin"
        _put(s3, Bucket=bucket, Key=key, Body=data)

        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=3600,
        )

        req = urllib.request.Request(url, headers={"Range": "bytes=0-99"})
        chunk = urllib.request.urlopen(req).read()
        assert chunk == data[:100]
        _delete(s3, Bucket=bucket, Key=key)

    def test_presigned_url_put(self, s3, s3_server, bucket) -> None:
        """Upload via presigned PUT URL, then download and verify."""
        data = os.urandom(10 * 1024)
        key = "presigned-upload.bin"

        url = s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=3600,
        )

        req = urllib.request.Request(url, data=data, method="PUT")
        req.add_header("Content-Length", str(len(data)))
        urllib.request.urlopen(req)

        downloaded = _get_body(s3, Bucket=bucket, Key=key)
        assert downloaded == data
        _delete(s3, Bucket=bucket, Key=key)
