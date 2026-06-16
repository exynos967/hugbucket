"""Tests for the S3 HTTP server using aiohttp test client.

These tests mock the Bridge layer to avoid hitting the live HF API,
focusing on correct S3 protocol behavior.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock
from xml.etree.ElementTree import fromstring

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient

from hugbucket.hub.client import BucketInfo, BucketFile
from hugbucket.s3.server import S3Handler


async def _async_chunks(*chunks: bytes) -> AsyncIterator[bytes]:
    """Helper: create an async iterator yielding the given byte chunks."""
    for chunk in chunks:
        yield chunk


@pytest.fixture
def mock_bridge() -> MagicMock:
    """Create a mock Bridge with async methods."""
    bridge = MagicMock()
    bridge.list_buckets = AsyncMock(return_value=[])
    bridge.create_bucket = AsyncMock(return_value="")
    bridge.delete_bucket = AsyncMock()
    bridge.head_bucket = AsyncMock(return_value=None)
    bridge.put_object = AsyncMock(return_value={"ETag": '"abc123"', "size": 0})
    bridge.pool_put_object = AsyncMock(return_value={"ETag": '"abc123"', "size": 0})
    bridge.get_object = AsyncMock(return_value=None)
    bridge.get_object_stream = AsyncMock(return_value=None)
    bridge.pool_get_object_stream = AsyncMock(return_value=None)
    bridge.delete_object = AsyncMock()
    bridge.pool_delete_object = AsyncMock()
    bridge.delete_objects = AsyncMock(return_value=([], []))
    bridge.pool_delete_objects = AsyncMock(return_value=([], []))
    bridge.head_object = AsyncMock(return_value=None)
    bridge.pool_head_object = AsyncMock(return_value=None)
    bridge.head_directory = AsyncMock(return_value=False)
    bridge.pool_head_directory = AsyncMock(return_value=False)
    bridge.copy_object = AsyncMock(
        return_value={"ETag": '"abc123"', "LastModified": "2026-01-01T00:00:00Z"}
    )
    bridge.pool_copy_object = AsyncMock(
        return_value={"ETag": '"abc123"', "LastModified": "2026-01-01T00:00:00Z"}
    )
    bridge.list_objects = AsyncMock(
        return_value={
            "contents": [],
            "common_prefixes": [],
            "is_truncated": False,
            "next_continuation_token": None,
        }
    )
    return bridge


@pytest.fixture
def app(mock_bridge: MagicMock) -> web.Application:
    """Create aiohttp app with S3 handler."""
    application = web.Application(client_max_size=16 * 1024 * 1024)
    application["config"] = MagicMock(pool_bucket_name="")
    application["bridge"] = mock_bridge
    handler = S3Handler(mock_bridge)
    handler.setup_routes(application)
    return application


@pytest.fixture
async def client(aiohttp_client, app: web.Application) -> TestClient:
    """Create test client."""
    return await aiohttp_client(app)


@pytest.fixture
def pool_app(mock_bridge: MagicMock) -> web.Application:
    """Create aiohttp app in aggregate pool mode."""
    application = web.Application(client_max_size=16 * 1024 * 1024)
    application["config"] = MagicMock(pool_bucket_name="pool")
    application["bridge"] = mock_bridge
    handler = S3Handler(mock_bridge)
    handler.setup_routes(application)
    return application


@pytest.fixture
async def pool_client(aiohttp_client, pool_app: web.Application) -> TestClient:
    """Create test client for aggregate pool mode."""
    return await aiohttp_client(pool_app)


class TestListBuckets:
    async def test_empty_list(self, client: TestClient) -> None:
        resp = await client.get("/")
        assert resp.status == 200
        body = await resp.text()
        assert "ListAllMyBucketsResult" in body

    async def test_with_buckets(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.list_buckets.return_value = [
            BucketInfo(
                id="user/my-bucket",
                private=False,
                created_at="2026-01-01T00:00:00Z",
                size=1000,
                total_files=5,
            )
        ]
        resp = await client.get("/")
        assert resp.status == 200
        body = await resp.text()
        assert "my-bucket" in body


class TestPoolMode:
    async def test_list_buckets_exposes_only_virtual_pool(
        self, pool_client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await pool_client.get("/")
        assert resp.status == 200
        body = await resp.text()
        assert "pool" in body
        mock_bridge.list_buckets.assert_not_awaited()

    async def test_non_pool_bucket_is_rejected(
        self, pool_client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await pool_client.put("/real-bucket/file.txt", data=b"hello")
        assert resp.status == 404
        assert "NoSuchBucket" in await resp.text()
        mock_bridge.put_object.assert_not_awaited()
        mock_bridge.pool_put_object.assert_not_awaited()

    async def test_create_virtual_pool_bucket_is_noop(
        self, pool_client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await pool_client.put("/pool")
        assert resp.status == 200
        mock_bridge.create_bucket.assert_not_awaited()

    async def test_delete_virtual_pool_bucket_is_noop(
        self, pool_client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await pool_client.delete("/pool")
        assert resp.status == 204
        mock_bridge.delete_bucket.assert_not_awaited()

    async def test_pool_put_uses_pool_bridge(
        self, pool_client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await pool_client.put("/pool/file.txt", data=b"hello")
        assert resp.status == 200
        mock_bridge.pool_put_object.assert_awaited_once_with("file.txt", b"hello")
        mock_bridge.put_object.assert_not_awaited()

    async def test_pool_batch_delete_uses_pool_bridge(
        self, pool_client: TestClient, mock_bridge: MagicMock
    ) -> None:
        body = (
            b"<Delete><Object><Key>a.txt</Key></Object>"
            b"<Object><Key>b.txt</Key></Object></Delete>"
        )
        mock_bridge.pool_delete_objects.return_value = (["a.txt", "b.txt"], [])
        resp = await pool_client.post("/pool?delete", data=body)
        assert resp.status == 200
        mock_bridge.pool_delete_objects.assert_awaited_once_with(["a.txt", "b.txt"])
        mock_bridge.delete_objects.assert_not_awaited()

    async def test_pool_copy_uses_pool_bridge(
        self, pool_client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await pool_client.put(
            "/pool/new.txt",
            headers={"x-amz-copy-source": "/pool/old.txt"},
        )
        assert resp.status == 200
        mock_bridge.pool_copy_object.assert_awaited_once_with("old.txt", "new.txt")
        mock_bridge.copy_object.assert_not_awaited()

    async def test_pool_multipart_complete_uses_pool_bridge(
        self, pool_client: TestClient, mock_bridge: MagicMock
    ) -> None:
        init = await pool_client.post("/pool/big.bin?uploads")
        assert init.status == 200
        upload_id = fromstring(await init.read()).findtext(
            "{http://s3.amazonaws.com/doc/2006-03-01/}UploadId"
        )
        assert upload_id

        part = await pool_client.put(
            f"/pool/big.bin?partNumber=1&uploadId={upload_id}",
            data=b"hello",
        )
        assert part.status == 200

        complete = await pool_client.post(
            f"/pool/big.bin?uploadId={upload_id}",
            data=b"<CompleteMultipartUpload/>",
        )
        assert complete.status == 200
        mock_bridge.pool_put_object.assert_awaited_once_with("big.bin", b"hello")
        mock_bridge.put_object.assert_not_awaited()


class TestBucketOps:
    async def test_create_bucket(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await client.put("/my-new-bucket")
        assert resp.status == 200
        mock_bridge.create_bucket.assert_awaited_once_with("my-new-bucket")

    async def test_delete_bucket(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await client.delete("/my-bucket")
        assert resp.status == 204
        mock_bridge.delete_bucket.assert_awaited_once_with("my-bucket")

    async def test_head_bucket_found(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.head_bucket.return_value = BucketInfo(
            id="user/test", private=False, created_at="", size=0, total_files=0
        )
        resp = await client.head("/test")
        assert resp.status == 200

    async def test_head_bucket_not_found(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.head_bucket.return_value = None
        resp = await client.head("/nonexistent")
        assert resp.status == 404


class TestPutObject:
    async def test_put_returns_etag(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.put_object.return_value = {"ETag": '"abc123"', "size": 5}
        resp = await client.put("/bucket/key.txt", data=b"hello")
        assert resp.status == 200
        assert resp.headers.get("ETag") == '"abc123"'
        mock_bridge.put_object.assert_awaited_once()

    async def test_put_folder_marker(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Creating a folder (trailing-slash, empty body) should succeed."""
        etag = hashlib.md5(b"").hexdigest()
        mock_bridge.put_object.return_value = {"ETag": f'"{etag}"', "size": 0}
        resp = await client.put("/bucket/New Folder/", data=b"")
        assert resp.status == 200
        assert "ETag" in resp.headers
        mock_bridge.put_object.assert_awaited_once_with("bucket", "New Folder/", b"")

    async def test_put_nested_folder_marker(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Creating a nested folder should succeed."""
        etag = hashlib.md5(b"").hexdigest()
        mock_bridge.put_object.return_value = {"ETag": f'"{etag}"', "size": 0}
        resp = await client.put("/bucket/parent/child/", data=b"")
        assert resp.status == 200
        mock_bridge.put_object.assert_awaited_once_with("bucket", "parent/child/", b"")


class TestDeleteObject:
    async def test_delete(self, client: TestClient, mock_bridge: MagicMock) -> None:
        resp = await client.delete("/bucket/key.txt")
        assert resp.status == 204
        mock_bridge.delete_object.assert_awaited_once_with("bucket", "key.txt")

    async def test_delete_folder_key(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Deleting a folder-marker key (trailing slash) should succeed."""
        resp = await client.delete("/bucket/New Folder/")
        assert resp.status == 204
        mock_bridge.delete_object.assert_awaited_once_with("bucket", "New Folder/")


class TestCopyObject:
    async def test_copy_object(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """PUT with x-amz-copy-source should copy the object."""
        mock_bridge.copy_object.return_value = {
            "ETag": '"abc123"',
            "LastModified": "2026-01-01T00:00:00Z",
        }
        resp = await client.put(
            "/dst-bucket/dst-key.txt",
            headers={"x-amz-copy-source": "/src-bucket/src-key.txt"},
        )
        assert resp.status == 200
        body = await resp.read()
        root = fromstring(body)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        assert root.find("s3:ETag", ns) is not None
        mock_bridge.copy_object.assert_awaited_once_with(
            "src-bucket", "src-key.txt", "dst-bucket", "dst-key.txt"
        )

    async def test_copy_object_url_encoded_source(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """x-amz-copy-source with URL-encoded path should be decoded."""
        mock_bridge.copy_object.return_value = {
            "ETag": '"def456"',
            "LastModified": "",
        }
        resp = await client.put(
            "/bucket/new%20name.txt",
            headers={"x-amz-copy-source": "/bucket/old%20name.txt"},
        )
        assert resp.status == 200
        mock_bridge.copy_object.assert_awaited_once_with(
            "bucket", "old name.txt", "bucket", "new name.txt"
        )

    async def test_copy_object_source_not_found(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """CopyObject with nonexistent source returns 404."""
        mock_bridge.copy_object.side_effect = FileNotFoundError("not found")
        resp = await client.put(
            "/bucket/dest.txt",
            headers={"x-amz-copy-source": "/bucket/missing.txt"},
        )
        assert resp.status == 404

    async def test_copy_object_invalid_source(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """CopyObject with invalid x-amz-copy-source returns 400."""
        resp = await client.put(
            "/bucket/dest.txt",
            headers={"x-amz-copy-source": "no-key"},
        )
        assert resp.status == 400


class TestGetObject:
    async def test_get_existing_object(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        test_data = b"hello world"
        mock_bridge.head_object.return_value = BucketFile(
            type="file",
            path="key.txt",
            size=len(test_data),
            xet_hash="a" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        mock_bridge.get_object_stream.return_value = _async_chunks(test_data)
        resp = await client.get("/bucket/key.txt")
        assert resp.status == 200
        body = await resp.read()
        assert body == test_data
        assert "ETag" in resp.headers
        assert "Last-Modified" in resp.headers

    async def test_get_missing_object(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.head_object.return_value = None
        resp = await client.get("/bucket/missing.txt")
        assert resp.status == 404
        body = await resp.text()
        assert "NoSuchKey" in body

    async def test_range_request(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.head_object.return_value = BucketFile(
            type="file",
            path="data.bin",
            size=10,
            xet_hash="b" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        # Bridge returns already-trimmed data for byte_range=(2,5)
        mock_bridge.get_object_stream.return_value = _async_chunks(b"2345")
        resp = await client.get("/bucket/data.bin", headers={"Range": "bytes=2-5"})
        assert resp.status == 206
        body = await resp.read()
        assert body == b"2345"
        assert resp.headers["Content-Range"] == "bytes 2-5/10"

    async def test_range_request_out_of_bounds(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.head_object.return_value = BucketFile(
            type="file",
            path="data.bin",
            size=5,
            xet_hash="c" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        # 416 is returned before get_object_stream is called
        resp = await client.get("/bucket/data.bin", headers={"Range": "bytes=100-200"})
        assert resp.status == 416

    async def test_range_request_suffix(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """bytes=5- means from byte 5 to end."""
        mock_bridge.head_object.return_value = BucketFile(
            type="file",
            path="data.bin",
            size=10,
            xet_hash="d" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        # Bridge returns already-trimmed data for byte_range=(5,9)
        mock_bridge.get_object_stream.return_value = _async_chunks(b"56789")
        resp = await client.get("/bucket/data.bin", headers={"Range": "bytes=5-"})
        assert resp.status == 206
        body = await resp.read()
        assert body == b"56789"

    async def test_streaming_multi_chunk(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Verify that multiple chunks are streamed and reassembled correctly."""
        mock_bridge.head_object.return_value = BucketFile(
            type="file",
            path="multi.bin",
            size=16,
            xet_hash="e" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        mock_bridge.get_object_stream.return_value = _async_chunks(
            b"Hello", b" ", b"World", b"!!!!!"
        )
        resp = await client.get("/bucket/multi.bin")
        assert resp.status == 200
        body = await resp.read()
        assert body == b"Hello World!!!!!"

    async def test_range_across_multiple_chunks(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Range request that spans across multiple streamed chunks."""
        mock_bridge.head_object.return_value = BucketFile(
            type="file",
            path="chunked.bin",
            size=15,
            xet_hash="f" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        # Bridge returns already-trimmed data for byte_range=(3,11)
        mock_bridge.get_object_stream.return_value = _async_chunks(
            b"AA", b"BBBBB", b"CC"
        )
        # Request bytes 3-11 → "AABBBBBCC"
        resp = await client.get("/bucket/chunked.bin", headers={"Range": "bytes=3-11"})
        assert resp.status == 206
        body = await resp.read()
        assert body == b"AABBBBBCC"
        assert resp.headers["Content-Range"] == "bytes 3-11/15"


class TestHeadObject:
    async def test_head_found(self, client: TestClient, mock_bridge: MagicMock) -> None:
        mock_bridge.head_object.return_value = BucketFile(
            type="file",
            path="key.txt",
            size=1234,
            xet_hash="e" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        resp = await client.head("/bucket/key.txt")
        assert resp.status == 200
        assert resp.headers["Content-Length"] == "1234"
        assert "ETag" in resp.headers
        assert "Last-Modified" in resp.headers

    async def test_head_not_found(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.head_object.return_value = None
        resp = await client.head("/bucket/missing.txt")
        assert resp.status == 404

    async def test_head_directory_with_marker(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """HEAD on a folder key should return 200 when .hugbucket_keep marker exists."""
        mock_bridge.head_object.return_value = None
        mock_bridge.head_directory.return_value = True
        resp = await client.head("/bucket/avatars/")
        assert resp.status == 200
        assert resp.headers["Content-Length"] == "0"
        assert resp.headers["Content-Type"] == "application/x-directory"
        mock_bridge.head_directory.assert_awaited_once_with("bucket", "avatars/")

    async def test_head_directory_with_files(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """HEAD on a folder key should return 200 when objects exist under prefix."""
        mock_bridge.head_object.return_value = None
        mock_bridge.head_directory.return_value = True
        resp = await client.head("/bucket/photos/vacation/")
        assert resp.status == 200
        assert resp.headers["Content-Length"] == "0"
        assert resp.headers["Content-Type"] == "application/x-directory"

    async def test_head_directory_not_found(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """HEAD on a non-existent folder should return 404."""
        mock_bridge.head_object.return_value = None
        mock_bridge.head_directory.return_value = False
        resp = await client.head("/bucket/nonexistent/")
        assert resp.status == 404

    async def test_head_non_directory_key_no_fallback(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """HEAD on a non-directory key (no trailing slash) should NOT try directory check."""
        mock_bridge.head_object.return_value = None
        resp = await client.head("/bucket/missing.txt")
        assert resp.status == 404
        mock_bridge.head_directory.assert_not_awaited()


class TestListObjectsV2:
    async def test_empty_listing(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await client.get("/bucket?list-type=2")
        assert resp.status == 200
        body = await resp.text()
        assert "ListBucketResult" in body

    async def test_with_prefix_and_delimiter(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.list_objects.return_value = {
            "contents": [
                BucketFile(
                    type="file",
                    path="dir/file.txt",
                    size=100,
                    xet_hash="f" * 64,
                    mtime="2026-01-01T00:00:00Z",
                )
            ],
            "common_prefixes": ["dir/sub/"],
            "is_truncated": False,
            "next_continuation_token": None,
        }
        resp = await client.get("/bucket?list-type=2&prefix=dir/&delimiter=/")
        assert resp.status == 200
        body = await resp.text()
        assert "dir/file.txt" in body
        assert "dir/sub/" in body


class TestMultipartUpload:
    async def test_full_multipart_flow(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Test initiate → upload parts → complete."""
        # Initiate
        resp = await client.post("/bucket/big-file.bin?uploads")
        assert resp.status == 200
        body = await resp.text()
        root = fromstring(body)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        upload_id_el = root.find("s3:UploadId", ns)
        assert upload_id_el is not None
        upload_id = upload_id_el.text
        assert upload_id

        # Upload parts
        part1 = b"A" * 5 * 1024 * 1024
        part2 = b"B" * 3 * 1024 * 1024
        etag1 = hashlib.md5(part1).hexdigest()
        etag2 = hashlib.md5(part2).hexdigest()

        resp = await client.put(
            f"/bucket/big-file.bin?partNumber=1&uploadId={upload_id}",
            data=part1,
        )
        assert resp.status == 200
        assert resp.headers["ETag"] == f'"{etag1}"'

        resp = await client.put(
            f"/bucket/big-file.bin?partNumber=2&uploadId={upload_id}",
            data=part2,
        )
        assert resp.status == 200

        # Complete
        complete_xml = f"""<CompleteMultipartUpload>
          <Part><PartNumber>1</PartNumber><ETag>"{etag1}"</ETag></Part>
          <Part><PartNumber>2</PartNumber><ETag>"{etag2}"</ETag></Part>
        </CompleteMultipartUpload>"""

        mock_bridge.put_object.return_value = {
            "ETag": '"combined-etag"',
            "size": len(part1) + len(part2),
        }
        resp = await client.post(
            f"/bucket/big-file.bin?uploadId={upload_id}",
            data=complete_xml.encode(),
        )
        assert resp.status == 200
        body = await resp.text()
        assert "CompleteMultipartUploadResult" in body

        # Verify bridge.put_object was called with concatenated data
        call_args = mock_bridge.put_object.call_args
        assert call_args[0][0] == "bucket"
        assert call_args[0][1] == "big-file.bin"
        assert call_args[0][2] == part1 + part2

    async def test_abort_multipart(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        # Initiate
        resp = await client.post("/bucket/file.bin?uploads")
        body = await resp.text()
        root = fromstring(body)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        upload_id_el = root.find("s3:UploadId", ns)
        assert upload_id_el is not None
        upload_id = upload_id_el.text

        # Abort
        resp = await client.delete(f"/bucket/file.bin?uploadId={upload_id}")
        assert resp.status == 204

    async def test_upload_part_nonexistent_upload(self, client: TestClient) -> None:
        resp = await client.put(
            "/bucket/file.bin?partNumber=1&uploadId=nonexistent",
            data=b"data",
        )
        assert resp.status == 404

    async def test_complete_nonexistent_upload(self, client: TestClient) -> None:
        resp = await client.post(
            "/bucket/file.bin?uploadId=nonexistent",
            data=b"<CompleteMultipartUpload/>",
        )
        assert resp.status == 404

    async def test_idempotent_complete_returns_cached_result(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Calling CompleteMultipartUpload twice returns the same cached result."""
        # Initiate
        resp = await client.post("/bucket/file.bin?uploads")
        assert resp.status == 200
        root = fromstring(await resp.text())
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        upload_id = root.find("s3:UploadId", ns).text

        # Upload a part
        resp = await client.put(
            f"/bucket/file.bin?partNumber=1&uploadId={upload_id}",
            data=b"payload",
        )
        assert resp.status == 200

        # Complete first time
        mock_bridge.put_object.return_value = {"ETag": '"etag1"', "size": 7}
        resp1 = await client.post(
            f"/bucket/file.bin?uploadId={upload_id}",
            data=b"<CompleteMultipartUpload/>",
        )
        assert resp1.status == 200
        body1 = await resp1.text()
        assert "CompleteMultipartUploadResult" in body1

        # Complete second time (idempotent retry)
        resp2 = await client.post(
            f"/bucket/file.bin?uploadId={upload_id}",
            data=b"<CompleteMultipartUpload/>",
        )
        assert resp2.status == 200
        body2 = await resp2.text()
        assert body1 == body2

        # put_object should only have been called once
        assert mock_bridge.put_object.await_count == 1

    async def test_complete_after_failure_resets_and_retries(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """If CompleteMultipartUpload fails, a retry should re-attempt the upload."""
        # Initiate
        resp = await client.post("/bucket/file.bin?uploads")
        root = fromstring(await resp.text())
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        upload_id = root.find("s3:UploadId", ns).text

        # Upload a part
        resp = await client.put(
            f"/bucket/file.bin?partNumber=1&uploadId={upload_id}",
            data=b"data",
        )
        assert resp.status == 200

        # First complete fails
        mock_bridge.put_object.side_effect = RuntimeError("CAS error")
        resp = await client.post(
            f"/bucket/file.bin?uploadId={upload_id}",
            data=b"<CompleteMultipartUpload/>",
        )
        assert resp.status == 500

        # Re-upload the part (since parts were consumed during concatenation)
        resp = await client.put(
            f"/bucket/file.bin?partNumber=1&uploadId={upload_id}",
            data=b"data",
        )
        assert resp.status == 200

        # Retry complete -- should succeed now
        mock_bridge.put_object.side_effect = None
        mock_bridge.put_object.return_value = {"ETag": '"ok"', "size": 4}
        resp = await client.post(
            f"/bucket/file.bin?uploadId={upload_id}",
            data=b"<CompleteMultipartUpload/>",
        )
        assert resp.status == 200
        body = await resp.text()
        assert "CompleteMultipartUploadResult" in body

    async def test_upload_part_rejected_during_completing(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Parts should be rejected when the upload is in completing state."""
        # Initiate
        resp = await client.post("/bucket/file.bin?uploads")
        root = fromstring(await resp.text())
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        upload_id = root.find("s3:UploadId", ns).text

        # Upload a part
        resp = await client.put(
            f"/bucket/file.bin?partNumber=1&uploadId={upload_id}",
            data=b"data",
        )
        assert resp.status == 200

        # Make put_object block so we can test concurrent behavior
        complete_started = asyncio.Event()
        proceed = asyncio.Event()

        async def slow_put_object(*args, **kwargs):
            complete_started.set()
            await proceed.wait()
            return {"ETag": '"slow"', "size": 4}

        mock_bridge.put_object.side_effect = slow_put_object

        # Start complete in background
        complete_task = asyncio.create_task(
            client.post(
                f"/bucket/file.bin?uploadId={upload_id}",
                data=b"<CompleteMultipartUpload/>",
            )
        )
        await complete_started.wait()

        # Try to upload a part while completing -- should be rejected
        resp = await client.put(
            f"/bucket/file.bin?partNumber=2&uploadId={upload_id}",
            data=b"more-data",
        )
        assert resp.status == 409

        # Let complete finish
        proceed.set()
        resp = await complete_task
        assert resp.status == 200

    async def test_abort_rejected_during_completing(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Abort should be rejected when the upload is being completed."""
        # Initiate
        resp = await client.post("/bucket/file.bin?uploads")
        root = fromstring(await resp.text())
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        upload_id = root.find("s3:UploadId", ns).text

        # Upload a part
        resp = await client.put(
            f"/bucket/file.bin?partNumber=1&uploadId={upload_id}",
            data=b"data",
        )
        assert resp.status == 200

        # Make put_object block
        complete_started = asyncio.Event()
        proceed = asyncio.Event()

        async def slow_put_object(*args, **kwargs):
            complete_started.set()
            await proceed.wait()
            return {"ETag": '"slow"', "size": 4}

        mock_bridge.put_object.side_effect = slow_put_object

        # Start complete in background
        complete_task = asyncio.create_task(
            client.post(
                f"/bucket/file.bin?uploadId={upload_id}",
                data=b"<CompleteMultipartUpload/>",
            )
        )
        await complete_started.wait()

        # Try abort while completing -- should be rejected
        resp = await client.delete(f"/bucket/file.bin?uploadId={upload_id}")
        assert resp.status == 409

        # Let complete finish
        proceed.set()
        resp = await complete_task
        assert resp.status == 200

    async def test_concurrent_complete_waits_for_first(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Second CompleteMultipartUpload should wait for the first to finish."""
        # Initiate
        resp = await client.post("/bucket/file.bin?uploads")
        root = fromstring(await resp.text())
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        upload_id = root.find("s3:UploadId", ns).text

        # Upload a part
        resp = await client.put(
            f"/bucket/file.bin?partNumber=1&uploadId={upload_id}",
            data=b"data",
        )
        assert resp.status == 200

        # Make put_object block
        complete_started = asyncio.Event()
        proceed = asyncio.Event()

        async def slow_put_object(*args, **kwargs):
            complete_started.set()
            await proceed.wait()
            return {"ETag": '"concurrent"', "size": 4}

        mock_bridge.put_object.side_effect = slow_put_object

        # Start first complete
        task1 = asyncio.create_task(
            client.post(
                f"/bucket/file.bin?uploadId={upload_id}",
                data=b"<CompleteMultipartUpload/>",
            )
        )
        await complete_started.wait()

        # Start second complete -- should wait for first
        task2 = asyncio.create_task(
            client.post(
                f"/bucket/file.bin?uploadId={upload_id}",
                data=b"<CompleteMultipartUpload/>",
            )
        )

        # Give task2 a moment to reach the event.wait()
        await asyncio.sleep(0.05)
        assert not task2.done()

        # Let first complete finish
        proceed.set()

        resp1 = await task1
        resp2 = await task2

        assert resp1.status == 200
        assert resp2.status == 200

        body1 = await resp1.text()
        body2 = await resp2.text()
        assert body1 == body2

        # put_object should only have been called once
        assert mock_bridge.put_object.await_count == 1


class TestDeleteObjects:
    """Tests for S3 multi-object delete (POST /{bucket}?delete)."""

    async def test_delete_multiple_objects(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.delete_objects.return_value = (
            ["file1.txt", "file2.txt", "dir/file3.txt"],
            [],
        )
        body = """<?xml version="1.0" encoding="UTF-8"?>
        <Delete>
            <Object><Key>file1.txt</Key></Object>
            <Object><Key>file2.txt</Key></Object>
            <Object><Key>dir/file3.txt</Key></Object>
        </Delete>"""
        resp = await client.post("/my-bucket?delete", data=body.encode())
        assert resp.status == 200
        text = await resp.text()
        assert "DeleteResult" in text
        assert "file1.txt" in text
        assert "file2.txt" in text
        assert "dir/file3.txt" in text
        mock_bridge.delete_objects.assert_awaited_once_with(
            "my-bucket", ["file1.txt", "file2.txt", "dir/file3.txt"]
        )

    async def test_delete_objects_with_errors(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.delete_objects.return_value = (
            ["ok.txt"],
            [{"key": "bad.txt", "code": "AccessDenied", "message": "Access Denied"}],
        )
        body = """<Delete>
            <Object><Key>ok.txt</Key></Object>
            <Object><Key>bad.txt</Key></Object>
        </Delete>"""
        resp = await client.post("/my-bucket?delete", data=body.encode())
        assert resp.status == 200
        text = await resp.text()
        assert "ok.txt" in text
        assert "AccessDenied" in text

    async def test_delete_objects_malformed_xml(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await client.post("/my-bucket?delete", data=b"not xml")
        assert resp.status == 400
        text = await resp.text()
        assert "MalformedXML" in text

    async def test_delete_objects_no_keys(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        body = """<Delete></Delete>"""
        resp = await client.post("/my-bucket?delete", data=body.encode())
        assert resp.status == 400
        text = await resp.text()
        assert "MalformedXML" in text

    async def test_delete_objects_with_namespace(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Keys in S3 namespace should be parsed correctly."""
        mock_bridge.delete_objects.return_value = (["a.txt", "b.txt"], [])
        body = """<?xml version="1.0" encoding="UTF-8"?>
        <Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Object><Key>a.txt</Key></Object>
            <Object><Key>b.txt</Key></Object>
        </Delete>"""
        resp = await client.post("/bucket?delete", data=body.encode())
        assert resp.status == 200
        text = await resp.text()
        assert "a.txt" in text
        assert "b.txt" in text
