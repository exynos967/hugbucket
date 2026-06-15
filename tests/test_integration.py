"""Integration tests against live HF API.

These tests require HF_TOKEN env var.
Run with: HF_TOKEN=hf_xxx uv run pytest -m integration
Skip with: uv run pytest -m 'not integration'
"""

from __future__ import annotations

import os
import hashlib
import tempfile
import time

import pytest

from hugbucket.config import Config
from hugbucket.bridge import Bridge


@pytest.fixture
def config(hf_token: str) -> Config:
    return Config(
        hf_namespace=os.environ.get("HF_NAMESPACE", "ninavacabsa"),
    )


@pytest.fixture
async def bridge(config: Config, hf_token: str):
    """Create a Bridge wired to a single-token pool (integration test)."""
    from hugbucket.admin.store import ConfigStore, TokenConfig
    from hugbucket.admin.token_pool import TokenPool

    # Create temp tokens.json with the env token
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
        f.write('{"tokens":[{"token":"' + hf_token + '","label":"test","namespace":"' + config.hf_namespace + '","healthy":true,"last_checked":0}]}')
        tmp_path = f.name

    store = ConfigStore(tmp_path)
    pool = TokenPool(store)
    config.tokens_file = tmp_path

    b = Bridge(config=config, _token_pool=pool)
    yield b
    await b.close()
    os.unlink(tmp_path)


# Unique bucket name per test run to avoid collisions
        finally:
            await bridge.delete_bucket(bucket_name)

    async def test_list_buckets(self, bridge: Bridge) -> None:
        buckets = await bridge.list_buckets()
        assert isinstance(buckets, list)


@pytest.mark.integration
class TestUploadDownload:
    """Test file upload/download with integrity verification."""

    async def test_small_file_roundtrip(self, bridge: Bridge) -> None:
        """Upload and download a 10KB file, verify byte-identical."""
        bucket_name = f"pytest-rt-{int(time.time()) % 100000}"
        try:
            await bridge.create_bucket(bucket_name)

            data = os.urandom(10 * 1024)
            key = "test-small.bin"

            result = await bridge.put_object(bucket_name, key, data)
            assert "ETag" in result

            downloaded = await bridge.get_object(bucket_name, key)
            assert downloaded is not None
            assert len(downloaded) == len(data)
            assert (
                hashlib.sha256(downloaded).hexdigest()
                == hashlib.sha256(data).hexdigest()
            )

            # Clean up file
            await bridge.delete_object(bucket_name, key)
        finally:
            await bridge.delete_bucket(bucket_name)

    async def test_multi_chunk_file_roundtrip(self, bridge: Bridge) -> None:
        """Upload and download a 200KB file (multiple CDC chunks)."""
        bucket_name = f"pytest-mc-{int(time.time()) % 100000}"
        try:
            await bridge.create_bucket(bucket_name)

            data = os.urandom(200 * 1024)
            key = "test-multi-chunk.bin"

            result = await bridge.put_object(bucket_name, key, data)
            assert "ETag" in result

            downloaded = await bridge.get_object(bucket_name, key)
            assert downloaded is not None
            assert downloaded == data

            await bridge.delete_object(bucket_name, key)
        finally:
            await bridge.delete_bucket(bucket_name)

    async def test_head_object(self, bridge: Bridge) -> None:
        """Test object metadata retrieval."""
        bucket_name = f"pytest-ho-{int(time.time()) % 100000}"
        try:
            await bridge.create_bucket(bucket_name)

            data = os.urandom(5 * 1024)
            key = "test-head.bin"
            await bridge.put_object(bucket_name, key, data)

            info = await bridge.head_object(bucket_name, key)
            assert info is not None
            assert info.path == key
            assert info.size == len(data)

            await bridge.delete_object(bucket_name, key)
        finally:
            await bridge.delete_bucket(bucket_name)

    async def test_list_objects(self, bridge: Bridge) -> None:
        """Test object listing with prefix."""
        bucket_name = f"pytest-lo-{int(time.time()) % 100000}"
        try:
            await bridge.create_bucket(bucket_name)

            # Upload two files
            await bridge.put_object(bucket_name, "dir/a.txt", b"aaa")
            await bridge.put_object(bucket_name, "dir/b.txt", b"bbb")
            await bridge.put_object(bucket_name, "other.txt", b"other")

            # List with prefix
            result = await bridge.list_objects(bucket_name, prefix="dir/")
            contents = result["contents"]
            assert len(contents) >= 2
            keys = {f.path for f in contents}
            assert "dir/a.txt" in keys
            assert "dir/b.txt" in keys

            # List with delimiter
            result = await bridge.list_objects(bucket_name, delimiter="/")
            assert "dir/" in result["common_prefixes"]

            # Clean up
            await bridge.delete_object(bucket_name, "dir/a.txt")
            await bridge.delete_object(bucket_name, "dir/b.txt")
            await bridge.delete_object(bucket_name, "other.txt")
        finally:
            await bridge.delete_bucket(bucket_name)

    async def test_delete_nonexistent_returns_none(self, bridge: Bridge) -> None:
        """head_object on missing key returns None."""
        bucket_name = f"pytest-dn-{int(time.time()) % 100000}"
        try:
            await bridge.create_bucket(bucket_name)
            info = await bridge.head_object(bucket_name, "does-not-exist.txt")
            # Should be None or empty list
            assert info is None or (isinstance(info, list) and len(info) == 0)
        finally:
            await bridge.delete_bucket(bucket_name)


@pytest.mark.integration
class TestDirectoryMarker:
    """Test folder creation (directory marker) against live HF API."""

    async def test_create_folder_marker(self, bridge: Bridge) -> None:
        """PUT with trailing-slash empty body should store a placeholder file."""
        bucket_name = f"pytest-dm-{int(time.time()) % 100000}"
        try:
            await bridge.create_bucket(bucket_name)

            # Create a folder marker — stores .hugbucket_keep placeholder
            result = await bridge.put_object(bucket_name, "New Folder/", b"")
            assert "ETag" in result
            assert result["size"] == 0

            # The placeholder should exist but be hidden from listings
            info = await bridge.head_object(bucket_name, "New Folder/.hugbucket_keep")
            assert info is not None

            # The folder itself should appear as a common prefix in listings
            listing = await bridge.list_objects(bucket_name, delimiter="/")
            assert "New Folder/" in listing["common_prefixes"]
            # The placeholder must NOT appear in contents
            content_keys = {f.path for f in listing["contents"]}
            assert "New Folder/.hugbucket_keep" not in content_keys

        finally:
            await bridge.delete_bucket(bucket_name)

    async def test_files_under_folder_visible_in_listing(self, bridge: Bridge) -> None:
        """Creating a folder marker then uploading files under it should list correctly."""
        bucket_name = f"pytest-df-{int(time.time()) % 100000}"
        try:
            await bridge.create_bucket(bucket_name)

            # Create folder marker (stores placeholder), then real files inside
            await bridge.put_object(bucket_name, "data/", b"")
            await bridge.put_object(bucket_name, "data/file1.txt", b"hello")
            await bridge.put_object(bucket_name, "data/file2.txt", b"world")

            # List with prefix — should see the files but not the placeholder
            result = await bridge.list_objects(bucket_name, prefix="data/")
            keys = {f.path for f in result["contents"]}
            assert "data/file1.txt" in keys
            assert "data/file2.txt" in keys
            assert "data/.hugbucket_keep" not in keys

            # Clean up (delete_object on dir/ also deletes the placeholder)
            await bridge.delete_object(bucket_name, "data/file1.txt")
            await bridge.delete_object(bucket_name, "data/file2.txt")
            await bridge.delete_object(bucket_name, "data/")
        finally:
            await bridge.delete_bucket(bucket_name)


@pytest.mark.integration
class TestCopyObject:
    """Test copy_object and rename (copy+delete) against live HF API."""

    async def test_copy_file_same_bucket(self, bridge: Bridge) -> None:
        """Copy a file within the same bucket — data should be identical."""
        bucket_name = f"pytest-cp-{int(time.time()) % 100000}"
        try:
            await bridge.create_bucket(bucket_name)

            # Upload original
            data = os.urandom(5 * 1024)
            await bridge.put_object(bucket_name, "original.bin", data)

            # Copy
            result = await bridge.copy_object(
                bucket_name, "original.bin", bucket_name, "copied.bin"
            )
            assert "ETag" in result

            # Verify copied file exists with same size
            info = await bridge.head_object(bucket_name, "copied.bin")
            assert info is not None
            assert info.size == len(data)

            # Download and verify identical content
            downloaded = await bridge.get_object(bucket_name, "copied.bin")
            assert downloaded == data

            # Clean up
            await bridge.delete_object(bucket_name, "original.bin")
            await bridge.delete_object(bucket_name, "copied.bin")
        finally:
            await bridge.delete_bucket(bucket_name)

    async def test_rename_file(self, bridge: Bridge) -> None:
        """Rename = copy + delete. The old key should disappear, new key should exist."""
        bucket_name = f"pytest-rn-{int(time.time()) % 100000}"
        try:
            await bridge.create_bucket(bucket_name)

            # Upload
            data = b"rename me please"
            await bridge.put_object(bucket_name, "old-name.txt", data)

            # Rename: copy to new name, then delete old
            await bridge.copy_object(
                bucket_name, "old-name.txt", bucket_name, "new-name.txt"
            )
            await bridge.delete_object(bucket_name, "old-name.txt")

            # Old key should be gone
            old_info = await bridge.head_object(bucket_name, "old-name.txt")
            assert old_info is None

            # New key should exist with same data
            new_info = await bridge.head_object(bucket_name, "new-name.txt")
            assert new_info is not None
            assert new_info.size == len(data)

            downloaded = await bridge.get_object(bucket_name, "new-name.txt")
            assert downloaded == data

            # Clean up
            await bridge.delete_object(bucket_name, "new-name.txt")
        finally:
            await bridge.delete_bucket(bucket_name)

    async def test_copy_nonexistent_raises(self, bridge: Bridge) -> None:
        """Copying a nonexistent source should raise FileNotFoundError."""
        bucket_name = f"pytest-cn-{int(time.time()) % 100000}"
        try:
            await bridge.create_bucket(bucket_name)

            with pytest.raises(FileNotFoundError):
                await bridge.copy_object(
                    bucket_name, "ghost.txt", bucket_name, "dst.txt"
                )
        finally:
            await bridge.delete_bucket(bucket_name)

    async def test_rename_into_subfolder(self, bridge: Bridge) -> None:
        """Rename (move) a file into a subfolder path."""
        bucket_name = f"pytest-rf-{int(time.time()) % 100000}"
        try:
            await bridge.create_bucket(bucket_name)

            data = b"moving to subfolder"
            await bridge.put_object(bucket_name, "root-file.txt", data)

            # Move into subfolder
            await bridge.copy_object(
                bucket_name,
                "root-file.txt",
                bucket_name,
                "subfolder/root-file.txt",
            )
            await bridge.delete_object(bucket_name, "root-file.txt")

            # Verify
            old = await bridge.head_object(bucket_name, "root-file.txt")
            assert old is None

            new = await bridge.head_object(bucket_name, "subfolder/root-file.txt")
            assert new is not None
            assert new.size == len(data)

            # List with delimiter to verify subfolder appears
            result = await bridge.list_objects(bucket_name, delimiter="/")
            assert "subfolder/" in result["common_prefixes"]

            # Clean up
            await bridge.delete_object(bucket_name, "subfolder/root-file.txt")
        finally:
            await bridge.delete_bucket(bucket_name)
