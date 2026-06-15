"""Unit tests for Bridge layer (mocking Hub + CAS clients)."""

from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hugbucket.config import Config
from hugbucket.hub.client import BucketFile


@pytest.fixture
def config() -> Config:
    return Config(hf_namespace="testns")


@pytest.fixture
def mock_hub() -> MagicMock:
    hub = MagicMock()
    hub.batch_files = AsyncMock()
    hub.get_xet_write_token = AsyncMock()
    hub.get_paths_info = AsyncMock(return_value=[])
    hub.close = AsyncMock()
    return hub


@pytest.fixture
def mock_cas() -> MagicMock:
    cas = MagicMock()
    cas.close = AsyncMock()
    return cas


@pytest.fixture
def bridge(config: Config, mock_hub: MagicMock, mock_cas: MagicMock):
    from hugbucket.bridge import Bridge

    b = Bridge(config)
    b.hub = mock_hub
    b.cas = mock_cas
    return b


class TestDirectoryMarkers:
    """Test that folder-creation PUTs (trailing-slash, empty body) store a
    hidden placeholder file so empty folders appear in listings."""

    @staticmethod
    def _setup_xet_mocks(mock_hub: MagicMock, mock_cas: MagicMock) -> None:
        """Wire up the Xet write-token / CAS mocks for full upload path."""
        mock_hub.get_xet_write_token.return_value = MagicMock(
            endpoint="https://example.com",
            access_token="tok",
            expiration_unix_epoch=9999999999,
        )
        mock_cas.upload_xorb = AsyncMock()
        mock_cas.upload_shard = AsyncMock()

    async def test_folder_marker_returns_success(
        self, bridge, mock_hub: MagicMock, mock_cas: MagicMock
    ) -> None:
        self._setup_xet_mocks(mock_hub, mock_cas)
        result = await bridge.put_object("mybucket", "New Folder/", b"")
        assert "ETag" in result
        assert result["size"] == 0

    async def test_folder_marker_stores_placeholder(
        self, bridge, mock_hub: MagicMock, mock_cas: MagicMock
    ) -> None:
        """Directory markers must store a .hugbucket_keep placeholder via batch API."""
        self._setup_xet_mocks(mock_hub, mock_cas)
        await bridge.put_object("mybucket", "some/dir/", b"")
        mock_hub.batch_files.assert_awaited_once()
        call_args = mock_hub.batch_files.call_args
        add_list = call_args.kwargs.get("add") or call_args[1].get("add")
        assert len(add_list) == 1
        assert add_list[0]["path"] == "some/dir/.hugbucket_keep"

    async def test_folder_marker_uploads_to_xet(
        self, bridge, mock_hub: MagicMock, mock_cas: MagicMock
    ) -> None:
        """Directory markers must go through the full Xet CAS upload path."""
        self._setup_xet_mocks(mock_hub, mock_cas)
        await bridge.put_object("mybucket", "folder/", b"")
        mock_hub.get_xet_write_token.assert_awaited_once()
        mock_cas.upload_xorb.assert_awaited_once()
        mock_cas.upload_shard.assert_awaited_once()

    async def test_nested_folder_marker(
        self, bridge, mock_hub: MagicMock, mock_cas: MagicMock
    ) -> None:
        self._setup_xet_mocks(mock_hub, mock_cas)
        result = await bridge.put_object("mybucket", "a/b/c/d/", b"")
        assert result["size"] == 0
        mock_hub.batch_files.assert_awaited_once()
        call_args = mock_hub.batch_files.call_args
        add_list = call_args.kwargs.get("add") or call_args[1].get("add")
        assert add_list[0]["path"] == "a/b/c/d/.hugbucket_keep"

    async def test_non_folder_empty_file_still_calls_batch(
        self, bridge, mock_hub: MagicMock
    ) -> None:
        """An empty file WITHOUT trailing slash should still go through batch API."""
        await bridge.put_object("mybucket", "empty.txt", b"")
        mock_hub.batch_files.assert_awaited_once()

    async def test_folder_with_data_is_not_noop(
        self, bridge, mock_hub: MagicMock
    ) -> None:
        """A trailing-slash key WITH data is not a directory marker — it should
        go through the normal upload path (Xet + batch)."""
        mock_hub.get_xet_write_token.return_value = MagicMock(
            endpoint="https://example.com",
            access_token="tok",
            expiration_unix_epoch=9999999999,
        )
        mock_cas = bridge.cas
        mock_cas.upload_xorb = AsyncMock()
        mock_cas.upload_shard = AsyncMock()

        await bridge.put_object("mybucket", "weird-key/", b"some data")
        # Should have uploaded via Xet
        mock_hub.get_xet_write_token.assert_awaited_once()
        mock_hub.batch_files.assert_awaited_once()


class TestCopyObject:
    """Test bridge.copy_object — server-side copy via xetHash reuse."""

    async def test_copy_registers_new_path_with_same_hash(
        self, bridge, mock_hub: MagicMock
    ) -> None:
        """copy_object should register the dest path with the source's xetHash."""
        src_file = BucketFile(
            type="file",
            path="src.txt",
            size=100,
            xet_hash="a" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        mock_hub.get_paths_info.return_value = [src_file]

        result = await bridge.copy_object("mybucket", "src.txt", "mybucket", "dst.txt")

        assert "ETag" in result
        assert "LastModified" in result

        # Verify batch_files was called with the same xetHash
        mock_hub.batch_files.assert_awaited_once()
        call_args = mock_hub.batch_files.call_args
        add_list = call_args.kwargs.get("add") or call_args[1].get("add")
        assert len(add_list) == 1
        assert add_list[0]["path"] == "dst.txt"
        assert add_list[0]["xetHash"] == "a" * 64

    async def test_copy_does_not_download_data(
        self, bridge, mock_hub: MagicMock, mock_cas: MagicMock
    ) -> None:
        """copy_object must NOT download the file — only metadata lookup + batch."""
        src_file = BucketFile(
            type="file",
            path="big.bin",
            size=1_000_000_000,
            xet_hash="b" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        mock_hub.get_paths_info.return_value = [src_file]

        await bridge.copy_object("mybucket", "big.bin", "mybucket", "big-copy.bin")

        # No Xet read/write token, no CAS operations
        mock_hub.get_xet_write_token.assert_not_awaited()

    async def test_copy_source_not_found_raises(
        self, bridge, mock_hub: MagicMock
    ) -> None:
        """copy_object should raise FileNotFoundError if source doesn't exist."""
        mock_hub.get_paths_info.return_value = []

        with pytest.raises(FileNotFoundError):
            await bridge.copy_object("mybucket", "missing.txt", "mybucket", "dst.txt")
        mock_hub.batch_files.assert_not_awaited()

    async def test_copy_cross_bucket(self, bridge, mock_hub: MagicMock) -> None:
        """copy_object should work across different buckets."""
        src_file = BucketFile(
            type="file",
            path="data.bin",
            size=500,
            xet_hash="c" * 64,
            mtime="2026-06-01T00:00:00Z",
        )
        mock_hub.get_paths_info.return_value = [src_file]

        result = await bridge.copy_object(
            "src-bucket", "data.bin", "dst-bucket", "copied.bin"
        )
        assert result["ETag"] == f'"{"c" * 32}"'

        # Verify get_paths_info used src bucket, batch_files used dst bucket
        get_call = mock_hub.get_paths_info.call_args
        assert "src-bucket" in get_call[0][0]

        batch_call = mock_hub.batch_files.call_args
        assert "dst-bucket" in batch_call[0][0]

    async def test_copy_preserves_content_type_from_dest_key(
        self, bridge, mock_hub: MagicMock
    ) -> None:
        """copy_object should guess content type from the destination key."""
        src_file = BucketFile(
            type="file",
            path="photo.dat",
            size=1024,
            xet_hash="d" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        mock_hub.get_paths_info.return_value = [src_file]

        await bridge.copy_object("mybucket", "photo.dat", "mybucket", "photo.jpg")

        call_args = mock_hub.batch_files.call_args
        add_list = call_args.kwargs.get("add") or call_args[1].get("add")
        assert add_list[0]["contentType"] == "image/jpeg"
