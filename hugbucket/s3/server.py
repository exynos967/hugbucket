"""S3-compatible HTTP server using aiohttp.

Routes S3 REST API requests to the Bridge layer.
Supports: ListBuckets, CreateBucket, DeleteBucket, HeadBucket,
          ListObjectsV2, PutObject, CopyObject, GetObject, DeleteObject,
          DeleteObjects, HeadObject.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import unquote
from xml.etree.ElementTree import fromstring

import aiohttp
from aiohttp import web

from hugbucket.bridge import HFStorageBackend
from hugbucket.s3.xml_responses import (
    list_buckets_xml,
    list_objects_v2_xml,
    error_xml,
    delete_result_xml,
    copy_object_result_xml,
    get_bucket_location_xml,
    initiate_multipart_upload_xml,
    complete_multipart_upload_xml,
)

logger = logging.getLogger(__name__)

XML_CONTENT = "application/xml"

# Paths commonly requested by browsers/crawlers that are not valid S3 operations.
# Reject these early to avoid unnecessary API calls to HuggingFace.
_IGNORED_PATHS = {"favicon.ico", "robots.txt", "sitemap.xml", ".well-known"}


def _request_id() -> str:
    return uuid.uuid4().hex[:16].upper()


def _format_last_modified(ts: str | None) -> str:
    """Format an ISO timestamp as HTTP Last-Modified (RFC 7231).

    Example: "Wed, 11 Mar 2026 11:44:58 GMT"
    """
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
        except Exception:
            pass
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def _s3_error(status: int, code: str, message: str, resource: str = "") -> web.Response:
    return web.Response(
        status=status,
        content_type=XML_CONTENT,
        body=error_xml(code, message, resource, _request_id()),
    )


def _parse_bucket_key(request: web.Request) -> tuple[str, str]:
    """Extract bucket name and object key from request path.

    Path-style: /{bucket}/{key...}
    """
    path = unquote(request.path)
    # Remove leading /
    path = path.lstrip("/")
    if not path:
        return "", ""

    parts = path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


class S3Handler:
    """Handles S3 API requests."""

    # Multipart upload statuses
    _MP_COLLECTING = "collecting"  # accepting parts
    _MP_COMPLETING = "completing"  # bridge.put_object in progress
    _MP_COMPLETED = "completed"  # upload finished successfully
    _MP_FAILED = "failed"  # upload failed, can be retried

    def __init__(
        self,
        bridge: HFStorageBackend,
        multipart_upload_ttl: int = 86400,
    ) -> None:
        self.bridge = bridge
        self._multipart_upload_ttl = multipart_upload_ttl
        # In-memory multipart upload state:
        # upload_id -> {bucket, key, parts: {part_num: bytes},
        #               status, created_at, event, result_body, result_etag}
        self._multipart_uploads: dict[str, dict] = {}
        self._cleanup_task: asyncio.Task | None = None

    def setup_routes(self, app: web.Application) -> None:
        # Service-level
        app.router.add_route("GET", "/", self.handle_list_buckets)

        # Bucket + object operations — catch-all
        app.router.add_route("*", "/{path:.*}", self.handle_request)

        # Lifecycle hooks for multipart upload cleanup
        app.on_startup.append(self._start_cleanup)
        app.on_shutdown.append(self._stop_cleanup)

    async def _start_cleanup(self, app: web.Application) -> None:
        """Start background task that purges stale multipart uploads."""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def _stop_cleanup(self, app: web.Application) -> None:
        """Cancel the cleanup background task on shutdown."""
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def _cleanup_loop(self) -> None:
        """Periodically remove stale multipart uploads to prevent memory leaks."""
        while True:
            await asyncio.sleep(600)  # run every 10 minutes
            try:
                now = time.monotonic()
                stale = [
                    uid
                    for uid, info in self._multipart_uploads.items()
                    if (now - info["created_at"]) > self._multipart_upload_ttl
                    and info["status"] != self._MP_COMPLETING
                ]
                for uid in stale:
                    logger.info(f"Cleaning up stale multipart upload: {uid}")
                    del self._multipart_uploads[uid]
            except Exception:
                logger.exception("Error in multipart cleanup loop")

    async def handle_request(
        self, request: web.Request
    ) -> web.Response | web.StreamResponse:
        """Main router: dispatch based on method + path."""
        bucket, key = _parse_bucket_key(request)

        if not bucket:
            return await self.handle_list_buckets(request)

        if self._is_pool_mode(request) and not self._is_pool_bucket(request, bucket):
            return _s3_error(404, "NoSuchBucket", f"Bucket '{bucket}' not found")

        # Reject browser/crawler paths that are not valid S3 bucket names
        if not key and bucket in _IGNORED_PATHS:
            return _s3_error(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                resource=f"/{bucket}",
            )

        # Bucket-level operations (no key)
        if not key:
            query = request.query

            # GetBucketLocation: GET /{bucket}?location
            # S3 clients (e.g. S3 Browser) call this to discover the
            # bucket region before constructing presigned URLs.
            if "location" in query:
                return await self.handle_get_bucket_location(request, bucket)

            # ListObjectsV2
            if "list-type" in query:
                return await self.handle_list_objects_v2(request, bucket)

            # Multi-object delete: POST /{bucket}?delete
            if request.method == "POST" and "delete" in query:
                return await self.handle_delete_objects(request, bucket)

            # Bucket operations by method
            if request.method == "GET":
                # Default GET on bucket = ListObjectsV2
                return await self.handle_list_objects_v2(request, bucket)
            elif request.method == "PUT":
                return await self.handle_create_bucket(request, bucket)
            elif request.method == "DELETE":
                return await self.handle_delete_bucket(request, bucket)
            elif request.method == "HEAD":
                return await self.handle_head_bucket(request, bucket)
            else:
                return _s3_error(405, "MethodNotAllowed", "Method not allowed")

        # Object-level operations
        query = request.query

        if request.method == "POST":
            if "uploads" in query:
                return await self.handle_initiate_multipart(request, bucket, key)
            elif "uploadId" in query:
                return await self.handle_complete_multipart(request, bucket, key)
            else:
                return _s3_error(400, "InvalidRequest", "Unknown POST operation")
        elif request.method == "GET":
            return await self.handle_get_object(request, bucket, key)
        elif request.method == "PUT":
            if "partNumber" in query and "uploadId" in query:
                return await self.handle_upload_part(request, bucket, key)
            if "x-amz-copy-source" in request.headers:
                return await self.handle_copy_object(request, bucket, key)
            return await self.handle_put_object(request, bucket, key)
        elif request.method == "DELETE":
            if "uploadId" in query:
                return await self.handle_abort_multipart(request, bucket, key)
            return await self.handle_delete_object(request, bucket, key)
        elif request.method == "HEAD":
            return await self.handle_head_object(request, bucket, key)
        else:
            return _s3_error(405, "MethodNotAllowed", "Method not allowed")

    # ---- Bucket operations ----

    async def handle_list_buckets(self, request: web.Request) -> web.Response:
        try:
            config = request.app["config"]
            if config.pool_bucket_name:
                bucket_dicts = [{"name": config.pool_bucket_name, "creation_date": ""}]
            else:
                buckets = await self.bridge.list_buckets()
                bucket_dicts = [
                    {
                        "name": b.id.split("/")[-1] if "/" in b.id else b.id,
                        "creation_date": b.created_at,
                    }
                    for b in buckets
                ]
            body = list_buckets_xml(bucket_dicts)
            return web.Response(status=200, content_type=XML_CONTENT, body=body)
        except Exception as e:
            logger.exception("ListBuckets failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_create_bucket(
        self, request: web.Request, bucket: str
    ) -> web.Response:
        try:
            if self._is_pool_bucket(request, bucket):
                return web.Response(status=200)
            await self.bridge.create_bucket(bucket)
            return web.Response(status=200)
        except Exception as e:
            logger.exception("CreateBucket failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_delete_bucket(
        self, request: web.Request, bucket: str
    ) -> web.Response:
        try:
            if self._is_pool_bucket(request, bucket):
                return web.Response(status=204)
            await self.bridge.delete_bucket(bucket)
            return web.Response(status=204)
        except Exception as e:
            logger.exception("DeleteBucket failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_head_bucket(
        self, request: web.Request, bucket: str
    ) -> web.Response:
        try:
            if self._is_pool_bucket(request, bucket):
                return web.Response(status=200)
            info = await self.bridge.head_bucket(bucket)
            if info is None:
                return _s3_error(404, "NoSuchBucket", f"Bucket '{bucket}' not found")
            return web.Response(status=200)
        except Exception as e:
            logger.exception("HeadBucket failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_get_bucket_location(
        self, request: web.Request, bucket: str
    ) -> web.Response:
        """Handle GetBucketLocation (GET /{bucket}?location).

        S3 clients (e.g. S3 Browser, boto3) call this to discover the
        bucket region before constructing presigned URLs.  Without a
        proper response, clients may parse a ListObjectsV2 response
        instead and produce corrupted presigned URL credentials.
        """
        try:
            # Virtual pool bucket always exists — skip HF API probe
            if self._is_pool_bucket(request, bucket):
                body = get_bucket_location_xml("us-east-1")
                return web.Response(status=200, content_type=XML_CONTENT, body=body)

            info = await self.bridge.head_bucket(bucket)
            if info is None:
                return _s3_error(404, "NoSuchBucket", f"Bucket '{bucket}' not found")
            body = get_bucket_location_xml("us-east-1")
            return web.Response(status=200, content_type=XML_CONTENT, body=body)
        except Exception as e:
            logger.exception("GetBucketLocation failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_delete_objects(
        self, request: web.Request, bucket: str
    ) -> web.Response:
        """Handle S3 multi-object delete (POST /{bucket}?delete).

        Parses an XML body with a list of object keys and deletes them
        in a single batch call.
        """
        try:
            body = await request.read()
            try:
                root = fromstring(body)
            except Exception:
                return _s3_error(
                    400, "MalformedXML", "The XML you provided was not well-formed."
                )

            # Parse <Object><Key>...</Key></Object> elements.
            # The root tag may or may not have the S3 namespace.
            ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
            keys: list[str] = []
            for obj in root.findall("s3:Object", ns):
                key_el = obj.find("s3:Key", ns)
                if key_el is not None and key_el.text:
                    keys.append(key_el.text)
            # Also try without namespace (common in many clients)
            if not keys:
                for obj in root.findall("Object"):
                    key_el = obj.find("Key")
                    if key_el is not None and key_el.text:
                        keys.append(key_el.text)

            if not keys:
                return _s3_error(
                    400,
                    "MalformedXML",
                    "The XML you provided did not contain any Object/Key elements.",
                )

            if self._is_pool_bucket(request, bucket):
                deleted, errors = await self.bridge.pool_delete_objects(keys)
            else:
                deleted, errors = await self.bridge.delete_objects(bucket, keys)

            body_xml = delete_result_xml(deleted, errors or None)
            return web.Response(status=200, content_type=XML_CONTENT, body=body_xml)
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning("DeleteObjects: bucket '%s' not found upstream", bucket)
                return _s3_error(404, "NoSuchBucket", f"Bucket '{bucket}' not found")
            logger.exception("DeleteObjects failed")
            return _s3_error(500, "InternalError", str(e))
        except Exception as e:
            logger.exception("DeleteObjects failed")
            return _s3_error(500, "InternalError", str(e))

    # ---- Object listing ----

    def _is_pool_bucket(self, request: web.Request, bucket: str) -> bool:
        config = request.app["config"]
        return bool(config.pool_bucket_name and bucket == config.pool_bucket_name)

    def _is_pool_mode(self, request: web.Request) -> bool:
        config = request.app["config"]
        return bool(config.pool_bucket_name)

    async def handle_list_objects_v2(
        self, request: web.Request, bucket: str
    ) -> web.Response:
        try:
            prefix = request.query.get("prefix", "")
            delimiter = request.query.get("delimiter", "")
            max_keys = int(request.query.get("max-keys", "1000"))
            continuation = request.query.get("continuation-token", "")

            if self._is_pool_bucket(request, bucket):
                result = await self.bridge.pool_list_objects(
                    prefix=prefix, delimiter=delimiter,
                    max_keys=max_keys, continuation_token=continuation,
                )
            else:
                result = await self.bridge.list_objects(
                bucket,
                prefix=prefix,
                delimiter=delimiter,
                max_keys=max_keys,
                continuation_token=continuation,
            )

            contents = [
                {
                    "key": f.path,
                    "size": f.size,
                    "last_modified": f.mtime or f.uploaded_at,
                    "etag": f'"{f.xet_hash[:32]}"' if f.xet_hash else '""',
                }
                for f in result["contents"]
            ]

            body = list_objects_v2_xml(
                bucket=bucket,
                prefix=prefix,
                delimiter=delimiter,
                max_keys=max_keys,
                contents=contents,
                common_prefixes=result["common_prefixes"],
                is_truncated=result["is_truncated"],
                continuation_token=continuation,
                next_continuation_token=result.get("next_continuation_token"),
                key_count=len(contents),
            )
            return web.Response(status=200, content_type=XML_CONTENT, body=body)
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning("ListObjectsV2: bucket '%s' not found upstream", bucket)
                return _s3_error(404, "NoSuchBucket", f"Bucket '{bucket}' not found")
            logger.exception("ListObjectsV2 failed")
            return _s3_error(500, "InternalError", str(e))
        except Exception as e:
            logger.exception("ListObjectsV2 failed")
            return _s3_error(500, "InternalError", str(e))

    # ---- Object operations ----

    async def handle_put_object(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        try:
            data = await request.read()
            if self._is_pool_bucket(request, bucket):
                result = await self.bridge.pool_put_object(key, data)
            else:
                result = await self.bridge.put_object(bucket, key, data)
            return web.Response(
                status=200,
                headers={
                    "ETag": result["ETag"],
                    "Content-Length": "0",
                },
            )
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning("PutObject: bucket '%s' not found upstream", bucket)
                return _s3_error(404, "NoSuchBucket", f"Bucket '{bucket}' not found")
            logger.exception("PutObject failed")
            return _s3_error(500, "InternalError", str(e))
        except Exception as e:
            logger.exception("PutObject failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_copy_object(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        """Handle S3 CopyObject (PUT with x-amz-copy-source header).

        Parses the source bucket/key from the header and delegates to
        bridge.copy_object which performs a server-side "copy" by
        registering the new path with the same Xet content hash.
        """
        copy_source = ""
        try:
            # x-amz-copy-source format: /bucket/key or bucket/key
            copy_source = unquote(request.headers["x-amz-copy-source"])
            copy_source = copy_source.lstrip("/")
            parts = copy_source.split("/", 1)
            if len(parts) < 2 or not parts[1]:
                return _s3_error(
                    400,
                    "InvalidArgument",
                    "Invalid x-amz-copy-source header.",
                )
            src_bucket, src_key = parts[0], parts[1]

            if self._is_pool_bucket(request, bucket):
                pool_name = request.app["config"].pool_bucket_name
                if src_bucket != pool_name:
                    return _s3_error(
                        404,
                        "NoSuchBucket",
                        f"Bucket '{src_bucket}' not found",
                    )
                result = await self.bridge.pool_copy_object(src_key, key)
            else:
                result = await self.bridge.copy_object(src_bucket, src_key, bucket, key)

            body = copy_object_result_xml(
                etag=result["ETag"],
                last_modified=result.get("LastModified", ""),
            )
            return web.Response(status=200, content_type=XML_CONTENT, body=body)
        except FileNotFoundError:
            return _s3_error(
                404,
                "NoSuchKey",
                "The specified source key does not exist.",
                resource=copy_source,
            )
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning("CopyObject: bucket not found upstream")
                return _s3_error(
                    404, "NoSuchBucket", "The specified bucket does not exist."
                )
            logger.exception("CopyObject failed")
            return _s3_error(500, "InternalError", str(e))
        except Exception as e:
            logger.exception("CopyObject failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_get_object(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response | web.StreamResponse:
        try:
            # Get metadata first for headers and 404 check
            if self._is_pool_bucket(request, bucket):
                file_info = await self.bridge.pool_head_object(key)
            else:
                file_info = await self.bridge.head_object(bucket, key)
            if file_info is None:
                return _s3_error(
                    404,
                    "NoSuchKey",
                    "The specified key does not exist.",
                    resource=f"/{bucket}/{key}",
                )

            etag = (
                f'"{file_info.xet_hash[:32]}"'
                if file_info.xet_hash
                else f'"{hashlib.md5(b"").hexdigest()}"'
            )
            content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
            last_modified = _format_last_modified(
                file_info.mtime or file_info.uploaded_at if file_info else None
            )
            total_size = file_info.size

            # Parse Range header (RFC 7233)
            byte_range: tuple[int, int] | None = None
            range_header = request.headers.get("Range", "")
            if range_header and range_header.startswith("bytes="):
                range_spec = range_header[6:]  # strip "bytes="
                parts = range_spec.split("-")
                if range_spec.startswith("-"):
                    # Suffix range: bytes=-N  → last N bytes
                    suffix = int(parts[1])
                    if suffix <= 0:
                        return web.Response(status=416, headers={"Content-Range": f"bytes */{total_size}"})
                    start = max(0, total_size - suffix)
                    end = total_size - 1
                else:
                    start = int(parts[0]) if parts[0] else 0
                    end = int(parts[1]) if len(parts) > 1 and parts[1] else total_size - 1
                    end = min(end, total_size - 1)

                if start >= total_size or start > end:
                    return web.Response(status=416, headers={"Content-Range": f"bytes */{total_size}"})
                byte_range = (start, end)

            # Get the stream — bridge skips irrelevant xorbs when
            # byte_range is set and trims to the exact byte window.
            if self._is_pool_bucket(request, bucket):
                stream = await self.bridge.pool_get_object_stream(
                    key, file_info=file_info, byte_range=byte_range
                )
            else:
                stream = await self.bridge.get_object_stream(
                    bucket, key, file_info=file_info, byte_range=byte_range
                )
            if stream is None:
                return _s3_error(
                    404,
                    "NoSuchKey",
                    "The specified key does not exist.",
                    resource=f"/{bucket}/{key}",
                )

            if byte_range is not None:
                start, end = byte_range
                slice_length = end - start + 1
                response = web.StreamResponse(
                    status=206,
                    headers={
                        "ETag": etag,
                        "Content-Length": str(slice_length),
                        "Content-Range": f"bytes {start}-{end}/{total_size}",
                        "Accept-Ranges": "bytes",
                        "Last-Modified": last_modified,
                    },
                )
            else:
                response = web.StreamResponse(
                    status=200,
                    headers={
                        "ETag": etag,
                        "Content-Length": str(total_size),
                        "Accept-Ranges": "bytes",
                        "Last-Modified": last_modified,
                    },
                )

            response.content_type = content_type
            await response.prepare(request)

            async for chunk in stream:
                await response.write(chunk)

            await response.write_eof()
            return response
        except (ConnectionResetError, ConnectionError):
            logger.debug("GetObject: client disconnected for /%s/%s", bucket, key)
            return web.Response(status=499)  # nginx-style "client closed request"
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning("GetObject: bucket '%s' not found upstream", bucket)
                return _s3_error(404, "NoSuchBucket", f"Bucket '{bucket}' not found")
            logger.exception("GetObject failed")
            return _s3_error(500, "InternalError", str(e))
        except Exception as e:
            logger.exception("GetObject failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_delete_object(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        try:
            if self._is_pool_bucket(request, bucket):
                await self.bridge.pool_delete_object(key)
            else:
                await self.bridge.delete_object(bucket, key)
            return web.Response(status=204)
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning("DeleteObject: bucket '%s' not found upstream", bucket)
                return _s3_error(404, "NoSuchBucket", f"Bucket '{bucket}' not found")
            logger.exception("DeleteObject failed")
            return _s3_error(500, "InternalError", str(e))
        except Exception as e:
            logger.exception("DeleteObject failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_head_object(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        try:
            if self._is_pool_bucket(request, bucket):
                file_info = await self.bridge.pool_head_object(key)
            else:
                file_info = await self.bridge.head_object(bucket, key)
            if file_info is None:
                # Directory HEAD: S3 clients (e.g. S3 Browser) send HEAD on
                # folder keys (trailing slash) to check if a folder exists.
                # In real AWS S3 the console stores a 0-byte object for
                # folders; HugBucket uses a hidden .hugbucket_keep marker
                # instead, so we check for it here.
                if key.endswith("/"):
                    if self._is_pool_bucket(request, bucket):
                        exists = await self.bridge.pool_head_directory(key)
                    else:
                        exists = await self.bridge.head_directory(bucket, key)
                    if exists:
                        return web.Response(
                            status=200,
                            headers={
                                "Content-Length": "0",
                                "Content-Type": "application/x-directory",
                                "Last-Modified": _format_last_modified(None),
                            },
                        )

                return _s3_error(
                    404,
                    "NoSuchKey",
                    "The specified key does not exist.",
                    resource=f"/{bucket}/{key}",
                )

            etag = f'"{file_info.xet_hash[:32]}"' if file_info.xet_hash else '""'
            content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
            last_modified = _format_last_modified(
                file_info.mtime or file_info.uploaded_at
            )

            return web.Response(
                status=200,
                headers={
                    "Content-Length": str(file_info.size),
                    "ETag": etag,
                    "Content-Type": content_type,
                    "Accept-Ranges": "bytes",
                    "Last-Modified": last_modified,
                },
            )
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning("HeadObject: bucket '%s' not found upstream", bucket)
                return _s3_error(404, "NoSuchBucket", f"Bucket '{bucket}' not found")
            logger.exception("HeadObject failed")
            return _s3_error(500, "InternalError", str(e))
        except Exception as e:
            logger.exception("HeadObject failed")
            return _s3_error(500, "InternalError", str(e))

    # ---- Multipart upload operations ----

    async def handle_initiate_multipart(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        """Initiate a multipart upload. Returns an uploadId."""
        try:
            upload_id = uuid.uuid4().hex
            self._multipart_uploads[upload_id] = {
                "bucket": bucket,
                "key": key,
                "parts": {},
                "status": self._MP_COLLECTING,
                "created_at": time.monotonic(),
                "event": None,
                "result_body": None,
                "result_etag": None,
            }
            logger.info(
                f"InitiateMultipartUpload: bucket={bucket} key={key} uploadId={upload_id}"
            )
            body = initiate_multipart_upload_xml(bucket, key, upload_id)
            return web.Response(status=200, content_type=XML_CONTENT, body=body)
        except Exception as e:
            logger.exception("InitiateMultipartUpload failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_upload_part(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        """Upload a part for a multipart upload."""
        try:
            upload_id = request.query["uploadId"]
            part_number = int(request.query["partNumber"])

            upload = self._multipart_uploads.get(upload_id)
            if upload is None:
                return _s3_error(
                    404,
                    "NoSuchUpload",
                    "The specified upload does not exist.",
                    resource=f"/{bucket}/{key}",
                )

            if upload["status"] not in (self._MP_COLLECTING, self._MP_FAILED):
                return _s3_error(
                    409,
                    "InvalidRequest",
                    "Upload is no longer accepting parts.",
                    resource=f"/{bucket}/{key}",
                )

            # If the upload previously failed, reset to collecting so parts
            # can be re-uploaded before retrying completion.
            if upload["status"] == self._MP_FAILED:
                upload["status"] = self._MP_COLLECTING

            data = await request.read()
            etag = await asyncio.to_thread(lambda: f'"{hashlib.md5(data).hexdigest()}"')
            upload["parts"][part_number] = data

            logger.info(
                f"UploadPart: uploadId={upload_id} part={part_number} "
                f"size={len(data)} etag={etag}"
            )
            return web.Response(
                status=200,
                headers={"ETag": etag},
            )
        except Exception as e:
            logger.exception("UploadPart failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_complete_multipart(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        """Complete a multipart upload: concatenate parts and upload as one object.

        This method is idempotent: if the client retries after a timeout
        while the upload is still in progress, it waits for the result.
        If the upload already completed, it returns the cached response.
        If the upload previously failed, it resets state so the client
        can re-upload parts and retry.
        """
        try:
            upload_id = request.query["uploadId"]

            upload = self._multipart_uploads.get(upload_id)
            if upload is None:
                return _s3_error(
                    404,
                    "NoSuchUpload",
                    "The specified upload does not exist.",
                    resource=f"/{bucket}/{key}",
                )

            status = upload["status"]

            # Already completed — return cached result (idempotent retry)
            if status == self._MP_COMPLETED:
                logger.info(
                    f"CompleteMultipartUpload: uploadId={upload_id} "
                    f"returning cached result"
                )
                return web.Response(
                    status=200,
                    content_type=XML_CONTENT,
                    body=upload["result_body"],
                )

            # Currently being completed by another request — wait for it
            if status == self._MP_COMPLETING:
                event = upload.get("event")
                if event is not None:
                    logger.info(
                        f"CompleteMultipartUpload: uploadId={upload_id} "
                        f"waiting for in-progress completion"
                    )
                    await event.wait()

                    # Re-check status after waiting
                    if upload["status"] == self._MP_COMPLETED:
                        return web.Response(
                            status=200,
                            content_type=XML_CONTENT,
                            body=upload["result_body"],
                        )
                    # If it failed, fall through to the failed handler below
                    status = upload["status"]

            # Previously failed — reset so client can retry
            if status == self._MP_FAILED:
                logger.info(
                    f"CompleteMultipartUpload: uploadId={upload_id} "
                    f"retrying after previous failure"
                )
                upload["status"] = self._MP_COLLECTING
                status = self._MP_COLLECTING

            if status != self._MP_COLLECTING:
                return _s3_error(
                    409,
                    "InvalidRequest",
                    f"Upload is in unexpected state: {status}",
                    resource=f"/{bucket}/{key}",
                )

            parts = upload["parts"]
            if not parts:
                return _s3_error(
                    400,
                    "MalformedXML",
                    "You must specify at least one part.",
                )

            # Transition to completing — prevent concurrent processing
            upload["status"] = self._MP_COMPLETING
            event = asyncio.Event()
            upload["event"] = event

            try:
                # Concatenate parts in order, freeing each part as we go
                # to reduce peak memory from ~2x to ~1x file size.
                sorted_part_nums = sorted(parts.keys())
                buf = bytearray()
                for n in sorted_part_nums:
                    buf.extend(parts.pop(n))
                data = bytes(buf)
                del buf  # free the bytearray

                logger.info(
                    f"CompleteMultipartUpload: uploadId={upload_id} "
                    f"parts={len(sorted_part_nums)} total_size={len(data)}"
                )

                # Upload as regular object
                if self._is_pool_bucket(request, bucket):
                    result = await self.bridge.pool_put_object(key, data)
                else:
                    result = await self.bridge.put_object(bucket, key, data)

                etag = result["ETag"]
                location = f"/{bucket}/{key}"
                body = complete_multipart_upload_xml(location, bucket, key, etag)

                # Cache success for idempotent retries
                upload["status"] = self._MP_COMPLETED
                upload["result_body"] = body
                upload["result_etag"] = etag
                event.set()

                return web.Response(status=200, content_type=XML_CONTENT, body=body)

            except Exception:
                # Mark as failed so the upload can be retried
                upload["status"] = self._MP_FAILED
                upload["event"] = None
                event.set()
                raise

        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                logger.warning(
                    "CompleteMultipartUpload: bucket '%s' not found upstream", bucket
                )
                return _s3_error(404, "NoSuchBucket", f"Bucket '{bucket}' not found")
            logger.exception("CompleteMultipartUpload failed")
            return _s3_error(500, "InternalError", str(e))
        except Exception as e:
            logger.exception("CompleteMultipartUpload failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_abort_multipart(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        """Abort a multipart upload and discard uploaded parts."""
        try:
            upload_id = request.query["uploadId"]

            upload = self._multipart_uploads.get(upload_id)
            if upload is None:
                return _s3_error(
                    404,
                    "NoSuchUpload",
                    "The specified upload does not exist.",
                    resource=f"/{bucket}/{key}",
                )

            if upload["status"] == self._MP_COMPLETING:
                return _s3_error(
                    409,
                    "InvalidRequest",
                    "Cannot abort upload while completion is in progress.",
                    resource=f"/{bucket}/{key}",
                )

            del self._multipart_uploads[upload_id]
            logger.info(f"AbortMultipartUpload: uploadId={upload_id}")
            return web.Response(status=204)
        except Exception as e:
            logger.exception("AbortMultipartUpload failed")
            return _s3_error(500, "InternalError", str(e))
