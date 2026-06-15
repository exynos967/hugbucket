"""S3 XML response builders.

S3 API returns XML responses. This module builds them.
"""

from __future__ import annotations

from datetime import datetime, timezone
from xml.etree.ElementTree import Element, SubElement, tostring

S3_XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _make_root(tag: str) -> Element:
    return Element(tag, xmlns=S3_XMLNS)


def _add_text(parent: Element, tag: str, text: str) -> Element:
    el = SubElement(parent, tag)
    el.text = text
    return el


def _iso_time(ts: str | None = None) -> str:
    """Format time as S3-style ISO 8601."""
    if ts:
        return ts
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def to_xml_bytes(root: Element) -> bytes:
    """Serialize Element to XML bytes with declaration."""
    return (
        b'<?xml version="1.0" encoding="UTF-8"?>\n'
        + tostring(root, encoding="unicode").encode()
    )


# ---- Response builders ----


def list_buckets_xml(
    buckets: list[dict],
    owner_id: str = "hugbucket",
    owner_display: str = "hugbucket",
) -> bytes:
    """Build ListAllMyBucketsResult XML.

    buckets: [{"name": ..., "creation_date": ...}, ...]
    """
    root = _make_root("ListAllMyBucketsResult")

    owner = SubElement(root, "Owner")
    _add_text(owner, "ID", owner_id)
    _add_text(owner, "DisplayName", owner_display)

    bl = SubElement(root, "Buckets")
    for b in buckets:
        be = SubElement(bl, "Bucket")
        _add_text(be, "Name", b["name"])
        _add_text(be, "CreationDate", _iso_time(b.get("creation_date")))

    return to_xml_bytes(root)


def list_objects_v2_xml(
    bucket: str,
    prefix: str,
    delimiter: str,
    max_keys: int,
    contents: list[dict],
    common_prefixes: list[str],
    is_truncated: bool,
    continuation_token: str = "",
    next_continuation_token: str | None = None,
    key_count: int = 0,
) -> bytes:
    """Build ListBucketResult XML (ListObjectsV2)."""
    root = _make_root("ListBucketResult")

    _add_text(root, "Name", bucket)
    _add_text(root, "Prefix", prefix)
    _add_text(root, "MaxKeys", str(max_keys))
    _add_text(root, "KeyCount", str(key_count or len(contents)))
    _add_text(root, "IsTruncated", "true" if is_truncated else "false")

    if delimiter:
        _add_text(root, "Delimiter", delimiter)
    if continuation_token:
        _add_text(root, "ContinuationToken", continuation_token)
    if next_continuation_token:
        _add_text(root, "NextContinuationToken", next_continuation_token)

    for obj in contents:
        c = SubElement(root, "Contents")
        _add_text(c, "Key", obj["key"])
        _add_text(c, "LastModified", _iso_time(obj.get("last_modified")))
        _add_text(c, "ETag", obj.get("etag", ""))
        _add_text(c, "Size", str(obj.get("size", 0)))
        _add_text(c, "StorageClass", "STANDARD")

    for cp in common_prefixes:
        cpe = SubElement(root, "CommonPrefixes")
        _add_text(cpe, "Prefix", cp)

    return to_xml_bytes(root)


def error_xml(
    code: str,
    message: str,
    resource: str = "",
    request_id: str = "",
    host_id: str = "",
    extra: dict[str, str] | None = None,
) -> bytes:
    """Build S3 Error XML.

    AWS S3 error responses do **not** carry an ``xmlns`` attribute
    (unlike success responses such as ``ListBucketResult``).

    *extra* — optional dict of additional child elements specific to
    the error type (e.g. ``AWSAccessKeyId``, ``StringToSign``).
    """
    # No xmlns on <Error> — matches real AWS behaviour
    root = Element("Error")
    _add_text(root, "Code", code)
    _add_text(root, "Message", message)
    if resource:
        _add_text(root, "Resource", resource)
    if extra:
        for k, v in extra.items():
            _add_text(root, k, v)
    _add_text(root, "RequestId", request_id or "hugbucket")
    _add_text(root, "HostId", host_id or "hugbucket")
    return to_xml_bytes(root)


def get_bucket_location_xml(location: str = "us-east-1") -> bytes:
    """Build GetBucketLocationResult XML.

    S3 clients (e.g. S3 Browser) call ``GET /{bucket}?location`` to
    discover the bucket region for presigned-URL construction.

    AWS omits ``xmlns`` for the default us-east-1 region and only
    includes it for non-default regions.  We mirror that behaviour.
    """
    if location == "us-east-1":
        root = Element("LocationConstraint")
    else:
        root = _make_root("LocationConstraint")
    root.text = location
    return to_xml_bytes(root)


def delete_result_xml(deleted: list[str], errors: list[dict] | None = None) -> bytes:
    """Build DeleteResult XML for multi-object delete."""
    root = _make_root("DeleteResult")
    for key in deleted:
        d = SubElement(root, "Deleted")
        _add_text(d, "Key", key)
    if errors:
        for err in errors:
            e = SubElement(root, "Error")
            _add_text(e, "Key", err["key"])
            _add_text(e, "Code", err.get("code", "InternalError"))
            _add_text(e, "Message", err.get("message", ""))
    return to_xml_bytes(root)


def copy_object_result_xml(etag: str, last_modified: str = "") -> bytes:
    """Build CopyObjectResult XML."""
    root = _make_root("CopyObjectResult")
    _add_text(root, "ETag", etag)
    _add_text(root, "LastModified", _iso_time(last_modified or None))
    return to_xml_bytes(root)


def initiate_multipart_upload_xml(bucket: str, key: str, upload_id: str) -> bytes:
    """Build InitiateMultipartUploadResult XML."""
    root = _make_root("InitiateMultipartUploadResult")
    _add_text(root, "Bucket", bucket)
    _add_text(root, "Key", key)
    _add_text(root, "UploadId", upload_id)
    return to_xml_bytes(root)


def complete_multipart_upload_xml(
    location: str, bucket: str, key: str, etag: str
) -> bytes:
    """Build CompleteMultipartUploadResult XML."""
    root = _make_root("CompleteMultipartUploadResult")
    _add_text(root, "Location", location)
    _add_text(root, "Bucket", bucket)
    _add_text(root, "Key", key)
    _add_text(root, "ETag", etag)
    return to_xml_bytes(root)
