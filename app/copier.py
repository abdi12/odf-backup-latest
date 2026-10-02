"""Copy one object ODF -> MinIO, idempotently."""
from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from typing import Optional

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import BotoCoreError, ClientError

INMEM_LIMIT = 8 * 1024 * 1024  # objects are 200KB-1MB; larger ones are streamed (bounds pod memory)
_TRANSFER = TransferConfig(multipart_threshold=16 * 1024 * 1024, multipart_chunksize=16 * 1024 * 1024)
NOT_FOUND = {"404", "NoSuchKey", "NotFound"}

COPIED, SKIPPED, SOURCE_MISSING, FAILED, DEFERRED = (
    "copied", "skipped", "source_missing", "failed", "deferred")


@dataclass
class CopyResult:
    key: str
    status: str
    size: int = 0
    etag: Optional[str] = None
    error: Optional[str] = None


def error_code(err: ClientError) -> str:
    return str(err.response.get("Error", {}).get("Code", ""))


def head_size(dst, bucket: str, key: str) -> Optional[int]:
    """Size of an object in the destination, or None if it isn't there."""
    try:
        return int(dst.head_object(Bucket=bucket, Key=key).get("ContentLength", 0))
    except ClientError as e:
        if error_code(e) in NOT_FOUND:
            return None
        raise


def copy_object(src, dst, src_bucket: str, dst_bucket: str, key: str, dst_prefix: str = "") -> CopyResult:
    dst_key = f"{dst_prefix}{key}"
    try:
        # 1) Already in MinIO? (covers restarts, overlap re-scans, lost local state)
        try:
            head = dst.head_object(Bucket=dst_bucket, Key=dst_key)
            etag = (head.get("Metadata") or {}).get("source-etag") or \
                str(head.get("ETag", "")).strip('"')
            return CopyResult(key, SKIPPED, int(head.get("ContentLength", 0)), etag or None)
        except ClientError as e:
            if error_code(e) not in NOT_FOUND:
                raise

        # 2) Read from ODF
        try:
            obj = src.get_object(Bucket=src_bucket, Key=key)
        except ClientError as e:
            if error_code(e) in NOT_FOUND:
                return CopyResult(key, SOURCE_MISSING, error="object not found in source bucket")
            raise

        size = int(obj["ContentLength"])
        src_etag = str(obj.get("ETag", "")).strip('"')
        metadata = dict(obj.get("Metadata") or {})
        metadata["source-etag"] = src_etag
        metadata["source-bucket"] = src_bucket
        if obj.get("LastModified"):
            metadata["source-last-modified"] = obj["LastModified"].isoformat()
        extra = {"Metadata": metadata,
                 "ContentType": obj.get("ContentType") or "application/octet-stream"}

        # 3) Write to MinIO, integrity-checked
        if size <= INMEM_LIMIT:
            body = obj["Body"].read()
            if len(body) != size:
                raise IOError(f"short read: got {len(body)} of {size} bytes")
            md5 = base64.b64encode(hashlib.md5(body).digest()).decode()
            dst.put_object(Bucket=dst_bucket, Key=dst_key, Body=body, ContentMD5=md5, **extra)
        else:
            dst.upload_fileobj(obj["Body"], dst_bucket, dst_key, ExtraArgs=extra, Config=_TRANSFER)
            if head_size(dst, dst_bucket, dst_key) != size:
                raise IOError("size mismatch after multipart upload")
        return CopyResult(key, COPIED, size, src_etag or None)

    except (ClientError, BotoCoreError, IOError, OSError) as e:
        return CopyResult(key, FAILED, error=f"{type(e).__name__}: {e}"[:500])
