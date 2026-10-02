"""Turn a docUrl into the object key inside the source bucket.

Example:
  https://storage.apps.btpnsyariah.com/images/DOMA/8973/W076808973/W07680897314/W07680897314_KLAUSUL_AKAD.jpg
  -> bucket "images", key "DOMA/8973/W076808973/W07680897314/W07680897314_KLAUSUL_AKAD.jpg"
"""
from __future__ import annotations

from typing import Optional
from urllib.parse import unquote, urlsplit


class InvalidDocUrl(ValueError):
    pass


def parse_doc_url(url: Optional[str], bucket: str, expected_host: Optional[str] = None) -> str:
    if url is None or not str(url).strip():
        raise InvalidDocUrl("empty docUrl")
    parts = urlsplit(str(url).strip())
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise InvalidDocUrl("docUrl is not an http(s) URL")
    if expected_host and parts.hostname.lower() != expected_host.lower():
        raise InvalidDocUrl(f"unexpected host {parts.hostname!r}")

    path = unquote(parts.path)
    prefix = f"/{bucket}/"
    if not path.startswith(prefix):
        raise InvalidDocUrl(f"path does not start with {prefix!r}")
    key = path[len(prefix):]
    if not key or key.endswith("/"):
        raise InvalidDocUrl("docUrl points to a folder, not an object")
    if any(seg in ("", ".", "..") for seg in key.split("/")):
        raise InvalidDocUrl("docUrl path contains empty or relative segments")
    return key
