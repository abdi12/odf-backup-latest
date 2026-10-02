"""boto3 clients for ODF (source) and MinIO (destination)."""
from __future__ import annotations

from typing import Union

import boto3
from botocore.config import Config


def make_client(endpoint: str, access_key: str, secret_key: str, region: str,
                verify: Union[bool, str], max_pool: int):
    cfg = Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},      # ODF RGW/NooBaa and MinIO use path-style
        retries={"max_attempts": 5, "mode": "standard"},
        max_pool_connections=max_pool,
        connect_timeout=10,
        read_timeout=60,
        # boto3 >= 1.36 sends CRC checksums by default; many S3-compatible
        # backends (older MinIO, RGW) reject them. Only send when required.
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )
    return boto3.session.Session().client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        verify=verify,
        config=cfg,
    )
