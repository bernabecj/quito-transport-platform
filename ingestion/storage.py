"""
Raw-zone storage backends.

The loaders address every file by an S3-style key such as
``raw/traffic/year=2026/month=10/day=13/hour=14/corridor_travel_time.parquet``.
The key carries the whole partition layout, so a backend only has to map a
key to a location. Two do:

  S3Storage     the production backend — ``s3://<bucket>/<key>``, via boto3.
                Lambda uses this; its behaviour is the boto3 code that used
                to live in each loader, unchanged.
  LocalStorage  a folder on disk, ``<root>/<key>``, for running and testing
                the loaders without an AWS account. The folder mirrors the
                bucket exactly, partition directories included.

Both expose the same three operations so the loaders do not care which one
they hold: ``put_parquet``, ``get_parquet`` and ``list_keys``. The last two
exist because traffic_loader reads the latest network snapshot back before
sampling; a write-only local backend would still leave it needing AWS.

``get_storage()`` picks the backend from the STORAGE_BACKEND environment
variable; the loaders call it and nothing else.
"""

import io
import os
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.constants import (
    LOCAL_STORAGE_ROOT,
    S3_BUCKET_NAME,
    STORAGE_BACKEND_LOCAL,
    STORAGE_BACKEND_S3,
    STORAGE_BACKEND_VAR,
)


def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialise exactly as the loaders always have: pyarrow, default options."""
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def _from_parquet_bytes(data: bytes) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(data))


class S3Storage:
    """Raw zone in an S3 bucket. ``uri(key)`` is ``s3://<bucket>/<key>``."""

    def __init__(self, bucket: str = S3_BUCKET_NAME, client=None):
        self.bucket = bucket
        self._client = client

    @property
    def client(self):
        # Created on first use, not at import: importing the loaders must not
        # require AWS credentials or a configured region.
        if self._client is None:
            self._client = boto3.client("s3")
        return self._client

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def put_parquet(self, df: pd.DataFrame, key: str) -> str:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=_to_parquet_bytes(df))
        return self.uri(key)

    def get_parquet(self, key: str) -> pd.DataFrame:
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return _from_parquet_bytes(body)

    def list_keys(self, prefix: str) -> list[str]:
        """Every key under ``prefix``, across all pages.

        A single ``list_objects_v2`` call returns at most 1,000 keys. The
        network snapshot is three files a day, so an unpaginated listing would
        stop seeing new days after about eleven months and ``read_latest_network``
        would silently pick a stale snapshot. The paginator follows the
        continuation tokens until the listing is complete.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        return [
            obj["Key"]
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            for obj in page.get("Contents", [])
        ]


class LocalStorage:
    """Raw zone in a folder. ``uri(key)`` is the absolute path of ``<root>/<key>``.

    Keys always use ``/`` as the separator, whatever the platform; pathlib
    splits them into path components, and ``list_keys`` joins them back with
    ``/`` so the loaders see the same strings they would get from S3.
    """

    def __init__(self, root: Path | str | None = None):
        # LOCAL_STORAGE_ROOT is looked up here, at call time, rather than as
        # a default argument (evaluated once at import), so tests can point
        # the backend at a temporary folder by patching the constant.
        self.root = Path(root) if root is not None else LOCAL_STORAGE_ROOT

    def uri(self, key: str) -> str:
        return str(self.root / key)

    def put_parquet(self, df: pd.DataFrame, key: str) -> str:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(_to_parquet_bytes(df))
        return str(path)

    def get_parquet(self, key: str) -> pd.DataFrame:
        return _from_parquet_bytes((self.root / key).read_bytes())

    def list_keys(self, prefix: str) -> list[str]:
        """Every file under ``prefix``, as keys relative to the root.

        Mirrors S3 prefix semantics: a prefix is a string match on the key, not
        a directory. ``raw/net`` therefore matches ``raw/network/...`` just as
        it would on S3.
        """
        if not self.root.is_dir():
            return []
        return sorted(
            path.relative_to(self.root).as_posix()
            for path in self.root.rglob("*")
            if path.is_file() and path.relative_to(self.root).as_posix().startswith(prefix)
        )


def get_storage() -> S3Storage | LocalStorage:
    """The backend selected by STORAGE_BACKEND.

    Unset, empty or ``s3`` gives the production bucket — the Lambdas set no
    such variable, so they are unaffected. ``local`` gives the folder under
    LOCAL_STORAGE_ROOT. Case and surrounding whitespace are ignored, so that
    ``STORAGE_BACKEND=Local `` in a hand-edited .env still works; anything
    else raises, because silently falling back to S3 is the one outcome a
    developer without AWS credentials must never get.
    """
    value = os.environ.get(STORAGE_BACKEND_VAR, "").strip().lower()
    if value in ("", STORAGE_BACKEND_S3):
        return S3Storage()
    if value == STORAGE_BACKEND_LOCAL:
        return LocalStorage()
    raise ValueError(
        f"{STORAGE_BACKEND_VAR}={os.environ[STORAGE_BACKEND_VAR]!r} is not a storage "
        f"backend; use {STORAGE_BACKEND_S3!r} or {STORAGE_BACKEND_LOCAL!r}."
    )
