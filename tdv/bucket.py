"""
Hugging Face Storage Bucket (`repo_type="bucket"`) client wrapper with retries
and safe overwrite-merge semantics.

Uses `huggingface_hub` >= 1.5.0 Bucket APIs:
- `batch_bucket_files`
- `download_bucket_files`
- `list_bucket_tree`
- `get_bucket_paths_info`
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
from huggingface_hub import (
    batch_bucket_files,
    download_bucket_files,
    get_bucket_paths_info,
    list_bucket_tree,
)

from tdv.config import DEFAULT_HF_BUCKET_ID, get_chunk_bucket_path
from tdv.crypto import decrypt_dataframe_parquet


class BucketClient:
    """Thread-safe wrapper around Hugging Face Storage Bucket operations."""

    def __init__(
        self,
        bucket_id: str | None = None,
        token: str | None = None,
        max_retries: int = 5,
        base_backoff: float = 2.0,
    ) -> None:
        self.bucket_id = (
            bucket_id
            or os.environ.get("HF_BUCKET_ID", "").strip()
            or os.environ.get("HF_REPO_ID", "").strip()
            or DEFAULT_HF_BUCKET_ID
        )
        self.token = token or os.environ.get("HF_TOKEN", "").strip() or None
        self.max_retries = max_retries
        self.base_backoff = base_backoff

    def _retry(self, op_name: str, fn, *args, **kwargs):
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                if attempt == self.max_retries:
                    break
                sleep_s = self.base_backoff * (2 ** (attempt - 1))
                print(
                    f"[BucketClient] {op_name} attempt {attempt}/{self.max_retries} "
                    f"failed ({exc}); retrying in {sleep_s:.1f}s...",
                    flush=True,
                )
                time.sleep(sleep_s)
        raise RuntimeError(
            f"[BucketClient] {op_name} failed after {self.max_retries} attempts: {last_exc}"
        ) from last_exc

    def path_exists(self, remote_path: str) -> bool:
        """Return True if `remote_path` exists in the bucket, False otherwise."""
        norm_path = remote_path.lstrip("/")

        def _check():
            infos = list(
                get_bucket_paths_info(
                    self.bucket_id,
                    paths=[norm_path],
                    token=self.token,
                )
            )
            for item in infos:
                item_path = getattr(item, "path", None)
                if item_path == norm_path:
                    exists_flag = getattr(item, "exists", True)
                    return bool(exists_flag)
            return False

        return bool(self._retry(f"path_exists({norm_path})", _check))

    def list_paths(self, prefix: str | None = None, recursive: bool = True) -> set[str]:
        """List all file paths in the bucket under `prefix`."""
        norm_prefix = prefix.strip("/") if prefix else None

        def _list():
            items = list_bucket_tree(
                self.bucket_id,
                prefix=norm_prefix,
                recursive=recursive,
                token=self.token,
            )
            paths: set[str] = set()
            for item in items:
                p = getattr(item, "path", None)
                item_type = getattr(item, "type", "file")
                if p and item_type != "directory":
                    paths.add(p.lstrip("/"))
            return paths

        return self._retry(f"list_paths({norm_prefix})", _list)

    def download_bytes(self, remote_path: str, must_exist: bool = False) -> bytes | None:
        """
        Download `remote_path` from the bucket and return its raw bytes.

        - If `must_exist` is False, first checks `path_exists(remote_path)`.
          Returns `None` ONLY when `path_exists` confirms the file does not exist.
        - If the file exists on the bucket, any download error after retries raises
          `RuntimeError` (never silently returns `None`).
        """
        norm_path = remote_path.lstrip("/")
        if not must_exist:
            if not self.path_exists(norm_path):
                return None

        def _dl() -> bytes:
            with tempfile.TemporaryDirectory(prefix="tdv_bkt_dl_") as tmp_dir:
                local_target = Path(tmp_dir) / Path(norm_path).name
                download_bucket_files(
                    self.bucket_id,
                    files=[(norm_path, str(local_target))],
                    token=self.token,
                )
                if not local_target.exists():
                    raise FileNotFoundError(
                        f"Bucket download did not materialize {local_target} for {norm_path}"
                    )
                return local_target.read_bytes()

        return self._retry(f"download_bytes({norm_path})", _dl)

    def upload_files_batch(
        self,
        add_files: Sequence[tuple[bytes | str | Path, str]],
        delete_paths: Sequence[str] | None = None,
    ) -> None:
        """Atomically upload/overwrite multiple files in the bucket in a single batch call."""
        normalized_add = [
            (src, dst.lstrip("/")) for src, dst in add_files
        ]
        normalized_del = [p.lstrip("/") for p in (delete_paths or [])]

        if not normalized_add and not normalized_del:
            return

        def _batch():
            batch_bucket_files(
                self.bucket_id,
                add=normalized_add if normalized_add else None,
                delete=normalized_del if normalized_del else None,
                token=self.token,
            )

        self._retry(
            f"upload_files_batch(add={len(normalized_add)}, delete={len(normalized_del)})",
            _batch,
        )

    def upload_bytes(self, remote_path: str, content: bytes) -> None:
        """Upload or overwrite a single file's bytes in the bucket."""
        self.upload_files_batch([(content, remote_path.lstrip("/"))])

    def delete_paths(self, remote_paths: Sequence[str]) -> None:
        """Delete one or more paths from the bucket."""
        if not remote_paths:
            return
        self.upload_files_batch([], delete_paths=remote_paths)

    def read_json(self, remote_path: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
        """Read and parse a JSON file from the bucket, returning `default` if absent."""
        raw = self.download_bytes(remote_path, must_exist=False)
        if raw is None:
            return dict(default) if default is not None else {}
        return json.loads(raw.decode("utf-8"))

    def write_json(self, remote_path: str, payload: dict[str, Any]) -> None:
        """Serialize `payload` as formatted JSON and upload to `remote_path` in the bucket."""
        raw = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.upload_bytes(remote_path, raw)

    def download_encrypted_chunk(
        self,
        exchange: str,
        interval: str,
        chunk_idx: int,
        fernet_key: bytes,
    ) -> tuple[pd.DataFrame | None, bool]:
        """
        Download and decrypt an existing `.parquet.enc` chunk from the bucket.

        Returns:
            `(df, True)` if the chunk exists on the bucket and was decrypted.
            `(None, False)` ONLY if the chunk is confirmed not to exist on the bucket.

        Raises:
            `RuntimeError` if the chunk exists on the bucket but download or
            decryption fails after retries (preventing silent overwrite of
            historical data during Phase 2 or chunk resume).
        """
        remote_path = get_chunk_bucket_path(exchange, interval, chunk_idx)
        raw_enc = self.download_bytes(remote_path, must_exist=False)
        if raw_enc is None:
            return None, False

        try:
            df = decrypt_dataframe_parquet(raw_enc, fernet_key)
            return df, True
        except Exception as exc:
            raise RuntimeError(
                f"Failed to decrypt existing bucket chunk {remote_path}: {exc}"
            ) from exc

