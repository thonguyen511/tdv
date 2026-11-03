"""
Fernet + PBKDF2-HMAC-SHA256 encryption utilities for Parquet chunks and JWT records.

Maintains 100% binary compatibility with `src/src_2/decrypt.py`:
- KDF: PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=b"tradingview", iterations=480000)
- Cipher: cryptography.fernet.Fernet
"""

from __future__ import annotations

import base64
import io
import json
from typing import Any

import pandas as pd
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

DEFAULT_SALT = b"tradingview"
PBKDF2_ITERATIONS = 480000


def derive_key(password: str, salt: bytes = DEFAULT_SALT) -> bytes:
    """Derive a 32-byte URL-safe base64-encoded Fernet key from a password."""
    if not password:
        raise ValueError("Encryption password cannot be empty.")
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=PBKDF2_ITERATIONS,
    )
    return base64.urlsafe_b64encode(kdf.derive(password.encode("utf-8")))


def encrypt_bytes(raw_bytes: bytes, key: bytes) -> bytes:
    """Encrypt raw bytes using a derived Fernet key."""
    return Fernet(key).encrypt(raw_bytes)


def decrypt_bytes(encrypted_bytes: bytes, key: bytes) -> bytes:
    """Decrypt Fernet-encrypted bytes using a derived key."""
    return Fernet(key).decrypt(encrypted_bytes)


def encrypt_json(payload: dict[str, Any], key: bytes) -> bytes:
    """Serialize a dict as UTF-8 JSON and encrypt it with Fernet."""
    raw = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    return encrypt_bytes(raw, key)


def decrypt_json(encrypted_bytes: bytes, key: bytes) -> dict[str, Any]:
    """Decrypt Fernet bytes and parse as a JSON dictionary."""
    raw = decrypt_bytes(encrypted_bytes, key)
    return json.loads(raw.decode("utf-8"))


def encrypt_dataframe_parquet(df: pd.DataFrame, key: bytes) -> bytes:
    """Serialize a DataFrame to an in-memory Parquet buffer and encrypt it."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return encrypt_bytes(buf.getvalue(), key)


def decrypt_dataframe_parquet(encrypted_bytes: bytes, key: bytes) -> pd.DataFrame:
    """Decrypt an encrypted `.parquet.enc` payload into a pandas DataFrame."""
    raw_parquet = decrypt_bytes(encrypted_bytes, key)
    return pd.read_parquet(io.BytesIO(raw_parquet))


__all__ = [
    "DEFAULT_SALT",
    "PBKDF2_ITERATIONS",
    "InvalidToken",
    "derive_key",
    "encrypt_bytes",
    "decrypt_bytes",
    "encrypt_json",
    "decrypt_json",
    "encrypt_dataframe_parquet",
    "decrypt_dataframe_parquet",
]

