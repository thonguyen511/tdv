"""
CLI utility to decrypt `.parquet.enc` files produced by the `tdv` pipeline.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from cryptography.fernet import InvalidToken

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tdv.crypto import decrypt_bytes, decrypt_dataframe_parquet, derive_key


def decrypt_parquet_file(input_path: str | Path, password: str, output_path: str | Path | None = None):
    """Decrypt a `.parquet.enc` file into a pandas DataFrame (and optionally save as `.parquet`)."""
    in_path = Path(input_path)
    if not in_path.exists():
        raise FileNotFoundError(f"File not found: {in_path}")

    key = derive_key(password)
    encrypted_data = in_path.read_bytes()

    if output_path:
        out_path = Path(output_path)
        raw_parquet = decrypt_bytes(encrypted_data, key)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(raw_parquet)
        import pandas as pd
        return pd.read_parquet(out_path)

    return decrypt_dataframe_parquet(encrypted_data, key)


def decrypt_folder(folder_path: str | Path, password: str, remove_encrypted: bool = False) -> int:
    """Recursively decrypt all `.parquet.enc` files inside `folder_path`."""
    root = Path(folder_path)
    key = derive_key(password)
    count = 0
    for enc_file in sorted(root.rglob("*.parquet.enc")):
        out_file = enc_file.with_suffix("")  # strips .enc -> leaves .parquet
        df = decrypt_dataframe_parquet(enc_file.read_bytes(), key)
        df.to_parquet(out_file, index=False, engine="pyarrow")
        if remove_encrypted:
            enc_file.unlink(missing_ok=True)
        count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Decrypt encrypted TradingView .parquet.enc files.")
    parser.add_argument("input", help="Path to the .parquet.enc file")
    parser.add_argument("-p", "--password", default=os.environ.get("DATA_PASSWORD", ""), help="Decryption password")
    parser.add_argument("-o", "--output", help="Optional path to save the decrypted .parquet file")
    args = parser.parse_args()

    if not args.password:
        print("Error: Password must be supplied via -p/--password or DATA_PASSWORD env var.", file=sys.stderr)
        return 1

    try:
        df = decrypt_parquet_file(args.input, args.password, args.output)
        print(f"Successfully decrypted: {args.input}")
        print(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns")
        print(f"Columns: {list(df.columns)}")
        print("\n--- First 5 Rows ---")
        print(df.head(5))
        return 0
    except InvalidToken:
        print("Decryption failed: Invalid password or corrupted file.", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
