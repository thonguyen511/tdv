"""
GitHub Actions `repository_dispatch` webhook sender for Kaggle nodes.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from typing import Any

import requests

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tdv.config import DEFAULT_GITHUB_REPO


def send_github_callback(
    node_id: int,
    status: str,
    phase: str = "",
    github_repo: str | None = None,
    gh_pat: str | None = None,
    max_retries: int = 4,
    extra_payload: dict[str, Any] | None = None,
) -> bool:
    """
    Send a `repository_dispatch` (`event_type="node_callback"`) event to GitHub
    Actions with retries.
    """
    token = (gh_pat or os.environ.get("GH_PAT", "")).strip()
    repo = (github_repo or os.environ.get("GITHUB_REPO", "") or DEFAULT_GITHUB_REPO).strip()

    if not token:
        print("[Callback] Warning: GH_PAT not set. Skipping GitHub dispatch.", flush=True)
        return False

    url = f"https://api.github.com/repos/{repo}/dispatches"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    client_payload: dict[str, Any] = {
        "node_id": int(node_id),
        "status": str(status),
        "phase": str(phase),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if extra_payload:
        for k, v in extra_payload.items():
            if v is not None:
                client_payload[k] = v

    payload = {
        "event_type": "node_callback",
        "client_payload": client_payload,
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=20)
            if resp.status_code == 204:
                print(
                    f"[Callback] Successfully dispatched '{status}' for Node {node_id} to {repo}.",
                    flush=True,
                )
                return True
            print(
                f"[Callback] Attempt {attempt}/{max_retries} HTTP {resp.status_code}: {resp.text}",
                flush=True,
            )
        except Exception as exc:
            print(
                f"[Callback] Attempt {attempt}/{max_retries} exception: {exc}",
                flush=True,
            )
        if attempt < max_retries:
            time.sleep(3.0 * attempt)

    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Send GitHub repository_dispatch callback")
    parser.add_argument("--node-id", type=int, required=True)
    parser.add_argument("--status", type=str, required=True)
    parser.add_argument("--phase", type=str, default="")
    parser.add_argument("--session-id", type=str, default="")
    parser.add_argument("--account-idx", type=int, default=None)
    args = parser.parse_args()
    extra: dict[str, Any] = {}
    if args.session_id:
        extra["session_id"] = args.session_id
    if args.account_idx is not None:
        extra["account_idx"] = args.account_idx
    ok = send_github_callback(
        node_id=args.node_id,
        status=args.status,
        phase=args.phase,
        extra_payload=extra or None,
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

