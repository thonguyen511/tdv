"""
Distributed 20-Node Kaggle Fleet Orchestrator (`fleet_manager.py`).

Key architectural improvements over `tdv_aio`:
1. Stores orchestrator state in the Hugging Face Storage Bucket at `control/state.json`
   (eliminating git commit/rebase race conditions in GitHub Actions).
2. Syncs `log/NODE_{i}/{run_tag}/COMPLETED` markers directly from the HF Bucket tree.
3. Queries Kaggle's REST API (`/api/v1/kernels/status`) directly with JSON parsing
   instead of fragile CLI stdout substring matching.
4. Waits up to 120s for a finishing Kaggle session to release its slot on `TIMEOUT`,
   `RATE_LIMITED`, `ERROR`, or `REARM_STANDBY` so continuation kernels are pushed
   immediately in the same `repository_dispatch` run without requiring a GitHub cron.
5. Clones the repo and stores HF caches inside `/tmp` on Kaggle runners so
   `/kaggle/working` stays clean and container teardown finishes in seconds.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from tdv.bucket import BucketClient
from tdv.config import (
    BUCKET_STATE_PATH,
    DEFAULT_GITHUB_REPO,
    DEFAULT_HF_BUCKET_ID,
    ERROR_COOLDOWN_SECONDS,
    MAX_CONSECUTIVE_NODE_FAILURES,
    NODES_PER_KAGGLE_ACCOUNT,
    RATE_LIMIT_COOLDOWN_SECONDS,
    TOTAL_KAGGLE_ACCOUNTS,
    TOTAL_NOTEBOOKS,
    compute_weekly_run_tag,
)

KAGGLE_API_BASE = "https://www.kaggle.com/api/v1"

DEFAULT_ACCOUNT_USERNAMES: dict[int, str] = {
    0: "thonguyen511",
    1: "hero0511acc",
    2: "tqunacc",
    3: "acc3conheo",
}


def load_kaggle_tokens() -> list[str]:
    """Load `KAGGLE_TOKEN_0..3` from environment variables."""
    tokens = [
        os.environ.get(f"KAGGLE_TOKEN_{i}", "").strip()
        for i in range(TOTAL_KAGGLE_ACCOUNTS)
    ]
    if not any(tokens):
        single = os.environ.get("KAGGLE_API_TOKEN", "").strip()
        if single:
            tokens[0] = single
    return tokens


def get_account_index_for_node(node_id: int) -> int:
    """Map node `0..19` to Kaggle account index `0..3` (5 nodes per account)."""
    return (int(node_id) // NODES_PER_KAGGLE_ACCOUNT) % TOTAL_KAGGLE_ACCOUNTS


def get_kaggle_username(api_token: str) -> str | None:
    """Resolve the Kaggle username associated with a `KGAT_...` bearer token."""
    if not api_token:
        return None
    headers = {"Authorization": f"Bearer {api_token}", "Accept": "application/json"}
    try:
        resp = requests.get(
            f"{KAGGLE_API_BASE}/kernels/list",
            headers=headers,
            params={"user": "true", "pageSize": 5},
            timeout=15,
        )
        if resp.status_code == 200:
            items = resp.json()
            if isinstance(items, list) and items:
                ref = items[0].get("ref", "")
                if "/" in ref:
                    return ref.split("/")[0]
    except Exception:
        pass

    # Fallback via kaggle CLI config view
    env = os.environ.copy()
    env["KAGGLE_API_TOKEN"] = api_token
    try:
        res = subprocess.run(
            ["kaggle", "kernels", "list", "--mine", "--page-size", "5", "-v"],
            env=env,
            capture_output=True,
            text=True,
            timeout=20,
        )
        for line in res.stdout.splitlines()[1:]:
            if "," in line and "/" in line:
                ref = line.split(",")[0].strip()
                if "/" in ref:
                    return ref.split("/")[0]
    except Exception:
        pass
    return None


def get_kernel_state(username: str, slug: str, api_token: str) -> str:
    """
    Query Kaggle for the live session status of `username/slug`.

    Returns one of: `"running"`, `"queued"`, `"complete"`, `"error"`,
    `"cancelacknowledged"`, `"cancelrequested"`, `"not_found"`, or `"unknown"`.
    """
    if not username or not api_token:
        return "unknown"

    # 1. Try kagglesdk / KaggleApi in-process (works on both Windows and Linux without spawning CLI)
    prev_tok = os.environ.get("KAGGLE_API_TOKEN")
    os.environ["KAGGLE_API_TOKEN"] = api_token
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi

        api = KaggleApi()
        api.authenticate()
        resp = api.kernels_status(f"{username}/{slug}")
        raw_status = str(getattr(resp, "status", "") or "").strip().lower()
        if "." in raw_status:
            raw_status = raw_status.split(".")[-1]
        if raw_status:
            return raw_status
    except ValueError as ve:
        if "cannot access kernel" in str(ve).lower() or "403" in str(ve) or "404" in str(ve):
            return "not_found"
    except Exception:
        pass
    finally:
        if prev_tok is None:
            os.environ.pop("KAGGLE_API_TOKEN", None)
        else:
            os.environ["KAGGLE_API_TOKEN"] = prev_tok

    # 2. Fallback to POST/GET REST endpoint
    headers = {"Authorization": f"Bearer {api_token}", "Accept": "application/json"}
    for method in ("POST", "GET"):
        try:
            if method == "POST":
                r = requests.post(
                    f"{KAGGLE_API_BASE}/kernels/status",
                    headers=headers,
                    json={"userName": username, "kernelSlug": slug},
                    timeout=15,
                )
            else:
                r = requests.get(
                    f"{KAGGLE_API_BASE}/kernels/status",
                    headers=headers,
                    params={"userName": username, "kernelSlug": slug},
                    timeout=15,
                )
            if r.status_code == 200:
                data = r.json()
                status_val = str(data.get("status", "")).strip().lower()
                if "." in status_val:
                    status_val = status_val.split(".")[-1]
                if status_val:
                    return status_val
        except Exception:
            pass
    return "unknown"


def wait_until_kernel_not_running(
    username: str,
    slug: str,
    api_token: str,
    max_wait_seconds: int = 120,
    poll_interval: int = 10,
) -> str:
    """
    Poll `get_kernel_state` until `username/slug` is no longer in
    `{"running", "queued", "cancelrequested"}` or `max_wait_seconds` elapses.
    """
    deadline = time.time() + max_wait_seconds
    state = get_kernel_state(username, slug, api_token)
    while state in ("running", "queued", "cancelrequested") and time.time() < deadline:
        print(
            f"[FleetManager] Waiting for {username}/{slug} (currently '{state}') "
            f"to finish container teardown...",
            flush=True,
        )
        time.sleep(poll_interval)
        state = get_kernel_state(username, slug, api_token)
    return state


def delete_kernel_if_exists(
    username: str,
    slug: str,
    api_token: str,
    wait_if_running: bool = True,
) -> bool:
    """
    Delete `username/slug` from Kaggle if it exists so old finished notebooks
    do not clutter the Kaggle accounts across Standby <-> Weekly transitions.
    """
    if not username or not api_token:
        return False
    if wait_if_running:
        k_state = wait_until_kernel_not_running(username, slug, api_token, max_wait_seconds=90)
    else:
        k_state = get_kernel_state(username, slug, api_token)

    if k_state == "not_found":
        return True

    # 1. Try kagglesdk delete_kernel in-process
    try:
        from kagglesdk import KaggleClient
        from kagglesdk.kernels.types.kernels_api_service import ApiDeleteKernelRequest

        client = KaggleClient(api_token=api_token)
        req = ApiDeleteKernelRequest()
        req.user_name = username
        req.kernel_slug = slug
        client.kernels.kernels_api_client.delete_kernel(req)
        print(f"[FleetManager] Deleted old kernel {username}/{slug}", flush=True)
        return True
    except Exception as exc:
        if "404" in str(exc) or "not found" in str(exc).lower() or "403" in str(exc):
            return True

    # 2. Fallback to kaggle CLI
    env = os.environ.copy()
    env["KAGGLE_API_TOKEN"] = api_token
    try:
        res = subprocess.run(
            ["kaggle", "kernels", "delete", "-y", f"{username}/{slug}"],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if res.returncode == 0 or "404" in (res.stdout + res.stderr):
            print(f"[FleetManager] Deleted old kernel {username}/{slug}", flush=True)
            return True
    except Exception:
        pass
    return False


def delete_all_worker_kernels(
    tokens: list[str],
    usernames: dict[int, str],
    wait_if_running: bool = False,
) -> None:
    """Delete all 20 worker kernels (`tdv-node-0..19`) across the 4 Kaggle accounts."""
    print("[FleetManager] Cleaning up old worker notebooks (tdv-node-0..19) across all accounts...", flush=True)
    for node_id in range(TOTAL_NOTEBOOKS):
        acc_idx = get_account_index_for_node(node_id)
        api_token = tokens[acc_idx] if acc_idx < len(tokens) else ""
        username = usernames.get(acc_idx, "")
        if api_token and username:
            delete_kernel_if_exists(
                username=username,
                slug=f"tdv-node-{node_id}",
                api_token=api_token,
                wait_if_running=wait_if_running,
            )


def default_fleet_state(phase: str = "initial", run_tag: str = "INITIAL") -> dict[str, Any]:
    """Return a clean initial `control/state.json` dictionary."""
    now_iso = datetime.now(timezone.utc).isoformat()
    return {
        "phase": phase,
        "run_tag": run_tag,
        "updated_at": now_iso,
        "nodes": {
            str(i): {"status": "IDLE", "retries": 0, "updated_at": now_iso}
            for i in range(TOTAL_NOTEBOOKS)
        },
        "standby": {"status": "IDLE", "updated_at": now_iso},
    }


def load_and_sync_fleet_state(bucket: BucketClient) -> dict[str, Any]:
    """
    Load `control/state.json` from the HF Bucket and reconcile it against
    `log/NODE_{i}/{run_tag}/COMPLETED` markers present in the bucket.
    """
    state = bucket.read_json(BUCKET_STATE_PATH, default=None)
    if not state or "nodes" not in state:
        state = default_fleet_state(phase="initial", run_tag="INITIAL")

    phase = str(state.get("phase", "initial"))
    run_tag = str(state.get("run_tag", "INITIAL" if phase == "initial" else phase))
    nodes = state.setdefault("nodes", {})
    now_iso = datetime.now(timezone.utc).isoformat()
    for i in range(TOTAL_NOTEBOOKS):
        nodes.setdefault(str(i), {"status": "IDLE", "retries": 0, "updated_at": now_iso})

    if phase != "standby":
        try:
            log_paths = bucket.list_paths(prefix="log", recursive=True)
            for i in range(TOTAL_NOTEBOOKS):
                marker = f"log/NODE_{i}/{run_tag}/COMPLETED"
                if marker in log_paths and nodes[str(i)].get("status") != "DONE":
                    nodes[str(i)]["status"] = "DONE"
                    nodes[str(i)]["retries"] = 0
                    nodes[str(i)]["updated_at"] = now_iso
        except Exception as exc:
            print(f"[FleetManager] Warning: could not list log markers: {exc}", flush=True)

    return state


def save_fleet_state(bucket: BucketClient, state: dict[str, Any]) -> None:
    """Persist `state` to `control/state.json` in the HF Bucket."""
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    bucket.write_json(BUCKET_STATE_PATH, state)


def build_worker_runner_script(
    node_id: int,
    phase: str,
    run_tag: str,
    start_delay_seconds: int = 0,
) -> str:
    """
    Generate the self-bootstrapping Python script executed on Kaggle for `tdv-node-{node_id}`.

    Note: Clones the repo into `/tmp/tdv_repo` and sets `HF_HOME=/tmp/hf_cache`
    so `/kaggle/working` stays completely empty for fast container exit.
    """
    hf_token = os.environ.get("HF_TOKEN", "").strip()
    hf_bucket = os.environ.get("HF_BUCKET_ID", DEFAULT_HF_BUCKET_ID).strip()
    data_password = os.environ.get("DATA_PASSWORD", "").strip()
    gh_pat = os.environ.get("GH_PAT", "").strip()
    gh_repo = os.environ.get("GITHUB_REPO", DEFAULT_GITHUB_REPO).strip()
    tv_login_pass = os.environ.get("TV_LOGIN_PASSWORD", "").strip()
    tv_sessions_json = os.environ.get("TV_SESSIONS_JSON", "").strip()
    tv_emails_json = os.environ.get("TV_EMAILS_JSON", "").strip()
    ult_session = os.environ.get("TV_ULTIMATE_SESSIONID", "").strip()
    ult_sign = os.environ.get("TV_ULTIMATE_SESSIONID_SIGN", "").strip()

    phase_mode = "initial" if phase == "initial" else "weekly"

    return f'''import os
import shutil
import subprocess
import sys

os.environ["HF_HOME"] = "/tmp/hf_cache"
os.environ["TDV_STAGING_DIR"] = "/tmp/tdv_staging"
os.environ["NOTEBOOK_ID"] = "{node_id}"
os.environ["HF_TOKEN"] = {json.dumps(hf_token)}
os.environ["HF_BUCKET_ID"] = {json.dumps(hf_bucket)}
os.environ["DATA_PASSWORD"] = {json.dumps(data_password)}
os.environ["GH_PAT"] = {json.dumps(gh_pat)}
os.environ["GITHUB_REPO"] = {json.dumps(gh_repo)}
os.environ["PIPELINE_PHASE"] = {json.dumps(phase_mode)}
os.environ["WEEK_TAG"] = {json.dumps(run_tag)}
os.environ["START_DELAY_SECONDS"] = "{int(start_delay_seconds)}"
os.environ["TV_LOGIN_PASSWORD"] = {json.dumps(tv_login_pass)}
os.environ["TV_SESSIONS_JSON"] = {json.dumps(tv_sessions_json)}
os.environ["TV_EMAILS_JSON"] = {json.dumps(tv_emails_json)}
os.environ["TV_ULTIMATE_SESSIONID"] = {json.dumps(ult_session)}
os.environ["TV_ULTIMATE_SESSIONID_SIGN"] = {json.dumps(ult_sign)}

print("=== [TDV Runner] Bootstrapping Node {node_id} ({phase_mode} / {run_tag}) ===", flush=True)

subprocess.run(
    [
        sys.executable, "-m", "pip", "install", "-q", "--upgrade",
        "huggingface_hub>=1.5.0", "hf-xet>=1.5.2",
        "cryptography", "websocket-client", "pandas", "pyarrow", "requests",
    ],
    check=True,
)

repo_dir = "/tmp/tdv_repo"
if os.path.exists(repo_dir):
    shutil.rmtree(repo_dir, ignore_errors=True)

clone_url = f"https://{{os.environ['GH_PAT']}}@github.com/{gh_repo}.git" if os.environ.get("GH_PAT") else "https://github.com/{gh_repo}.git"
subprocess.run(["git", "clone", "--depth", "1", clone_url, repo_dir], check=True)

proc = subprocess.run(
    [
        sys.executable, "-u", "-m", "tdv.supervisor",
        "--notebook-id", "{node_id}",
        "--phase", {json.dumps(phase_mode)},
        "--start-delay", "{int(start_delay_seconds)}",
    ],
    cwd=repo_dir,
)
shutil.rmtree(repo_dir, ignore_errors=True)
shutil.rmtree("/tmp/hf_cache", ignore_errors=True)
sys.exit(proc.returncode)
'''


def build_standby_runner_script() -> str:
    """Generate the self-bootstrapping script for `tdv-standby`."""
    gh_pat = os.environ.get("GH_PAT", "").strip()
    gh_repo = os.environ.get("GITHUB_REPO", DEFAULT_GITHUB_REPO).strip()
    hf_token = os.environ.get("HF_TOKEN", "").strip()
    hf_bucket = os.environ.get("HF_BUCKET_ID", DEFAULT_HF_BUCKET_ID).strip()

    return f'''import os
import shutil
import subprocess
import sys

os.environ["HF_HOME"] = "/tmp/hf_cache"
os.environ["GH_PAT"] = {json.dumps(gh_pat)}
os.environ["GITHUB_REPO"] = {json.dumps(gh_repo)}
os.environ["HF_TOKEN"] = {json.dumps(hf_token)}
os.environ["HF_BUCKET_ID"] = {json.dumps(hf_bucket)}

print("=== [TDV Standby Runner] Bootstrapping Standby Master Clock ===", flush=True)
subprocess.run(
    [sys.executable, "-m", "pip", "install", "-q", "requests", "huggingface_hub>=1.5.0"],
    check=True,
)

repo_dir = "/tmp/tdv_repo"
if os.path.exists(repo_dir):
    shutil.rmtree(repo_dir, ignore_errors=True)

clone_url = f"https://{{os.environ['GH_PAT']}}@github.com/{gh_repo}.git" if os.environ.get("GH_PAT") else "https://github.com/{gh_repo}.git"
subprocess.run(["git", "clone", "--depth", "1", clone_url, repo_dir], check=True)

proc = subprocess.run([sys.executable, "-u", "-m", "tdv.standby_clock"], cwd=repo_dir)
if proc.returncode != 0 and os.environ.get("GH_PAT"):
    import requests
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    st = "TRIGGER_WEEKLY" if now.weekday() == 0 else "REARM_STANDBY"
    ph = "weekly" if st == "TRIGGER_WEEKLY" else "standby"
    try:
        requests.post(
            f"https://api.github.com/repos/{gh_repo}/dispatches",
            headers={{
                "Authorization": f"Bearer {{os.environ['GH_PAT']}}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }},
            json={{"event_type": "kaggle_node_callback", "client_payload": {{"node_id": 99, "status": st, "phase": ph}}}},
            timeout=20,
        )
    except Exception:
        pass
shutil.rmtree(repo_dir, ignore_errors=True)
shutil.rmtree("/tmp/hf_cache", ignore_errors=True)
sys.exit(proc.returncode)
'''


def _push_kernel_via_cli(
    username: str,
    slug: str,
    title: str,
    script_code: str,
    api_token: str,
    max_attempts: int = 3,
) -> bool:
    """Write kernel metadata + script to a temp directory and push via Kaggle API/CLI with retry."""
    with tempfile.TemporaryDirectory(prefix=f"kgl_{slug}_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        code_file = f"{slug}.py"
        (tmp_path / code_file).write_text(script_code, encoding="utf-8")
        metadata = {
            "id": f"{username}/{slug}",
            "title": title,
            "code_file": code_file,
            "language": "python",
            "kernel_type": "script",
            "is_private": "true",
            "enable_gpu": "false",
            "enable_tpu": "false",
            "enable_internet": "true",
            "dataset_sources": [],
            "competition_sources": [],
            "kernel_sources": [],
            "model_sources": [],
        }
        (tmp_path / "kernel-metadata.json").write_text(
            json.dumps(metadata, indent=2), encoding="utf-8"
        )

        for attempt in range(1, max_attempts + 1):
            prev_tok = os.environ.get("KAGGLE_API_TOKEN")
            os.environ["KAGGLE_API_TOKEN"] = api_token
            try:
                from kaggle.api.kaggle_api_extended import KaggleApi

                api = KaggleApi()
                api.authenticate()
                resp = api.kernels_push(str(tmp_path))
                err_msg = getattr(resp, "error", None)
                if not err_msg:
                    url = getattr(resp, "url", f"https://www.kaggle.com/code/{username}/{slug}")
                    print(f"[FleetManager] Pushed {username}/{slug} -> {url}", flush=True)
                    return True
            except Exception:
                pass
            finally:
                if prev_tok is None:
                    os.environ.pop("KAGGLE_API_TOKEN", None)
                else:
                    os.environ["KAGGLE_API_TOKEN"] = prev_tok

            env = os.environ.copy()
            env["KAGGLE_API_TOKEN"] = api_token
            res = subprocess.run(
                ["kaggle", "kernels", "push", "-p", str(tmp_path)],
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            if res.returncode == 0:
                print(f"[FleetManager] Pushed {username}/{slug}: {res.stdout.strip()}", flush=True)
                return True
            print(
                f"[FleetManager] Attempt {attempt}/{max_attempts} failed to push {username}/{slug} "
                f"(code={res.returncode}): {res.stdout} {res.stderr}",
                flush=True,
            )
            if attempt < max_attempts:
                time.sleep(5 * attempt)
        return False


def push_worker_node(
    node_id: int,
    state: dict[str, Any],
    tokens: list[str],
    usernames: dict[int, str],
    start_delay_seconds: int = 0,
    wait_if_running: bool = False,
) -> bool:
    """Push `tdv-node-{node_id}` to its assigned Kaggle account."""
    acc_idx = get_account_index_for_node(node_id)
    api_token = tokens[acc_idx] if acc_idx < len(tokens) else ""
    username = usernames.get(acc_idx)
    if not api_token or not username:
        print(f"[FleetManager] Cannot push Node {node_id}: missing Kaggle account {acc_idx}", flush=True)
        return False

    slug = f"tdv-node-{node_id}"
    title = f"TDV Node {node_id}"
    if wait_if_running:
        k_state = wait_until_kernel_not_running(username, slug, api_token, max_wait_seconds=120)
    else:
        k_state = get_kernel_state(username, slug, api_token)

    if k_state in ("running", "queued"):
        print(f"[FleetManager] {username}/{slug} is already '{k_state}'. Skipping duplicate push.", flush=True)
        state["nodes"][str(node_id)]["status"] = "RUNNING"
        return True

    phase = str(state.get("phase", "initial"))
    run_tag = str(state.get("run_tag", "INITIAL" if phase == "initial" else phase))
    script_code = build_worker_runner_script(
        node_id=node_id,
        phase=phase,
        run_tag=run_tag,
        start_delay_seconds=start_delay_seconds,
    )
    ok = _push_kernel_via_cli(username, slug, title, script_code, api_token)
    if ok:
        state["nodes"][str(node_id)]["status"] = "RUNNING"
        state["nodes"][str(node_id)]["updated_at"] = datetime.now(timezone.utc).isoformat()
    return ok


def push_standby_node(
    state: dict[str, Any],
    tokens: list[str],
    usernames: dict[int, str],
    wait_if_running: bool = False,
) -> bool:
    """Push `tdv-standby` on Kaggle Account 0."""
    api_token = tokens[0] if tokens else ""
    username = usernames.get(0)
    if not api_token or not username:
        print("[FleetManager] Cannot push tdv-standby: Account 0 not configured.", flush=True)
        return False

    slug = "tdv-standby"
    title = "TDV Standby"
    if wait_if_running:
        k_state = wait_until_kernel_not_running(username, slug, api_token, max_wait_seconds=120)
        if k_state not in ("running", "queued", "not_found"):
            delete_kernel_if_exists(username, slug, api_token, wait_if_running=False)
    else:
        k_state = get_kernel_state(username, slug, api_token)

    if k_state in ("running", "queued"):
        print(f"[FleetManager] {username}/{slug} is already '{k_state}'.", flush=True)
        state["standby"] = {"status": "RUNNING", "updated_at": datetime.now(timezone.utc).isoformat()}
        return True

    script_code = build_standby_runner_script()
    ok = _push_kernel_via_cli(username, slug, title, script_code, api_token)
    if ok:
        state["standby"] = {"status": "RUNNING", "updated_at": datetime.now(timezone.utc).isoformat()}
    return ok


def resolve_all_usernames(tokens: list[str]) -> dict[int, str]:
    usernames: dict[int, str] = {}
    for idx, tok in enumerate(tokens):
        if tok:
            uname = DEFAULT_ACCOUNT_USERNAMES.get(idx) or get_kaggle_username(tok)
            if uname:
                usernames[idx] = uname
    return usernames


def reconcile_fleet(
    bucket: BucketClient,
    state: dict[str, Any],
    tokens: list[str],
    usernames: dict[int, str],
) -> None:
    """
    Reconcile all 20 worker nodes (or `tdv-standby` when in `standby` phase).
    """
    phase = str(state.get("phase", "initial"))
    run_tag = str(state.get("run_tag", "INITIAL" if phase == "initial" else phase))
    nodes = state["nodes"]

    # Check if all 20 nodes are DONE
    if phase != "standby" and all(nodes[str(i)].get("status") == "DONE" for i in range(TOTAL_NOTEBOOKS)):
        print(
            f"[FleetManager] All {TOTAL_NOTEBOOKS} nodes completed {run_tag}! "
            f"Deleting finished worker notebooks, transitioning fleet to 'standby' phase, "
            f"and launching Standby Master Clock.",
            flush=True,
        )
        delete_all_worker_kernels(tokens, usernames, wait_if_running=True)
        state["phase"] = "standby"
        push_standby_node(state, tokens, usernames, wait_if_running=False)
        save_fleet_state(bucket, state)
        return

    if state.get("phase") == "standby":
        push_standby_node(state, tokens, usernames, wait_if_running=False)
        save_fleet_state(bucket, state)
        return

    for i in range(TOTAL_NOTEBOOKS):
        node_info = nodes[str(i)]
        status = node_info.get("status", "IDLE")
        retries = int(node_info.get("retries", 0))

        if status == "DONE":
            continue
        if status == "AUTH_FAILED":
            print(f"[FleetManager] Node {i} is marked AUTH_FAILED. Skipping until credentials fixed.", flush=True)
            continue
        if retries > MAX_CONSECUTIVE_NODE_FAILURES:
            print(
                f"[FleetManager] Node {i} exceeded max consecutive failures ({retries}). Skipping.",
                flush=True,
            )
            continue

        acc_idx = get_account_index_for_node(i)
        tok = tokens[acc_idx] if acc_idx < len(tokens) else ""
        uname = usernames.get(acc_idx, "")
        live_k = get_kernel_state(uname, f"tdv-node-{i}", tok)

        if live_k in ("running", "queued"):
            node_info["status"] = "RUNNING"
            continue

        print(
            f"[FleetManager] Reconciling Node {i} (stored={status}, live={live_k}) -> pushing kernel...",
            flush=True,
        )
        push_worker_node(i, state, tokens, usernames, start_delay_seconds=0, wait_if_running=False)
        time.sleep(2)

    save_fleet_state(bucket, state)


def handle_node_callback(
    node_id: int,
    callback_status: str,
    bucket: BucketClient,
    state: dict[str, Any],
    tokens: list[str],
    usernames: dict[int, str],
) -> None:
    """
    Process a `repository_dispatch` callback from a Kaggle worker or Standby Clock.
    """
    cb = callback_status.strip().upper()
    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.isoformat()
    print(f"[FleetManager] Processing callback: node_id={node_id}, status={cb}", flush=True)

    # Safety promotion: if REARM_STANDBY arrives on Monday UTC and this week's tag hasn't run yet,
    # promote it to TRIGGER_WEEKLY so a Monday boundary handoff never misses the weekly launch.
    if cb == "REARM_STANDBY" and now_utc.weekday() == 0:
        expected_week_tag = compute_weekly_run_tag(now_utc)
        if str(state.get("run_tag", "")) != expected_week_tag:
            print(
                f"[FleetManager] Promoting Monday REARM_STANDBY -> TRIGGER_WEEKLY ({expected_week_tag})",
                flush=True,
            )
            cb = "TRIGGER_WEEKLY"

    # 1. Standby Clock Callbacks
    if cb == "REARM_STANDBY":
        state["phase"] = "standby"
        push_standby_node(state, tokens, usernames, wait_if_running=True)
        save_fleet_state(bucket, state)
        return

    if cb == "TRIGGER_WEEKLY":
        week_tag = compute_weekly_run_tag(now_utc)
        print(
            f"[FleetManager] TRIGGER_WEEKLY received -> deleting tdv-standby & old worker notebooks, "
            f"then starting {week_tag} across all 20 nodes!",
            flush=True,
        )
        if tokens and usernames.get(0):
            delete_kernel_if_exists(
                username=usernames[0],
                slug="tdv-standby",
                api_token=tokens[0],
                wait_if_running=True,
            )
        delete_all_worker_kernels(tokens, usernames, wait_if_running=False)
        new_state = default_fleet_state(phase=week_tag, run_tag=week_tag)
        state.clear()
        state.update(new_state)
        save_fleet_state(bucket, state)
        reconcile_fleet(bucket, state, tokens, usernames)
        return

    # 2. Worker Node Callbacks (0..19)
    node_key = str(node_id)
    node_rec = state["nodes"].setdefault(
        node_key, {"status": "IDLE", "retries": 0, "updated_at": now_iso}
    )
    node_rec["updated_at"] = now_iso

    if cb == "DONE":
        node_rec["status"] = "DONE"
        node_rec["retries"] = 0
        save_fleet_state(bucket, state)

        if all(state["nodes"][str(i)].get("status") == "DONE" for i in range(TOTAL_NOTEBOOKS)):
            print(
                f"[FleetManager] All {TOTAL_NOTEBOOKS} nodes are now DONE! "
                "Deleting finished worker notebooks, transitioning to 'standby', and launching tdv-standby.",
                flush=True,
            )
            delete_all_worker_kernels(tokens, usernames, wait_if_running=True)
            state["phase"] = "standby"
            push_standby_node(state, tokens, usernames, wait_if_running=False)
            save_fleet_state(bucket, state)
            return

        # Also reconcile any sibling nodes whose callbacks may have been coalesced by GitHub concurrency
        reconcile_fleet(bucket, state, tokens, usernames)
        return

    if cb == "TIMEOUT":
        node_rec["status"] = "TIMEOUT"
        node_rec["retries"] = 0
        save_fleet_state(bucket, state)
        print(
            f"[FleetManager] Node {node_id} reached 11h 45m TIMEOUT -> "
            "waiting for container teardown and re-pushing continuation session...",
            flush=True,
        )
        push_worker_node(
            node_id,
            state,
            tokens,
            usernames,
            start_delay_seconds=0,
            wait_if_running=True,
        )
        save_fleet_state(bucket, state)
        # Reconcile any other nodes whose TIMEOUT callbacks arrived concurrently
        reconcile_fleet(bucket, state, tokens, usernames)
        return

    if cb == "AUTH_FAILED":
        node_rec["status"] = "AUTH_FAILED"
        save_fleet_state(bucket, state)
        print(
            f"[FleetManager] CRITICAL: Node {node_id} reported AUTH_FAILED. "
            "Not re-pushing to avoid anonymous scraping.",
            flush=True,
        )
        return

    if cb == "RATE_LIMITED":
        retries = int(node_rec.get("retries", 0)) + 1
        node_rec["status"] = "RATE_LIMITED"
        node_rec["retries"] = retries
        save_fleet_state(bucket, state)
        if retries <= MAX_CONSECUTIVE_NODE_FAILURES:
            print(
                f"[FleetManager] Node {node_id} RATE_LIMITED (retry {retries}/{MAX_CONSECUTIVE_NODE_FAILURES}) -> "
                f"re-pushing with {RATE_LIMIT_COOLDOWN_SECONDS}s in-container cooldown...",
                flush=True,
            )
            push_worker_node(
                node_id,
                state,
                tokens,
                usernames,
                start_delay_seconds=RATE_LIMIT_COOLDOWN_SECONDS,
                wait_if_running=True,
            )
            save_fleet_state(bucket, state)
        reconcile_fleet(bucket, state, tokens, usernames)
        return

    # Default: ERROR
    retries = int(node_rec.get("retries", 0)) + 1
    node_rec["status"] = "ERROR"
    node_rec["retries"] = retries
    save_fleet_state(bucket, state)
    if retries <= MAX_CONSECUTIVE_NODE_FAILURES:
        print(
            f"[FleetManager] Node {node_id} ERROR (retry {retries}/{MAX_CONSECUTIVE_NODE_FAILURES}) -> "
            f"re-pushing with {ERROR_COOLDOWN_SECONDS}s in-container cooldown...",
            flush=True,
        )
        push_worker_node(
            node_id,
            state,
            tokens,
            usernames,
            start_delay_seconds=ERROR_COOLDOWN_SECONDS,
            wait_if_running=True,
        )
        save_fleet_state(bucket, state)
    reconcile_fleet(bucket, state, tokens, usernames)


def print_fleet_status(
    state: dict[str, Any],
    tokens: list[str],
    usernames: dict[int, str],
) -> None:
    """Print a comprehensive 4x5 fleet matrix showing both bucket state and live Kaggle status."""
    phase = state.get("phase", "initial")
    run_tag = state.get("run_tag", "INITIAL")
    print("=" * 78)
    print(f" TDV 20-NODE KAGGLE FLEET STATUS | Phase: {phase} | Run Tag: {run_tag}")
    print("=" * 78)

    for acc_idx in range(TOTAL_KAGGLE_ACCOUNTS):
        uname = usernames.get(acc_idx, "<unconfigured>")
        tok = tokens[acc_idx] if acc_idx < len(tokens) else ""
        print(f"\n[Account {acc_idx}: {uname}]")
        start_n = acc_idx * NODES_PER_KAGGLE_ACCOUNT
        for node_id in range(start_n, start_n + NODES_PER_KAGGLE_ACCOUNT):
            n_info = state.get("nodes", {}).get(str(node_id), {})
            b_status = n_info.get("status", "IDLE")
            retries = n_info.get("retries", 0)
            live_k = get_kernel_state(uname, f"tdv-node-{node_id}", tok) if tok and uname != "<unconfigured>" else "n/a"
            print(
                f"  Node {node_id:02d} (tdv-node-{node_id:<2d}) | "
                f"Bucket: {b_status:<12s} | Kaggle: {live_k:<10s} | Retries: {retries}"
            )

    if tokens and usernames.get(0):
        sb_live = get_kernel_state(usernames[0], "tdv-standby", tokens[0])
        sb_bucket = state.get("standby", {}).get("status", "IDLE")
        print(f"\n[Standby Clock ({usernames[0]}/tdv-standby)] Bucket: {sb_bucket} | Kaggle: {sb_live}")
    print("=" * 78)


def main() -> int:
    parser = argparse.ArgumentParser(description="TDV 20-Node Kaggle Fleet Orchestrator")
    parser.add_argument("--status", action="store_true", help="Print full fleet status")
    parser.add_argument("--reconcile", action="store_true", help="Reconcile and launch pending nodes")
    parser.add_argument("--push-node", type=int, default=None, help="Force-push a single worker node (0..19)")
    parser.add_argument("--push-standby", action="store_true", help="Force-push the standby clock notebook")
    parser.add_argument("--reset-phase", type=str, default=None, help="Reset fleet state to 'initial' or 'WEEK_<WWYY>'")
    parser.add_argument("--callback-node", type=int, default=None, help="Node ID from repository_dispatch callback")
    parser.add_argument("--callback-status", type=str, default=None, help="Status from repository_dispatch callback")
    args = parser.parse_args()

    bucket = BucketClient()
    tokens = load_kaggle_tokens()
    usernames = resolve_all_usernames(tokens)
    state = load_and_sync_fleet_state(bucket)

    if args.reset_phase:
        target = args.reset_phase.strip()
        r_tag = "INITIAL" if target.lower() == "initial" else target.upper()
        p_val = "initial" if target.lower() == "initial" else r_tag
        state = default_fleet_state(phase=p_val, run_tag=r_tag)
        save_fleet_state(bucket, state)
        print(f"[FleetManager] Reset fleet state to phase={p_val}, run_tag={r_tag}", flush=True)

    if args.callback_node is not None and args.callback_status:
        handle_node_callback(
            node_id=args.callback_node,
            callback_status=args.callback_status,
            bucket=bucket,
            state=state,
            tokens=tokens,
            usernames=usernames,
        )
        return 0

    if args.push_node is not None:
        push_worker_node(args.push_node, state, tokens, usernames, wait_if_running=True)
        save_fleet_state(bucket, state)
        return 0

    if args.push_standby:
        state["phase"] = "standby"
        push_standby_node(state, tokens, usernames, wait_if_running=True)
        save_fleet_state(bucket, state)
        return 0

    if args.reconcile:
        reconcile_fleet(bucket, state, tokens, usernames)
        return 0

    print_fleet_status(state, tokens, usernames)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

