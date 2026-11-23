"""
Distributed 20-Node Kaggle Fleet Orchestrator (`fleet_manager.py`).

Key architectural improvements over `tdv_aio`:
1. Stores orchestrator state in the Hugging Face Storage Bucket at `control/state.json`
   (eliminating git commit/rebase race conditions in GitHub Actions).
2. Verifies per-node completion directly against yearly CSV logs (`log/node_{x}_{yyyy}.csv`)
   in the HF Bucket (`1`, `E1`, `E2`, `E3` vs `""`/`E4`).
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
    BOOT_VERIFY_POLL_INTERVAL_SECONDS,
    BOOT_VERIFY_STABLE_SECONDS,
    BOOT_VERIFY_TIMEOUT_SECONDS,
    BUCKET_STATE_PATH,
    DEFAULT_GITHUB_REPO,
    DEFAULT_HF_BUCKET_ID,
    ERROR_COOLDOWN_SECONDS,
    MAX_ALLOWED_MISSING_RATE_PCT,
    MAX_CONSECUTIVE_NODE_FAILURES,
    NODES_PER_KAGGLE_ACCOUNT,
    RATE_LIMIT_COOLDOWN_SECONDS,
    TOTAL_KAGGLE_ACCOUNTS,
    TOTAL_NOTEBOOKS,
    compute_next_monday_utc,
    compute_weekly_run_tag,
)
from tdv.pipeline import count_node_missing_tasks, count_node_task_stats

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


def verify_kernel_boot(
    username: str,
    slug: str,
    api_token: str,
    timeout_seconds: int = BOOT_VERIFY_TIMEOUT_SECONDS,
    stable_seconds: int = BOOT_VERIFY_STABLE_SECONDS,
    poll_interval: int = BOOT_VERIFY_POLL_INTERVAL_SECONDS,
    not_found_grace_seconds: int = 25,
    treat_complete_as_failure: bool = True,
) -> tuple[bool, str]:
    """
    Poll Kaggle after a kernel push until the kernel enters `running` and stays
    `running` continuously for `stable_seconds`.

    Catches immediate Kaggle container-provisioning crashes (such as stale
    service-account `SignBlob failed` errors) before GitHub Actions exits.
    """
    start_ts = time.time()
    deadline = start_ts + max(timeout_seconds, stable_seconds + poll_interval)
    running_since: float | None = None
    last_state = "unknown"

    # Short initial pause so Kaggle registers the newly pushed version
    initial_pause = min(poll_interval, 8)
    if initial_pause > 0:
        time.sleep(initial_pause)

    while time.time() < deadline:
        state = get_kernel_state(username, slug, api_token)
        last_state = state
        elapsed = time.time() - start_ts

        if state in ("error", "cancelacknowledged", "cancelrequested"):
            print(
                f"[FleetManager] Boot check FAILED for {username}/{slug}: state='{state}' "
                f"(after {elapsed:.0f}s).",
                flush=True,
            )
            return False, state

        if state == "complete":
            if treat_complete_as_failure:
                print(
                    f"[FleetManager] Boot check FAILED for {username}/{slug}: exited early "
                    f"with state='complete' (after {elapsed:.0f}s).",
                    flush=True,
                )
                return False, state
            print(
                f"[FleetManager] Boot check PASSED for {username}/{slug}: completed cleanly "
                f"(after {elapsed:.0f}s).",
                flush=True,
            )
            return True, state

        if state == "not_found" and elapsed >= not_found_grace_seconds:
            print(
                f"[FleetManager] Boot check FAILED for {username}/{slug}: state='not_found' "
                f"after {elapsed:.0f}s.",
                flush=True,
            )
            return False, state

        if state == "running":
            now = time.time()
            if running_since is None:
                running_since = now
                deadline = max(deadline, running_since + stable_seconds + poll_interval)
                print(
                    f"[FleetManager] {username}/{slug} entered 'running' (after {elapsed:.0f}s); "
                    f"confirming stability for {stable_seconds}s...",
                    flush=True,
                )
            elif now - running_since >= stable_seconds:
                print(
                    f"[FleetManager] Boot check PASSED for {username}/{slug} "
                    f"(stable 'running' for {now - running_since:.0f}s).",
                    flush=True,
                )
                return True, "running"
        else:
            running_since = None

        remaining = deadline - time.time()
        if remaining <= 0:
            break
        time.sleep(min(poll_interval, remaining))

    if running_since is not None:
        final_state = get_kernel_state(username, slug, api_token)
        last_state = final_state
        if final_state == "running" and (time.time() - running_since) >= stable_seconds:
            return True, "running"

    print(
        f"[FleetManager] Boot check TIMED OUT for {username}/{slug}: last_state='{last_state}'.",
        flush=True,
    )
    return False, last_state


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

    # 1. Try kagglesdk cancel_kernel_session (if running) + delete_kernel in-process
    try:
        from kagglesdk import KaggleClient
        from kagglesdk.kernels.types.kernels_api_service import (
            ApiCancelKernelSessionRequest,
            ApiDeleteKernelRequest,
            ApiGetKernelRequest,
        )

        client = KaggleClient(api_token=api_token)
        if k_state in ("running", "queued"):
            try:
                gk = ApiGetKernelRequest()
                gk.user_name = username
                gk.kernel_slug = slug
                k_obj = client.kernels.kernels_api_client.get_kernel(gk)
                sid = getattr(k_obj, "kernel_session_id", None) or getattr(
                    getattr(k_obj, "metadata", None), "current_running_version_id", None
                )
                if sid:
                    creq = ApiCancelKernelSessionRequest()
                    creq.kernel_session_id = int(sid)
                    client.kernels.kernels_api_client.cancel_kernel_session(creq)
                    print(f"[FleetManager] Canceled running session {sid} for {username}/{slug}", flush=True)
                    time.sleep(3)
            except Exception as cancel_exc:
                print(f"[FleetManager] cancel_kernel_session note for {username}/{slug}: {cancel_exc}", flush=True)

        req = ApiDeleteKernelRequest()
        req.user_name = username
        req.kernel_slug = slug
        client.kernels.kernels_api_client.delete_kernel(req)
        print(f"[FleetManager] Deleted kernel {username}/{slug} (was '{k_state}')", flush=True)
        return True
    except Exception as exc:
        if "404" in str(exc) or "not found" in str(exc).lower():
            return True
        print(f"[FleetManager] kagglesdk delete_kernel note for {username}/{slug}: {exc}", flush=True)

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
            print(f"[FleetManager] Deleted kernel {username}/{slug} via CLI (was '{k_state}')", flush=True)
            return True
        print(
            f"[FleetManager] Warning: CLI delete failed for {username}/{slug}: {res.stdout} {res.stderr}",
            flush=True,
        )
    except Exception as exc:
        print(f"[FleetManager] Warning: CLI delete exception for {username}/{slug}: {exc}", flush=True)
    return False


def delete_all_worker_kernels(
    tokens: list[str],
    usernames: dict[int, str],
    wait_if_running: bool = False,
    verify_stopped: bool = False,
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

    if verify_stopped:
        deadline = time.time() + 45
        pending_nodes = list(range(TOTAL_NOTEBOOKS))
        while pending_nodes and time.time() < deadline:
            still_running: list[int] = []
            for node_id in pending_nodes:
                acc_idx = get_account_index_for_node(node_id)
                api_token = tokens[acc_idx] if acc_idx < len(tokens) else ""
                username = usernames.get(acc_idx, "")
                if not api_token or not username:
                    continue
                st = get_kernel_state(username, f"tdv-node-{node_id}", api_token)
                if st in ("running", "queued", "cancelrequested"):
                    still_running.append(node_id)
                    delete_kernel_if_exists(
                        username=username,
                        slug=f"tdv-node-{node_id}",
                        api_token=api_token,
                        wait_if_running=False,
                    )
            pending_nodes = still_running
            if pending_nodes:
                print(
                    f"[FleetManager] Waiting 5s for {len(pending_nodes)} deleted node(s) "
                    f"({pending_nodes}) to finish stopping on Kaggle...",
                    flush=True,
                )
                time.sleep(5)


def delete_all_standby_kernels(
    tokens: list[str],
    usernames: dict[int, str],
    exclude_account_idx: int | None = None,
    wait_account_idx: int | None = None,
    wait_seconds: int = 35,
) -> None:
    """Delete `tdv-standby` kernels across Kaggle accounts (optionally excluding the active one)."""
    for acc_idx in range(TOTAL_KAGGLE_ACCOUNTS):
        if exclude_account_idx is not None and acc_idx == exclude_account_idx:
            continue
        api_token = tokens[acc_idx] if acc_idx < len(tokens) else ""
        username = usernames.get(acc_idx, "")
        if not api_token or not username:
            continue
        if wait_account_idx is not None and acc_idx == wait_account_idx:
            wait_until_kernel_not_running(
                username,
                "tdv-standby",
                api_token,
                max_wait_seconds=wait_seconds,
                poll_interval=5,
            )
        delete_kernel_if_exists(
            username=username,
            slug="tdv-standby",
            api_token=api_token,
            wait_if_running=False,
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


def load_and_sync_fleet_state(bucket: BucketClient, sync_logs: bool = True) -> dict[str, Any]:
    """
    Load `control/state.json` from the HF Bucket and verify any `DONE`/`ERROR`
    nodes against their yearly CSV logs (`log/node_{i}_{yyyy}.csv`).
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

    if sync_logs and phase != "standby":
        try:
            for i in range(TOTAL_NOTEBOOKS):
                n_rec = nodes[str(i)]
                if n_rec.get("status") in ("DONE", "ERROR"):
                    if n_rec.get("integrity_verified") == run_tag and n_rec.get("status") == "DONE":
                        n_rec["status"] = "DONE"
                        n_rec["retries"] = 0
                        continue
                    try:
                        missing_cnt, total_tasks, missing_rate_pct = count_node_task_stats(bucket, i, run_tag)
                    except Exception as ver_exc:
                        print(
                            f"[FleetManager] Warning: could not verify integrity for Node {i}: {ver_exc}",
                            flush=True,
                        )
                        missing_cnt, total_tasks, missing_rate_pct = 0, 0, 0.0

                    n_rec["missing_tasks"] = missing_cnt
                    n_rec["total_tasks"] = total_tasks
                    n_rec["missing_rate_pct"] = missing_rate_pct

                    if total_tasks > 0 and missing_rate_pct <= MAX_ALLOWED_MISSING_RATE_PCT:
                        n_rec["status"] = "DONE"
                        n_rec["retries"] = 0
                        n_rec["integrity_verified"] = run_tag
                        n_rec["updated_at"] = now_iso
                    else:
                        print(
                            f"[FleetManager] Integrity check for Node {i} ({run_tag}): "
                            f"{missing_cnt}/{total_tasks} task(s) missing ({missing_rate_pct:.3f}% > {MAX_ALLOWED_MISSING_RATE_PCT}%)! "
                            f"Marking Node {i} IDLE to heal missing symbols.",
                            flush=True,
                        )
                        if n_rec.get("status") == "DONE":
                            n_rec["status"] = "IDLE"
                            n_rec.pop("integrity_verified", None)
                        n_rec["updated_at"] = now_iso
        except Exception as exc:
            print(f"[FleetManager] Warning: could not verify node CSV logs: {exc}", flush=True)

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
    # Premium cookie/WS URL are only used during the initial run
    if phase_mode != "initial":
        ult_session = ""
        ult_sign = ""
        ult_ws_url = ""
    else:
        ult_ws_url = (
            os.environ.get("TV_PREMIUM_WS_URL", "").strip()
            or os.environ.get("TV_ULTIMATE_WS_URL", "").strip()
        )

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
os.environ["TV_PREMIUM_WS_URL"] = {json.dumps(ult_ws_url)}

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


def build_standby_runner_script(
    account_idx: int = 0,
    session_id: str = "",
) -> str:
    """Generate the self-bootstrapping script for `tdv-standby`."""
    gh_pat = os.environ.get("GH_PAT", "").strip()
    gh_repo = os.environ.get("GITHUB_REPO", DEFAULT_GITHUB_REPO).strip()
    hf_token = os.environ.get("HF_TOKEN", "").strip()
    hf_bucket = os.environ.get("HF_BUCKET_ID", DEFAULT_HF_BUCKET_ID).strip()

    return f'''import os
import shutil
import subprocess
import sys
import traceback

os.environ["HF_HOME"] = "/tmp/hf_cache"
os.environ["GH_PAT"] = {json.dumps(gh_pat)}
os.environ["GITHUB_REPO"] = {json.dumps(gh_repo)}
os.environ["HF_TOKEN"] = {json.dumps(hf_token)}
os.environ["HF_BUCKET_ID"] = {json.dumps(hf_bucket)}
os.environ["STANDBY_ACCOUNT_IDX"] = "{int(account_idx)}"
os.environ["STANDBY_SESSION_ID"] = {json.dumps(session_id)}

print("=== [TDV Standby Runner] Bootstrapping Standby Master Clock (Account {int(account_idx)}, Session {session_id}) ===", flush=True)

repo_dir = "/tmp/tdv_repo"
exit_code = 1
try:
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "requests", "huggingface_hub>=1.5.0"],
        check=True,
    )
    if os.path.exists(repo_dir):
        shutil.rmtree(repo_dir, ignore_errors=True)

    clone_url = f"https://{{os.environ['GH_PAT']}}@github.com/{gh_repo}.git" if os.environ.get("GH_PAT") else "https://github.com/{gh_repo}.git"
    subprocess.run(["git", "clone", "--depth", "1", clone_url, repo_dir], check=True)

    proc = subprocess.run([sys.executable, "-u", "-m", "tdv.standby_clock"], cwd=repo_dir)
    exit_code = proc.returncode
except Exception as exc:
    print(f"[TDV Standby Runner] Bootstrap/runtime exception: {{exc}}", flush=True)
    traceback.print_exc()
    exit_code = 1
finally:
    if exit_code != 0 and os.environ.get("GH_PAT"):
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
                json={{
                    "event_type": "node_callback",
                    "client_payload": {{
                        "node_id": 99,
                        "status": st,
                        "phase": ph,
                        "account_idx": {int(account_idx)},
                        "session_id": {json.dumps(session_id)},
                    }},
                }},
                timeout=20,
            )
        except Exception:
            pass
    shutil.rmtree(repo_dir, ignore_errors=True)
    shutil.rmtree("/tmp/hf_cache", ignore_errors=True)

sys.exit(exit_code)
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
    force_push: bool = False,
    verify_boot: bool = False,
    max_boot_retries: int = 3,
) -> bool:
    """Push `tdv-node-{node_id}` to its assigned Kaggle account (with optional boot verification)."""
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

    if not force_push and k_state in ("running", "queued"):
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

    attempts = max(1, max_boot_retries) if verify_boot else 1
    for attempt in range(1, attempts + 1):
        if k_state in ("error", "cancelacknowledged") or attempt > 1:
            delete_kernel_if_exists(username, slug, api_token, wait_if_running=False)

        ok = _push_kernel_via_cli(username, slug, title, script_code, api_token)
        if not ok:
            if attempt < attempts:
                time.sleep(10)
            continue

        if verify_boot:
            boot_ok, boot_state = verify_kernel_boot(
                username=username,
                slug=slug,
                api_token=api_token,
                treat_complete_as_failure=False,
            )
            if not boot_ok:
                print(
                    f"[FleetManager] Worker {username}/{slug} failed boot verification "
                    f"(attempt {attempt}/{attempts}, state='{boot_state}').",
                    flush=True,
                )
                delete_kernel_if_exists(username, slug, api_token, wait_if_running=False)
                if attempt < attempts:
                    time.sleep(10)
                continue

        now_iso = datetime.now(timezone.utc).isoformat()
        state["nodes"][str(node_id)]["status"] = "RUNNING"
        state["nodes"][str(node_id)]["launched_at"] = now_iso
        state["nodes"][str(node_id)]["updated_at"] = now_iso
        return True

    return False


def _parse_iso_utc(raw: Any) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _is_weekly_due(state: dict[str, Any], now_utc: datetime) -> tuple[bool, str]:
    """
    Determine whether the weekly cycle `WEEK_<WWYY>` is due to trigger now.

    Returns `(is_due, expected_week_tag)`. Handles both Monday UTC and late
    recovery after Monday using `state["standby"]["next_weekly_utc"]`.
    """
    expected_tag = compute_weekly_run_tag(now_utc)
    if str(state.get("run_tag", "")) == expected_tag:
        return False, expected_tag

    next_weekly_dt = _parse_iso_utc(state.get("standby", {}).get("next_weekly_utc"))
    if next_weekly_dt is not None and now_utc >= next_weekly_dt:
        return True, expected_tag

    if now_utc.weekday() == 0:
        return True, expected_tag

    return False, expected_tag


def push_standby_node(
    state: dict[str, Any],
    tokens: list[str],
    usernames: dict[int, str],
    bucket: BucketClient | None = None,
    start_account_idx: int | None = None,
    rotate_from_account_idx: int | None = None,
    wait_if_running: bool = False,
    verify_boot: bool = True,
    max_rounds: int = 2,
) -> bool:
    """
    Push `tdv-standby` rotating round-robin across Kaggle accounts
    (`0 -> 1 -> 2 -> 3 -> 0`) with post-push boot verification and automatic
    failover across accounts if Kaggle container provisioning fails.
    """
    configured_accounts = [
        idx
        for idx in range(TOTAL_KAGGLE_ACCOUNTS)
        if idx < len(tokens) and tokens[idx] and usernames.get(idx)
    ]
    if not configured_accounts:
        print("[FleetManager] Cannot push tdv-standby: no Kaggle accounts configured.", flush=True)
        return False

    sb_state = state.setdefault("standby", {})
    now_dt = datetime.now(timezone.utc)
    existing_next_weekly = _parse_iso_utc(sb_state.get("next_weekly_utc"))
    if existing_next_weekly is not None and existing_next_weekly > now_dt:
        next_weekly_iso = existing_next_weekly.isoformat()
    else:
        next_weekly_iso = compute_next_monday_utc(now_dt).isoformat()

    raw_last_acc = sb_state.get("account_idx")
    last_acc = int(raw_last_acc) if isinstance(raw_last_acc, int) or str(raw_last_acc).isdigit() else None

    # During passive reconciliation (no explicit start/rotation), keep any already-running standby
    if start_account_idx is None and rotate_from_account_idx is None:
        check_order = configured_accounts
        if last_acc in configured_accounts:
            pos = configured_accounts.index(last_acc)
            check_order = configured_accounts[pos:] + configured_accounts[:pos]
        for acc_idx in check_order:
            uname = usernames[acc_idx]
            tok = tokens[acc_idx]
            k_state = get_kernel_state(uname, "tdv-standby", tok)
            if k_state in ("running", "queued"):
                print(
                    f"[FleetManager] Account {acc_idx} ({uname}/tdv-standby) is already '{k_state}'.",
                    flush=True,
                )
                now_iso = datetime.now(timezone.utc).isoformat()
                sb_state["status"] = "RUNNING"
                sb_state["account_idx"] = acc_idx
                sb_state["username"] = uname
                sb_state.setdefault("next_weekly_utc", next_weekly_iso)
                sb_state["updated_at"] = now_iso
                if bucket is not None:
                    save_fleet_state(bucket, state)
                return True

    # Determine starting account in round-robin order (0 -> 1 -> 2 -> 3 -> 0)
    if rotate_from_account_idx is not None:
        base_start = (int(rotate_from_account_idx) + 1) % TOTAL_KAGGLE_ACCOUNTS
    elif start_account_idx is not None:
        base_start = int(start_account_idx) % TOTAL_KAGGLE_ACCOUNTS
    elif last_acc is not None:
        # Recovering from a dead/crashed standby -> advance to the next account
        base_start = (last_acc + 1) % TOTAL_KAGGLE_ACCOUNTS
    else:
        base_start = 0

    ordered_accounts: list[int] = []
    for offset in range(TOTAL_KAGGLE_ACCOUNTS):
        cand = (base_start + offset) % TOTAL_KAGGLE_ACCOUNTS
        if cand in configured_accounts:
            ordered_accounts.append(cand)

    slug = "tdv-standby"
    title = "TDV Standby"
    print(
        f"[FleetManager] Launching {slug} with round-robin order {ordered_accounts} "
        f"(rotate_from={rotate_from_account_idx}, verify_boot={verify_boot})...",
        flush=True,
    )

    for round_num in range(1, max(1, max_rounds) + 1):
        for acc_idx in ordered_accounts:
            username = usernames[acc_idx]
            api_token = tokens[acc_idx]

            # If failing all the way back to the retiring account, stop/delete it before re-pushing
            if rotate_from_account_idx is not None and acc_idx == int(rotate_from_account_idx):
                if wait_if_running:
                    wait_until_kernel_not_running(username, slug, api_token, max_wait_seconds=60, poll_interval=5)
                delete_kernel_if_exists(username, slug, api_token, wait_if_running=False)
            else:
                k_state = get_kernel_state(username, slug, api_token)
                if k_state != "not_found":
                    delete_kernel_if_exists(username, slug, api_token, wait_if_running=False)

            session_id = f"sb_{acc_idx}_{int(time.time())}"
            script_code = build_standby_runner_script(account_idx=acc_idx, session_id=session_id)
            print(
                f"[FleetManager] Standby push attempt (round {round_num}/{max_rounds}) -> "
                f"Account {acc_idx} ({username}/{slug}, session={session_id})...",
                flush=True,
            )
            pushed = _push_kernel_via_cli(username, slug, title, script_code, api_token)
            if not pushed:
                print(
                    f"[FleetManager] Push failed on Account {acc_idx} ({username}/{slug}); failing over...",
                    flush=True,
                )
                time.sleep(5)
                continue

            if verify_boot:
                boot_ok, boot_state = verify_kernel_boot(
                    username=username,
                    slug=slug,
                    api_token=api_token,
                    treat_complete_as_failure=True,
                )
                if not boot_ok:
                    print(
                        f"[FleetManager] Standby boot FAILED on Account {acc_idx} "
                        f"({username}/{slug} -> '{boot_state}'). Deleting and failing over to next account...",
                        flush=True,
                    )
                    delete_kernel_if_exists(username, slug, api_token, wait_if_running=False)
                    time.sleep(5)
                    continue

            # Verified running! Persist state BEFORE cleaning up old standby so the retiring
            # session sees the new session_id in control/state.json and exits cleanly.
            now_iso = datetime.now(timezone.utc).isoformat()
            state["standby"] = {
                "status": "RUNNING",
                "account_idx": acc_idx,
                "username": username,
                "session_id": session_id,
                "next_weekly_utc": next_weekly_iso,
                "launched_at": now_iso,
                "updated_at": now_iso,
            }
            if bucket is not None:
                save_fleet_state(bucket, state)

            delete_all_standby_kernels(
                tokens=tokens,
                usernames=usernames,
                exclude_account_idx=acc_idx,
                wait_account_idx=rotate_from_account_idx if rotate_from_account_idx != acc_idx else None,
                wait_seconds=35,
            )
            print(
                f"[FleetManager] Standby active on Account {acc_idx} ({username}/{slug}, "
                f"session={session_id}, next_weekly={next_weekly_iso}).",
                flush=True,
            )
            return True

    print(
        "[FleetManager] CRITICAL: Failed to boot tdv-standby on ALL configured Kaggle accounts!",
        flush=True,
    )
    now_iso = datetime.now(timezone.utc).isoformat()
    state["standby"] = {
        "status": "ERROR",
        "account_idx": last_acc if last_acc is not None else 0,
        "next_weekly_utc": next_weekly_iso,
        "updated_at": now_iso,
    }
    if bucket is not None:
        save_fleet_state(bucket, state)
    return False


def _enter_standby_phase(
    bucket: BucketClient,
    state: dict[str, Any],
    tokens: list[str],
    usernames: dict[int, str],
    wait_workers: bool = True,
) -> bool:
    """
    Clean up finished worker notebooks, transition `state` to `'standby'`,
    record the next Monday `00:00:00 UTC` deadline, and start standby rotation
    at Account 0 (failing over automatically if Account 0 fails to boot).
    """
    delete_all_worker_kernels(tokens, usernames, wait_if_running=wait_workers)
    now_dt = datetime.now(timezone.utc)
    state["phase"] = "standby"
    state["standby"] = {
        "status": "STARTING",
        "account_idx": 0,
        "next_weekly_utc": compute_next_monday_utc(now_dt).isoformat(),
        "updated_at": now_dt.isoformat(),
    }
    save_fleet_state(bucket, state)
    ok = push_standby_node(
        state=state,
        tokens=tokens,
        usernames=usernames,
        bucket=bucket,
        start_account_idx=0,
        verify_boot=True,
    )
    save_fleet_state(bucket, state)
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
    force_push: bool = False,
    verify_batch_boot: bool = True,
) -> bool:
    """
    Reconcile all 20 worker nodes (or `tdv-standby` when in `standby` phase).
    Returns `True` on success, `False` if standby failed to boot across all accounts.
    """
    phase = str(state.get("phase", "initial"))
    run_tag = str(state.get("run_tag", "INITIAL" if phase == "initial" else phase))
    nodes = state["nodes"]

    # Check if all 20 nodes are DONE
    if phase != "standby" and all(nodes[str(i)].get("status") == "DONE" for i in range(TOTAL_NOTEBOOKS)):
        print(
            f"[FleetManager] All {TOTAL_NOTEBOOKS} nodes completed {run_tag}! "
            f"Deleting finished worker notebooks, transitioning fleet to 'standby' phase, "
            f"and launching Standby Master Clock starting on Account 0.",
            flush=True,
        )
        return _enter_standby_phase(bucket, state, tokens, usernames, wait_workers=True)

    if state.get("phase") == "standby":
        now_utc = datetime.now(timezone.utc)
        weekly_due, expected_tag = _is_weekly_due(state, now_utc)
        if weekly_due:
            print(
                f"[FleetManager] Standby reconcile detected overdue/due weekly cycle "
                f"({expected_tag}) -> triggering TRIGGER_WEEKLY!",
                flush=True,
            )
            return handle_node_callback(
                node_id=99,
                callback_status="TRIGGER_WEEKLY",
                bucket=bucket,
                state=state,
                tokens=tokens,
                usernames=usernames,
            )
        ok = push_standby_node(
            state=state,
            tokens=tokens,
            usernames=usernames,
            bucket=bucket,
            verify_boot=True,
        )
        save_fleet_state(bucket, state)
        return ok

    pushed_nodes: list[int] = []
    for i in range(TOTAL_NOTEBOOKS):
        node_info = nodes[str(i)]
        status = node_info.get("status", "IDLE")
        retries = int(node_info.get("retries", 0))

        if status == "DONE":
            continue
        if status == "AUTH_FAILED":
            print(f"[FleetManager] Node {i} is marked AUTH_FAILED. Skipping until credentials fixed.", flush=True)
            continue
        if status == "IDLE" and retries >= MAX_CONSECUTIVE_NODE_FAILURES:
            retries = 0
            node_info["retries"] = 0
        if not force_push and retries > MAX_CONSECUTIVE_NODE_FAILURES:
            print(
                f"[FleetManager] Node {i} exceeded max consecutive failures ({retries}). Skipping.",
                flush=True,
            )
            continue

        acc_idx = get_account_index_for_node(i)
        tok = tokens[acc_idx] if acc_idx < len(tokens) else ""
        uname = usernames.get(acc_idx, "")
        live_k = get_kernel_state(uname, f"tdv-node-{i}", tok)

        if not force_push and live_k in ("running", "queued"):
            node_info["status"] = "RUNNING"
            continue

        print(
            f"[FleetManager] Reconciling Node {i} (stored={status}, live={live_k}, force={force_push}) -> pushing kernel...",
            flush=True,
        )
        ok = push_worker_node(
            i,
            state,
            tokens,
            usernames,
            start_delay_seconds=0,
            wait_if_running=False,
            force_push=force_push,
            verify_boot=False,
        )
        if ok:
            pushed_nodes.append(i)
        time.sleep(2)

    save_fleet_state(bucket, state)

    # Batch boot verification: catch immediate Kaggle provisioning crashes (e.g. SignBlob)
    # on any freshly pushed worker nodes and retry them before exiting.
    if verify_batch_boot and pushed_nodes:
        pending_check = list(pushed_nodes)
        for sweep in range(1, 3):
            print(
                f"[FleetManager] Batch boot check sweep {sweep}/2 for {len(pending_check)} "
                f"pushed worker(s): sleeping 30s...",
                flush=True,
            )
            time.sleep(30)
            failed_nodes: list[int] = []
            still_queued: list[int] = []
            for node_id in pending_check:
                acc_idx = get_account_index_for_node(node_id)
                tok = tokens[acc_idx] if acc_idx < len(tokens) else ""
                uname = usernames.get(acc_idx, "")
                if not tok or not uname:
                    continue
                live_k = get_kernel_state(uname, f"tdv-node-{node_id}", tok)
                if live_k in ("error", "cancelacknowledged"):
                    failed_nodes.append(node_id)
                elif live_k in ("queued", "unknown", "not_found"):
                    still_queued.append(node_id)

            if failed_nodes:
                print(
                    f"[FleetManager] Batch boot check detected immediate failure on worker(s) "
                    f"{failed_nodes} -> deleting and re-pushing...",
                    flush=True,
                )
                for node_id in failed_nodes:
                    acc_idx = get_account_index_for_node(node_id)
                    tok = tokens[acc_idx] if acc_idx < len(tokens) else ""
                    uname = usernames.get(acc_idx, "")
                    delete_kernel_if_exists(uname, f"tdv-node-{node_id}", tok, wait_if_running=False)
                    push_worker_node(
                        node_id,
                        state,
                        tokens,
                        usernames,
                        start_delay_seconds=0,
                        wait_if_running=False,
                        force_push=True,
                        verify_boot=False,
                    )
                    time.sleep(2)
                save_fleet_state(bucket, state)

            pending_check = failed_nodes + still_queued
            if not pending_check:
                print("[FleetManager] All pushed worker nodes verified healthy!", flush=True)
                break

    return True


def _is_stale_teardown_callback(
    node_id: int,
    cb: str,
    node_rec: dict[str, Any],
    tokens: list[str],
    usernames: dict[int, str],
    now_utc: datetime,
) -> bool:
    """
    Detect if a `TIMEOUT`, `ERROR`, or `RATE_LIMITED` callback was emitted by a dying
    old container that was just replaced by `TRIGGER_WEEKLY` (or a recent push).
    """
    launched_str = str(node_rec.get("launched_at", "")).strip()
    if not launched_str:
        return False
    try:
        launched_dt = datetime.fromisoformat(launched_str)
        age_sec = (now_utc - launched_dt).total_seconds()
    except Exception:
        return False

    acc_idx = get_account_index_for_node(node_id)
    tok = tokens[acc_idx] if acc_idx < len(tokens) else ""
    uname = usernames.get(acc_idx, "")
    if not tok or not uname:
        return False

    slug = f"tdv-node-{node_id}"
    # 1. A real 11h 45m TIMEOUT cannot occur within 30 minutes of launch
    if cb == "TIMEOUT" and age_sec < 1800:
        live_k = get_kernel_state(uname, slug, tok)
        if live_k in ("running", "queued"):
            print(
                f"[FleetManager] Ignoring stale {cb} callback for Node {node_id} "
                f"(new kernel was launched {age_sec:.0f}s ago and is currently '{live_k}').",
                flush=True,
            )
            return True

    # 2. If an ERROR/RATE_LIMITED arrives within 5 minutes of a fresh launch while the
    #    new kernel is still running after a 15s grace check, it came from the killed old container
    if cb in ("ERROR", "RATE_LIMITED") and age_sec < 300:
        live_k = get_kernel_state(uname, slug, tok)
        if live_k in ("running", "queued"):
            time.sleep(15)
            live_k_after = get_kernel_state(uname, slug, tok)
            if live_k_after in ("running", "queued"):
                print(
                    f"[FleetManager] Ignoring stale {cb} callback for Node {node_id} "
                    f"(new kernel was launched {age_sec:.0f}s ago and remains '{live_k_after}').",
                    flush=True,
                )
                return True

    return False


def handle_node_callback(
    node_id: int,
    callback_status: str,
    bucket: BucketClient,
    state: dict[str, Any],
    tokens: list[str],
    usernames: dict[int, str],
    callback_session_id: str = "",
    callback_account_idx: int | None = None,
    force_weekly: bool = False,
) -> bool:
    """
    Process a `repository_dispatch` callback from a Kaggle worker or Standby Clock.
    Returns `True` on success, `False` if standby failed to boot across all accounts.
    """
    cb = callback_status.strip().upper()
    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.isoformat()
    print(
        f"[FleetManager] Processing callback: node_id={node_id}, status={cb}, "
        f"acc={callback_account_idx}, sid={callback_session_id or 'n/a'}",
        flush=True,
    )

    # Safety promotion: if REARM_STANDBY arrives when this week's cycle is due/overdue,
    # promote it directly to TRIGGER_WEEKLY.
    if cb == "REARM_STANDBY":
        weekly_due, expected_week_tag = _is_weekly_due(state, now_utc)
        if weekly_due:
            print(
                f"[FleetManager] Promoting due REARM_STANDBY -> TRIGGER_WEEKLY ({expected_week_tag})",
                flush=True,
            )
            cb = "TRIGGER_WEEKLY"

    # 1. Standby Clock Callbacks
    if cb == "REARM_STANDBY":
        if state.get("phase") != "standby":
            print(
                f"[FleetManager] Ignoring stale REARM_STANDBY because fleet phase is already '{state.get('phase')}'.",
                flush=True,
            )
            return True

        sb_rec = state.get("standby", {})
        active_sid = str(sb_rec.get("session_id", "")).strip()
        raw_active_acc = sb_rec.get("account_idx")
        active_acc = (
            int(raw_active_acc)
            if isinstance(raw_active_acc, int) or str(raw_active_acc).isdigit()
            else None
        )

        # Idempotency guard: ignore duplicate/retried REARM_STANDBY from an already-rotated session
        if active_acc is not None and active_acc < len(tokens) and tokens[active_acc] and usernames.get(active_acc):
            is_stale_sid = bool(callback_session_id and active_sid and callback_session_id != active_sid)
            launched_dt = _parse_iso_utc(sb_rec.get("launched_at"))
            is_recent_launch = (
                not callback_session_id
                and launched_dt is not None
                and (now_utc - launched_dt).total_seconds() < 600
            )
            if is_stale_sid or is_recent_launch:
                live_sb = get_kernel_state(usernames[active_acc], "tdv-standby", tokens[active_acc])
                if live_sb in ("running", "queued"):
                    print(
                        f"[FleetManager] Ignoring duplicate/stale REARM_STANDBY (cb_sid={callback_session_id or 'none'}) "
                        f"because Account {active_acc} standby (sid={active_sid}) is already '{live_sb}'.",
                        flush=True,
                    )
                    return True

        prev_acc = callback_account_idx if callback_account_idx is not None else (active_acc if active_acc is not None else 0)
        state["phase"] = "standby"
        ok = push_standby_node(
            state=state,
            tokens=tokens,
            usernames=usernames,
            bucket=bucket,
            rotate_from_account_idx=prev_acc,
            wait_if_running=True,
            verify_boot=True,
        )
        save_fleet_state(bucket, state)
        return ok

    if cb == "TRIGGER_WEEKLY":
        week_tag = compute_weekly_run_tag(now_utc)
        if not force_weekly:
            if state.get("phase") == week_tag:
                print(
                    f"[FleetManager] TRIGGER_WEEKLY for {week_tag} already active -> reconciling without reset.",
                    flush=True,
                )
                return reconcile_fleet(bucket, state, tokens, usernames)
            if state.get("phase") == "standby" and str(state.get("run_tag", "")) == week_tag:
                print(
                    f"[FleetManager] Ignoring duplicate TRIGGER_WEEKLY: {week_tag} already completed this week.",
                    flush=True,
                )
                return push_standby_node(state=state, tokens=tokens, usernames=usernames, bucket=bucket, verify_boot=True)

        print(
            f"[FleetManager] TRIGGER_WEEKLY received -> deleting tdv-standby (all accounts) and old worker notebooks, "
            f"then starting {week_tag} across all 20 nodes (appending {week_tag} column to yearly CSV logs)!",
            flush=True,
        )
        raw_active_acc = state.get("standby", {}).get("account_idx")
        active_sb_acc = (
            callback_account_idx
            if callback_account_idx is not None
            else (int(raw_active_acc) if isinstance(raw_active_acc, int) or str(raw_active_acc).isdigit() else 0)
        )
        delete_all_standby_kernels(
            tokens=tokens,
            usernames=usernames,
            wait_account_idx=active_sb_acc,
            wait_seconds=25,
        )
        delete_all_worker_kernels(tokens, usernames, wait_if_running=False, verify_stopped=True)
        try:
            all_log_paths = bucket.list_paths(prefix="log", recursive=True)
            legacy_week_paths = [p for p in all_log_paths if f"/NODE_" in f"/{p}" and f"/{week_tag}/" in p]
            if legacy_week_paths:
                bucket.delete_paths(legacy_week_paths)
        except Exception as exc:
            print(f"[FleetManager] Warning: could not check legacy {week_tag} logs: {exc}", flush=True)
        new_state = default_fleet_state(phase=week_tag, run_tag=week_tag)
        state.clear()
        state.update(new_state)
        save_fleet_state(bucket, state)
        return reconcile_fleet(bucket, state, tokens, usernames, force_push=True)

    # 2. Worker Node Callbacks (0..19)
    node_key = str(node_id)
    node_rec = state["nodes"].setdefault(
        node_key, {"status": "IDLE", "retries": 0, "updated_at": now_iso}
    )

    if cb in ("TIMEOUT", "ERROR", "RATE_LIMITED") and _is_stale_teardown_callback(
        node_id, cb, node_rec, tokens, usernames, now_utc
    ):
        return True

    node_rec["updated_at"] = now_iso

    if cb == "DONE":
        phase_str = str(state.get("phase", "initial"))
        run_tag_str = str(state.get("run_tag", "INITIAL" if phase_str == "initial" else phase_str))
        try:
            missing_cnt, total_tasks, missing_rate_pct = count_node_task_stats(bucket, node_id, run_tag_str)
        except Exception as ver_exc:
            print(f"[FleetManager] Warning: integrity check error on Node {node_id}: {ver_exc}", flush=True)
            missing_cnt, total_tasks, missing_rate_pct = 0, 0, 0.0

        node_rec["missing_tasks"] = missing_cnt
        node_rec["total_tasks"] = total_tasks
        node_rec["missing_rate_pct"] = missing_rate_pct

        if missing_rate_pct > MAX_ALLOWED_MISSING_RATE_PCT:
            retries = int(node_rec.get("retries", 0)) + 1
            node_rec["status"] = "IDLE"
            node_rec["retries"] = retries
            node_rec.pop("integrity_verified", None)
            save_fleet_state(bucket, state)
            if retries <= MAX_CONSECUTIVE_NODE_FAILURES:
                print(
                    f"[FleetManager] Node {node_id} reported DONE but has {missing_cnt}/{total_tasks} incomplete "
                    f"tasks ({missing_rate_pct:.3f}% > {MAX_ALLOWED_MISSING_RATE_PCT}%) in {run_tag_str} (heal attempt {retries}/{MAX_CONSECUTIVE_NODE_FAILURES})! "
                    f"Re-pushing continuation to heal...",
                    flush=True,
                )
                push_worker_node(
                    node_id,
                    state,
                    tokens,
                    usernames,
                    start_delay_seconds=0,
                    wait_if_running=True,
                    verify_boot=True,
                )
                save_fleet_state(bucket, state)
            else:
                node_rec["status"] = "ERROR"
                save_fleet_state(bucket, state)
                print(
                    f"[FleetManager] Node {node_id} reported DONE with {missing_cnt}/{total_tasks} incomplete tasks "
                    f"({missing_rate_pct:.3f}%) but exceeded max heal attempts ({retries}/{MAX_CONSECUTIVE_NODE_FAILURES}). Stopping loop.",
                    flush=True,
                )
            return reconcile_fleet(bucket, state, tokens, usernames)

        node_rec["status"] = "DONE"
        node_rec["retries"] = 0
        node_rec["integrity_verified"] = run_tag_str
        save_fleet_state(bucket, state)

        if all(state["nodes"][str(i)].get("status") == "DONE" for i in range(TOTAL_NOTEBOOKS)):
            print(
                f"[FleetManager] All {TOTAL_NOTEBOOKS} nodes are now DONE! "
                "Deleting finished worker notebooks, transitioning to 'standby', and launching tdv-standby on Account 0.",
                flush=True,
            )
            return _enter_standby_phase(bucket, state, tokens, usernames, wait_workers=True)

        # Also reconcile any sibling nodes whose callbacks may have been coalesced by GitHub concurrency
        return reconcile_fleet(bucket, state, tokens, usernames)

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
            verify_boot=True,
        )
        save_fleet_state(bucket, state)
        # Reconcile any other nodes whose TIMEOUT callbacks arrived concurrently
        return reconcile_fleet(bucket, state, tokens, usernames)

    if cb == "AUTH_FAILED":
        node_rec["status"] = "AUTH_FAILED"
        save_fleet_state(bucket, state)
        print(
            f"[FleetManager] CRITICAL: Node {node_id} reported AUTH_FAILED. "
            "Not re-pushing to avoid anonymous scraping.",
            flush=True,
        )
        return True

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
                verify_boot=True,
            )
            save_fleet_state(bucket, state)
        return reconcile_fleet(bucket, state, tokens, usernames)

    # Default: ERROR
    phase_str = str(state.get("phase", "initial"))
    run_tag_str = str(state.get("run_tag", "INITIAL" if phase_str == "initial" else phase_str))
    try:
        missing_cnt, total_tasks, missing_rate_pct = count_node_task_stats(bucket, node_id, run_tag_str)
    except Exception:
        missing_cnt, total_tasks, missing_rate_pct = 0, 0, 0.0

    node_rec["missing_tasks"] = missing_cnt
    node_rec["total_tasks"] = total_tasks
    node_rec["missing_rate_pct"] = missing_rate_pct

    # If within tolerance (< 2%), promote to DONE!
    if total_tasks > 0 and missing_rate_pct <= MAX_ALLOWED_MISSING_RATE_PCT:
        print(
            f"[FleetManager] Node {node_id} reported ERROR but missing rate is {missing_rate_pct:.3f}% "
            f"({missing_cnt}/{total_tasks} <= {MAX_ALLOWED_MISSING_RATE_PCT}%). Accepting as DONE within tolerance!",
            flush=True,
        )
        node_rec["status"] = "DONE"
        node_rec["retries"] = 0
        node_rec["integrity_verified"] = run_tag_str
        save_fleet_state(bucket, state)
        if all(state["nodes"][str(i)].get("status") == "DONE" for i in range(TOTAL_NOTEBOOKS)):
            print(
                f"[FleetManager] All {TOTAL_NOTEBOOKS} nodes are now DONE! "
                "Deleting finished worker notebooks, transitioning to 'standby', and launching tdv-standby on Account 0.",
                flush=True,
            )
            return _enter_standby_phase(bucket, state, tokens, usernames, wait_workers=True)
        return reconcile_fleet(bucket, state, tokens, usernames)

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
            verify_boot=True,
        )
        save_fleet_state(bucket, state)
    return reconcile_fleet(bucket, state, tokens, usernames)


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
            miss_pct = n_info.get("missing_rate_pct")
            miss_str = f"({miss_pct:.2f}% miss)" if miss_pct is not None and miss_pct > 0 else ""
            live_k = get_kernel_state(uname, f"tdv-node-{node_id}", tok) if tok and uname != "<unconfigured>" else "n/a"
            print(
                f"  Node {node_id:02d} (tdv-node-{node_id:<2d}) | "
                f"Bucket: {b_status:<8s} {miss_str:<13s} | Kaggle: {live_k:<10s} | Retries: {retries}"
            )

    sb_info = state.get("standby", {})
    sb_bucket = sb_info.get("status", "IDLE")
    sb_acc = sb_info.get("account_idx", "n/a")
    sb_sid = sb_info.get("session_id", "n/a")
    sb_next = sb_info.get("next_weekly_utc", "n/a")
    print(
        f"\n[Standby Clock] Bucket: {sb_bucket} | Active Acc: {sb_acc} | "
        f"Session: {sb_sid} | Next Weekly: {sb_next}"
    )
    for acc_idx in range(TOTAL_KAGGLE_ACCOUNTS):
        uname = usernames.get(acc_idx, "")
        tok = tokens[acc_idx] if acc_idx < len(tokens) else ""
        if tok and uname:
            sb_live = get_kernel_state(uname, "tdv-standby", tok)
            marker = " <-- active" if str(sb_acc) == str(acc_idx) else ""
            print(f"  Acc {acc_idx} ({uname}/tdv-standby): Kaggle={sb_live}{marker}")
    print("=" * 78)


def main() -> int:
    parser = argparse.ArgumentParser(description="TDV 20-Node Kaggle Fleet Orchestrator")
    parser.add_argument("--status", action="store_true", help="Print full fleet status")
    parser.add_argument("--reconcile", action="store_true", help="Reconcile and launch pending nodes")
    parser.add_argument("--push-node", type=int, default=None, help="Force-push a single worker node (0..19)")
    parser.add_argument("--push-standby", action="store_true", help="Force-push the standby clock notebook (starting on Account 0)")
    parser.add_argument("--reset-phase", type=str, default=None, help="Reset fleet state to 'initial' or 'WEEK_<WWYY>'")
    parser.add_argument("--callback-node", type=int, default=None, help="Node ID from repository_dispatch callback")
    parser.add_argument("--callback-status", type=str, default=None, help="Status from repository_dispatch callback")
    parser.add_argument("--callback-session-id", type=str, default="", help="Optional Standby session_id from callback payload")
    parser.add_argument("--callback-account-idx", type=int, default=None, help="Optional Standby account_idx from callback payload")
    parser.add_argument("--force-weekly", action="store_true", help="Force TRIGGER_WEEKLY even if current week tag is already active")
    args = parser.parse_args()

    bucket = BucketClient()
    tokens = load_kaggle_tokens()
    usernames = resolve_all_usernames(tokens)
    is_trigger_weekly = str(args.callback_status or "").strip().upper() == "TRIGGER_WEEKLY"
    state = load_and_sync_fleet_state(
        bucket,
        sync_logs=not (is_trigger_weekly or bool(args.reset_phase)),
    )

    if args.reset_phase:
        target = args.reset_phase.strip()
        r_tag = "INITIAL" if target.lower() == "initial" else target.upper()
        p_val = "initial" if target.lower() == "initial" else r_tag
        if p_val == "initial":
            delete_all_standby_kernels(tokens, usernames)
            delete_all_worker_kernels(tokens, usernames, wait_if_running=False, verify_stopped=True)
        state = default_fleet_state(phase=p_val, run_tag=r_tag)
        save_fleet_state(bucket, state)
        print(f"[FleetManager] Reset fleet state to phase={p_val}, run_tag={r_tag}", flush=True)

    if args.callback_node is not None and args.callback_status:
        ok = handle_node_callback(
            node_id=args.callback_node,
            callback_status=args.callback_status,
            bucket=bucket,
            state=state,
            tokens=tokens,
            usernames=usernames,
            callback_session_id=args.callback_session_id,
            callback_account_idx=args.callback_account_idx,
            force_weekly=args.force_weekly,
        )
        return 0 if ok else 1

    if args.push_node is not None:
        state["nodes"].setdefault(str(args.push_node), {})["retries"] = 0
        ok = push_worker_node(
            args.push_node,
            state,
            tokens,
            usernames,
            wait_if_running=True,
            force_push=True,
            verify_boot=True,
        )
        save_fleet_state(bucket, state)
        return 0 if ok else 1

    if args.push_standby:
        run_tag_str = str(state.get("run_tag", "WEEKLY"))
        print(
            f"[FleetManager] Manual standby triggered for {run_tag_str} -> "
            "cleaning worker kernels and launching tdv-standby starting on Account 0...",
            flush=True,
        )
        for i in range(TOTAL_NOTEBOOKS):
            state["nodes"].setdefault(str(i), {})["status"] = "DONE"
            state["nodes"][str(i)]["retries"] = 0
            state["nodes"][str(i)]["integrity_verified"] = run_tag_str
        ok = _enter_standby_phase(bucket, state, tokens, usernames, wait_workers=False)
        return 0 if ok else 1

    if args.reconcile:
        ok = reconcile_fleet(bucket, state, tokens, usernames)
        return 0 if ok else 1

    print_fleet_status(state, tokens, usernames)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

