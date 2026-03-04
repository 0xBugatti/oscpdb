"""
clone_repos.py - Phase 2: Shallow-clone every unfetched repo into cloned_repos/

Usage:
    python clone_repos.py [--force]

Flags:
    --force   Re-clone repos that previously failed (clone=-1)
"""
import os
import sys
import subprocess
import shutil
from pathlib import Path
from config import REPOS_DIR, CLONE_DEPTH, CLONE_TIMEOUT, MAX_CLONE_SIZE, DB_PATH
from db import get_conn, mark_cloned, mark_clone_error


# ──────────────────────────────────────────────────
# Git clone wrapper
# ──────────────────────────────────────────────────
def _clone(clone_url: str, target_dir: str) -> tuple[bool, str]:
    """
    Attempt a shallow clone.
    Returns (success: bool, error_message: str)
    """
    if os.path.exists(target_dir):
        # Already cloned in a previous partial run
        if os.path.isdir(os.path.join(target_dir, ".git")):
            return True, ""
        # Corrupted directory – remove and retry
        shutil.rmtree(target_dir, ignore_errors=True)

    cmd = [
        "git", "clone",
        "--depth", str(CLONE_DEPTH),
        "--single-branch",
        "--no-tags",
        "--quiet",
        clone_url,
        target_dir,
    ]
    try:
        result = subprocess.run(
            cmd,
            timeout=CLONE_TIMEOUT,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return True, ""
        err = result.stderr.strip()[:500]
        return False, err
    except subprocess.TimeoutExpired:
        shutil.rmtree(target_dir, ignore_errors=True)
        return False, f"Timeout after {CLONE_TIMEOUT}s"
    except Exception as exc:
        return False, str(exc)[:300]


# ──────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────
def run(force: bool = False) -> None:
    os.makedirs(REPOS_DIR, exist_ok=True)

    with get_conn() as conn:
        if force:
            where = "cloned != 1"
        else:
            where = "cloned = 0"

        rows = conn.execute(
            f"SELECT full_name, clone_url, size_kb FROM repos WHERE {where} ORDER BY stars DESC"
        ).fetchall()

    total  = len(rows)
    done   = 0
    failed = 0

    print(f"[clone] {total} repos to clone  (force={force})")
    print(f"[clone] Target directory: {REPOS_DIR}\n")

    for idx, row in enumerate(rows, 1):
        full_name = row["full_name"]
        clone_url = row["clone_url"]
        size_kb   = row["size_kb"] or 0

        # Build a safe directory name  owner__repo
        safe_name  = full_name.replace("/", "__")
        target_dir = os.path.join(REPOS_DIR, safe_name)

        prefix = f"[{idx:>3}/{total}]"

        # Oversized repo – skip actual clone (metadata will use GitHub API fallback)
        if size_kb > MAX_CLONE_SIZE:
            print(f"{prefix} SKIP (too large {size_kb}KB) → {full_name}")
            with get_conn() as conn:
                mark_clone_error(conn, full_name, f"skipped: size {size_kb}KB > limit {MAX_CLONE_SIZE}KB")
            failed += 1
            continue

        print(f"{prefix} Cloning {full_name} ({size_kb}KB) ...", end=" ", flush=True)
        ok, err = _clone(clone_url, target_dir)

        with get_conn() as conn:
            if ok:
                mark_cloned(conn, full_name, target_dir)
                done += 1
                print("OK")
            else:
                mark_clone_error(conn, full_name, err)
                failed += 1
                print(f"FAIL – {err[:80]}")

    print(f"\n[clone] Done.  Cloned: {done}  |  Failed/Skipped: {failed}")


if __name__ == "__main__":
    force_flag = "--force" in sys.argv
    run(force=force_flag)
