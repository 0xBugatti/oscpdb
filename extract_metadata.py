"""
extract_metadata.py - Phase 3: Walk each cloned repo, extract file tree + README.

Produces two Base64-encoded JSON blobs per repo:
  file_structure_b64 → hierarchical directory tree (JSON)
  file_names_b64     → flat list of all relative file paths (JSON)
  readme_b64         → raw bytes of the best README found (or empty)

Falls back to GitHub Contents API for repos that were too large to clone.

Usage:
    python extract_metadata.py [--reprocess]
"""
import os
import sys
import base64
import json
import time
import requests
from pathlib import Path
from config import (
    REPOS_DIR, MAX_DIR_DEPTH, README_MAX_BYTES,
    GITHUB_TOKEN, GITHUB_API_BASE, DB_PATH,
)
from db import get_conn, update_metadata


# Typical README filenames to look for (case-insensitive)
README_NAMES = {"readme.md", "readme.txt", "readme.rst", "readme",
                "readme.html", "readme.adoc"}

# Directories to skip entirely
SKIP_DIRS = {".git", ".github", "__pycache__", "node_modules", ".idea",
             ".vscode", "venv", ".venv", "env", ".env", "dist", "build"}


# ──────────────────────────────────────────────────
# File-tree builders (local clone)
# ──────────────────────────────────────────────────

def _build_tree(root: Path, rel: Path = None, depth: int = 0) -> dict:
    """Recursively build a dict representing the directory tree."""
    if rel is None:
        rel = Path(".")

    node = {"name": root.name, "type": "dir", "children": []}

    if depth >= MAX_DIR_DEPTH:
        node["truncated"] = True
        return node

    try:
        entries = sorted(root.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
    except PermissionError:
        return node

    for entry in entries:
        if entry.name in SKIP_DIRS:
            continue
        if entry.is_symlink():
            continue
        if entry.is_dir():
            node["children"].append(_build_tree(entry, rel / entry.name, depth + 1))
        else:
            node["children"].append({
                "name": entry.name,
                "type": "file",
                "size": _safe_size(entry),
            })

    return node


def _collect_file_paths(root: Path, depth: int = 0) -> list[str]:
    """Return a flat list of all relative file paths (forward slashes)."""
    paths = []
    if depth > MAX_DIR_DEPTH:
        return paths

    try:
        entries = sorted(root.iterdir(), key=lambda p: p.name.lower())
    except PermissionError:
        return paths

    for entry in entries:
        if entry.name in SKIP_DIRS:
            continue
        if entry.is_symlink():
            continue
        if entry.is_dir():
            paths.extend(_collect_file_paths(entry, depth + 1))
        else:
            # Relative path from repo root, forward slashes
            rel = entry.relative_to(root.parent)
            paths.append(str(rel).replace("\\", "/"))

    return paths


def _safe_size(p: Path) -> int:
    try:
        return p.stat().st_size
    except Exception:
        return 0


def _find_readme(root: Path) -> bytes:
    """Find the best README in the repo root."""
    for entry in root.iterdir():
        if entry.is_file() and entry.name.lower() in README_NAMES:
            try:
                data = entry.read_bytes()
                return data[:README_MAX_BYTES]
            except Exception:
                pass
    return b""


def _b64(data: bytes | str) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8", errors="replace")
    return base64.b64encode(data).decode("ascii")


# ──────────────────────────────────────────────────
# GitHub API fallback (for repos too large to clone)
# ──────────────────────────────────────────────────

def _api_headers() -> dict:
    h = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h


def _api_fetch_tree(full_name: str) -> tuple[dict, list[str]]:
    """Fetch full recursive file tree from GitHub API (git trees endpoint)."""
    # First get the default branch
    r = requests.get(
        f"{GITHUB_API_BASE}/repos/{full_name}",
        headers=_api_headers(), timeout=20,
    )
    if r.status_code != 200:
        return {}, []

    default_branch = r.json().get("default_branch", "main")
    time.sleep(0.5)

    # Get the recursive tree
    r2 = requests.get(
        f"{GITHUB_API_BASE}/repos/{full_name}/git/trees/{default_branch}?recursive=1",
        headers=_api_headers(), timeout=30,
    )
    if r2.status_code != 200:
        return {}, []

    tree_data = r2.json()
    items = tree_data.get("tree", [])

    file_paths = [
        item["path"].replace("\\", "/")
        for item in items
        if item.get("type") == "blob"
    ]

    # Build a simple hierarchical tree from the flat list
    root_node = {"name": full_name.split("/")[1], "type": "dir", "children": []}
    for fpath in file_paths:
        parts = fpath.split("/")
        cur = root_node
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                cur["children"].append({"name": part, "type": "file"})
            else:
                # find or create dir node
                found = next((c for c in cur["children"]
                              if c.get("type") == "dir" and c["name"] == part), None)
                if found is None:
                    found = {"name": part, "type": "dir", "children": []}
                    cur["children"].append(found)
                cur = found

    return root_node, file_paths


def _api_fetch_readme(full_name: str) -> bytes:
    r = requests.get(
        f"{GITHUB_API_BASE}/repos/{full_name}/readme",
        headers={**_api_headers(), "Accept": "application/vnd.github.raw+json"},
        timeout=20,
    )
    if r.status_code == 200:
        return r.content[:README_MAX_BYTES]
    return b""


# ──────────────────────────────────────────────────
# Per-repo processor
# ──────────────────────────────────────────────────

def _process_local(clone_path: str) -> dict:
    root = Path(clone_path)
    tree = _build_tree(root)
    paths = _collect_file_paths(root)
    # paths list starts from the repo-root dir name – strip it
    paths = ["/".join(p.split("/")[1:]) for p in paths]
    readme_bytes = _find_readme(root)
    return {
        "file_structure_b64": _b64(json.dumps(tree, ensure_ascii=False)),
        "file_names_b64":     _b64(json.dumps(paths, ensure_ascii=False)),
        "readme_b64":         _b64(readme_bytes),
    }


def _process_api(full_name: str) -> dict:
    tree, paths = _api_fetch_tree(full_name)
    readme_bytes = _api_fetch_readme(full_name)
    time.sleep(1)
    return {
        "file_structure_b64": _b64(json.dumps(tree, ensure_ascii=False)),
        "file_names_b64":     _b64(json.dumps(paths, ensure_ascii=False)),
        "readme_b64":         _b64(readme_bytes),
    }


# ──────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────

def run(reprocess: bool = False) -> None:
    with get_conn() as conn:
        if reprocess:
            where = "1=1"
        else:
            where = "metadata_extracted = 0"

        rows = conn.execute(
            f"""SELECT full_name, clone_path, clone_error, cloned
                FROM repos WHERE {where} ORDER BY id"""
        ).fetchall()

    total = len(rows)
    done  = 0
    failed = 0

    print(f"[extract] {total} repos to process  (reprocess={reprocess})\n")

    for idx, row in enumerate(rows, 1):
        full_name  = row["full_name"]
        clone_path = row["clone_path"]
        cloned     = row["cloned"]
        prefix     = f"[{idx:>3}/{total}]"

        print(f"{prefix} {full_name} ...", end=" ", flush=True)

        try:
            if cloned == 1 and clone_path and os.path.isdir(clone_path):
                payload = _process_local(clone_path)
                src = "local"
            else:
                # Fallback: fetch tree via GitHub API
                payload = _process_api(full_name)
                src = "api"

            with get_conn() as conn:
                update_metadata(conn, full_name, payload)
            done += 1
            print(f"OK ({src})")

        except Exception as exc:
            failed += 1
            print(f"ERROR – {exc}")

    print(f"\n[extract] Done.  Processed: {done}  |  Failed: {failed}")


if __name__ == "__main__":
    reprocess_flag = "--reprocess" in sys.argv
    run(reprocess=reprocess_flag)
