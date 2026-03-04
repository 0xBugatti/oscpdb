"""
fetch_repos.py - Phase 1: Query GitHub Search API and populate the DB.

Usage:
    python fetch_repos.py

Env vars required:
    GITHUB_TOKEN  (optional but strongly recommended to avoid rate-limiting)
"""
import json
import time
import datetime
import requests
from config import (
    GITHUB_TOKEN, GITHUB_API_BASE, GITHUB_QUERY,
    PER_PAGE, MAX_PAGES, MIN_SIZE_KB, DB_PATH,
)
from db import get_conn, init_db, upsert_repo


# ──────────────────────────────────────────────────
# HTTP session
# ──────────────────────────────────────────────────
def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    if GITHUB_TOKEN:
        s.headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    else:
        print("[fetch] WARNING: No GITHUB_TOKEN set – rate limit is 60 req/hr")
    return s


def _check_rate_limit(resp: requests.Response) -> None:
    remaining = int(resp.headers.get("X-RateLimit-Remaining", 99))
    reset_ts   = int(resp.headers.get("X-RateLimit-Reset", 0))
    if remaining <= 2:
        wait = max(0, reset_ts - int(time.time())) + 5
        print(f"[fetch] Rate limit nearly exhausted – sleeping {wait}s ...")
        time.sleep(wait)


# ──────────────────────────────────────────────────
# Fetch one page
# ──────────────────────────────────────────────────
def _fetch_page(session: requests.Session, page: int) -> dict:
    url = f"{GITHUB_API_BASE}/search/repositories"
    params = {
        "q":        GITHUB_QUERY,
        "sort":     "updated",
        "order":    "desc",
        "per_page": PER_PAGE,
        "page":     page,
    }
    for attempt in range(3):
        r = session.get(url, params=params, timeout=30)
        _check_rate_limit(r)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 422:
            print(f"[fetch] 422 Unprocessable – GitHub won't return page {page} (>1000 result cap)")
            return {}
        if r.status_code in (403, 429):
            wait = 60 * (attempt + 1)
            print(f"[fetch] Rate limited ({r.status_code}) – retry in {wait}s")
            time.sleep(wait)
        else:
            print(f"[fetch] HTTP {r.status_code} on page {page} – {r.text[:200]}")
            time.sleep(5)
    return {}


# ──────────────────────────────────────────────────
# Parse a raw item into a DB-ready dict
# ──────────────────────────────────────────────────
def _parse_item(item: dict) -> dict:
    return {
        "name":        item["name"],
        "full_name":   item["full_name"],
        "description": (item.get("description") or "")[:2000],
        "url":         item["html_url"],
        "clone_url":   item["clone_url"],
        "created_at":  item.get("created_at", ""),
        "pushed_at":   item.get("pushed_at", ""),
        "fetched_at":  datetime.datetime.utcnow().isoformat(),
        "size_kb":     item.get("size", 0),
        "stars":       item.get("stargazers_count", 0),
        "forks":       item.get("forks_count", 0),
        "language":    item.get("language") or "",
        "topics":      json.dumps(item.get("topics", [])),
    }


# ──────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────
def run() -> None:
    init_db()
    session = _build_session()
    total_fetched  = 0
    total_inserted = 0
    total_skipped  = 0

    print(f"[fetch] Query: {GITHUB_QUERY}")
    print(f"[fetch] Min size filter: {MIN_SIZE_KB} KB\n")

    for page in range(1, MAX_PAGES + 1):
        print(f"[fetch] Fetching page {page} ...", end=" ", flush=True)
        data = _fetch_page(session, page)

        if not data:
            print("empty / error – stopping.")
            break

        items = data.get("items", [])
        if not items:
            print("no items – done.")
            break

        total_count = data.get("total_count", 0)
        if page == 1:
            print(f"\n[fetch] Total matching repos reported by GitHub: {total_count}")
            if total_count > 1000:
                print("[fetch] WARNING: GitHub caps search at 1000 results. Refine query if needed.\n")

        print(f"{len(items)} repos received.", end=" ")

        with get_conn() as conn:
            page_inserted = 0
            page_skipped  = 0
            for item in items:
                total_fetched += 1
                # Size filter
                size_kb = item.get("size", 0)
                if size_kb < MIN_SIZE_KB:
                    total_skipped += 1
                    page_skipped  += 1
                    continue
                parsed = _parse_item(item)
                upsert_repo(conn, parsed)
                total_inserted += 1
                page_inserted  += 1

        print(f"Inserted {page_inserted}, skipped {page_skipped} (<{MIN_SIZE_KB}KB).")

        # GitHub search paginates up to 1000; stop early if last page
        if len(items) < PER_PAGE:
            print("[fetch] Reached last page.")
            break

        # Be polite between pages
        time.sleep(1.5)

    print(f"\n[fetch] Done.  Fetched: {total_fetched}  |  "
          f"Inserted/updated: {total_inserted}  |  "
          f"Skipped (<{MIN_SIZE_KB}KB): {total_skipped}")


if __name__ == "__main__":
    run()
