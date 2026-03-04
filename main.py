"""
main.py - Orchestrator: run all phases in sequence or individually.

Usage:
    python main.py                    # run all phases
    python main.py fetch              # Phase 1 only
    python main.py clone              # Phase 2 only
    python main.py extract            # Phase 3 only
    python main.py categorize         # Phase 4 only
    python main.py export             # Phase 5 only
    python main.py serve              # Launch dashboard in browser
    python main.py status             # Show DB stats

Flags (appended after phase name):
    --force         clone: re-clone failed repos
    --reprocess     extract: re-process already-extracted repos
    --recategorize  categorize: re-run AI on already-categorised repos
"""
import sys
import os
from config import DB_PATH, EXPORT_JSON_PATH, BASE_DIR
from db import get_conn, init_db, get_stats


# ──────────────────────────────────────────────────
# Phase runners
# ──────────────────────────────────────────────────

def phase_fetch():
    from fetch_repos import run
    run()

def phase_clone():
    from clone_repos import run
    run(force="--force" in sys.argv)

def phase_extract():
    from extract_metadata import run
    run(reprocess="--reprocess" in sys.argv)

def phase_categorize():
    from categorize import run
    run(recategorize="--recategorize" in sys.argv)

def phase_export():
    from export import run
    run()

def phase_serve():
    import subprocess
    print("[serve] Starting Node dashboard server...")
    print("[serve] http://127.0.0.1:8787")
    print("[serve] Ctrl-C to stop.")
    try:
        subprocess.run(["node", "server.js"], cwd=BASE_DIR)
    except KeyboardInterrupt:
        print("\n[serve] Stopped.")
    except FileNotFoundError:
        print("[serve] Error: node not found. Install Node.js or run: node server.js")

def phase_status():
    init_db()
    with get_conn() as conn:
        stats = get_stats(conn)
    print("\n─── DB Status ───────────────────────────────")
    for k, v in stats.items():
        print(f"  {k:<22} {v}")
    print("─────────────────────────────────────────────\n")


# ──────────────────────────────────────────────────
# Dispatch
# ──────────────────────────────────────────────────

PHASES = {
    "fetch":      (phase_fetch,      "GitHub API → DB"),
    "clone":      (phase_clone,      "Clone repos locally"),
    "extract":    (phase_extract,    "Extract file trees + README"),
    "categorize": (phase_categorize, "AI categorisation (Claude)"),
    "export":     (phase_export,     "Export DB → data.json"),
    "serve":      (phase_serve,      "Launch HTML dashboard"),
    "status":     (phase_status,     "Show DB statistics"),
}


def main():
    init_db()

    # Determine which phases to run
    args = [a for a in sys.argv[1:] if not a.startswith("--")]

    if not args:
        # Run all data phases in sequence
        print("=" * 54)
        print("  OSCP Repo Collector – Full Run")
        print("=" * 54)
        for name in ["fetch", "clone", "extract", "categorize", "export"]:
            fn, desc = PHASES[name]
            print(f"\n{'─'*54}")
            print(f"  Phase: {desc}")
            print(f"{'─'*54}")
            fn()
        print("\nAll phases complete.  Run  python main.py serve  to open the dashboard.")
        return

    for arg in args:
        if arg not in PHASES:
            print(f"Unknown phase: {arg!r}")
            print(f"Available: {', '.join(PHASES)}")
            sys.exit(1)
        fn, desc = PHASES[arg]
        print(f"\n[main] Phase: {desc}")
        fn()


if __name__ == "__main__":
    main()
