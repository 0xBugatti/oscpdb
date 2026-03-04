"""
config.py - Central configuration for OSCP Repo Collector
"""
import os

# ──────────────────────────────────────────────────
# API Keys  (set as environment variables)
# ──────────────────────────────────────────────────
GITHUB_TOKEN      = os.environ.get("GITHUB_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ──────────────────────────────────────────────────
# GitHub Search
# ──────────────────────────────────────────────────
GITHUB_API_BASE = "https://api.github.com"
GITHUB_QUERY    = "OSCP in:name,description created:>2025-01-01 pushed:>2025-10-01 fork:false"
PER_PAGE        = 100          # GitHub max per page
MAX_PAGES       = 10           # Safety cap  (100 × 10 = 1000 repos max)

# ──────────────────────────────────────────────────
# Filters
# ──────────────────────────────────────────────────
MIN_SIZE_KB = 4                # Drop repos smaller than this

# ──────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────
BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
REPOS_DIR        = os.path.join(BASE_DIR, "cloned_repos")
DB_PATH          = os.path.join(BASE_DIR, "oscp_repos.db")
EXPORT_JSON_PATH = os.path.join(BASE_DIR, "visualize", "data.json")

# ──────────────────────────────────────────────────
# Clone settings
# ──────────────────────────────────────────────────
CLONE_DEPTH     = 1            # Shallow clone – we only need the latest snapshot
CLONE_TIMEOUT   = 120          # seconds per repo before giving up
MAX_CLONE_SIZE  = 500_000      # KB – skip cloning repos larger than this (fetch tree via API instead)

# ──────────────────────────────────────────────────
# Metadata extraction
# ──────────────────────────────────────────────────
MAX_DIR_DEPTH   = 8            # Max recursion depth when walking the file tree
README_MAX_BYTES = 30_000      # Cap README at 30 KB before Base64 encoding

# ──────────────────────────────────────────────────
# Categorization (Claude)
# ──────────────────────────────────────────────────
CLAUDE_MODEL    = "claude-sonnet-4-6"
CAT_MAX_RETRIES = 3
CAT_RETRY_DELAY = 5            # seconds between retries
