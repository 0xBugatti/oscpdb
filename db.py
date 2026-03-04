"""
db.py - SQLite schema setup and helper functions
"""
import sqlite3
from config import DB_PATH


# ──────────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS repos (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Identity
    name                    TEXT NOT NULL,
    full_name               TEXT UNIQUE NOT NULL,
    description             TEXT,
    url                     TEXT,
    clone_url               TEXT,
    -- Timestamps (ISO-8601)
    created_at              TEXT,
    pushed_at               TEXT,
    fetched_at              TEXT,
    -- Stats
    size_kb                 INTEGER,
    stars                   INTEGER DEFAULT 0,
    forks                   INTEGER DEFAULT 0,
    language                TEXT,
    topics                  TEXT,        -- JSON array  ["oscp","pentest",...]
    -- Payload (Base64-encoded)
    file_structure_b64      TEXT,        -- hierarchical JSON tree
    file_names_b64          TEXT,        -- flat JSON array of all relative paths
    readme_b64              TEXT,        -- raw README content
    -- Categorization
    category                TEXT,        -- Generic|OSCP+|Writeups|Sensitive|Tools|None
    category_confidence     REAL,
    category_reasoning      TEXT,
    -- Processing flags
    cloned                  INTEGER DEFAULT 0,
    metadata_extracted      INTEGER DEFAULT 0,
    categorized             INTEGER DEFAULT 0,
    clone_path              TEXT,
    clone_error             TEXT
);

CREATE INDEX IF NOT EXISTS idx_category  ON repos(category);
CREATE INDEX IF NOT EXISTS idx_stars     ON repos(stars);
CREATE INDEX IF NOT EXISTS idx_size_kb   ON repos(size_kb);
"""


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.executescript(SCHEMA)
    print(f"[db] Database initialised -> {db_path}")


# ──────────────────────────────────────────────────
# Upsert helpers
# ──────────────────────────────────────────────────

def upsert_repo(conn: sqlite3.Connection, data: dict) -> None:
    """Insert or update a repo row (keyed on full_name)."""
    cols = list(data.keys())
    placeholders = ", ".join(["?"] * len(cols))
    updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "full_name")
    sql = f"""
        INSERT INTO repos ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT(full_name) DO UPDATE SET {updates}
    """
    conn.execute(sql, list(data.values()))


def mark_cloned(conn: sqlite3.Connection, full_name: str, clone_path: str) -> None:
    conn.execute(
        "UPDATE repos SET cloned=1, clone_path=? WHERE full_name=?",
        (clone_path, full_name),
    )


def mark_clone_error(conn: sqlite3.Connection, full_name: str, error: str) -> None:
    conn.execute(
        "UPDATE repos SET cloned=-1, clone_error=? WHERE full_name=?",
        (error, full_name),
    )


def update_metadata(conn: sqlite3.Connection, full_name: str, payload: dict) -> None:
    payload["metadata_extracted"] = 1
    payload["full_name"] = full_name
    cols = list(payload.keys())
    updates = ", ".join(f"{c}=?" for c in cols if c != "full_name")
    sql = f"UPDATE repos SET {updates} WHERE full_name=?"
    values = [payload[c] for c in cols if c != "full_name"] + [full_name]
    conn.execute(sql, values)


def update_category(conn: sqlite3.Connection, full_name: str, category: str,
                    confidence: float, reasoning: str) -> None:
    conn.execute(
        """UPDATE repos
           SET category=?, category_confidence=?, category_reasoning=?, categorized=1
           WHERE full_name=?""",
        (category, confidence, reasoning, full_name),
    )


# ──────────────────────────────────────────────────
# Query helpers
# ──────────────────────────────────────────────────

def get_repos(conn: sqlite3.Connection, where: str = "1=1") -> list:
    return conn.execute(f"SELECT * FROM repos WHERE {where}").fetchall()


def get_stats(conn: sqlite3.Connection) -> dict:
    row = conn.execute("""
        SELECT
            COUNT(*)                                        AS total,
            SUM(cloned=1)                                   AS cloned,
            SUM(metadata_extracted=1)                       AS extracted,
            SUM(categorized=1)                              AS categorized,
            SUM(category='Generic')                         AS generic,
            SUM(category='OSCP+')                          AS oscp_plus,
            SUM(category='Writeups')                        AS writeups,
            SUM(category='Sensitive')                       AS sensitive,
            SUM(category='Tools')                           AS tools,
            SUM(category='None')                            AS none_cat
        FROM repos
    """).fetchone()
    return dict(row)
