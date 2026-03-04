"""
export.py - Phase 5: Export DB → visualize/data.json for the HTML dashboard.
"""
import json
import os
import sqlite3
from config import DB_PATH, EXPORT_JSON_PATH
from db import get_conn


def run() -> None:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT
                id, name, full_name, description, url,
                created_at, pushed_at,
                size_kb, stars, forks, language, topics,
                file_structure_b64, file_names_b64, readme_b64,
                category, category_confidence, category_reasoning,
                cloned, metadata_extracted, categorized,
                clone_error
            FROM repos
            ORDER BY stars DESC
        """).fetchall()

    data = []
    for row in rows:
        r = dict(row)
        # topics: parse JSON array back to list
        try:
            r["topics"] = json.loads(r["topics"] or "[]")
        except Exception:
            r["topics"] = []
        # round confidence
        if r["category_confidence"] is not None:
            r["category_confidence"] = round(float(r["category_confidence"]), 2)
        data.append(r)

    # Write JSON
    os.makedirs(os.path.dirname(EXPORT_JSON_PATH), exist_ok=True)
    with open(EXPORT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump({"repos": data}, f, ensure_ascii=False)

    size_kb = os.path.getsize(EXPORT_JSON_PATH) // 1024
    print(f"[export] Exported {len(data)} repos → {EXPORT_JSON_PATH}  ({size_kb} KB)")


if __name__ == "__main__":
    run()
