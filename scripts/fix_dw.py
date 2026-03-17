from __future__ import annotations

import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

DB_PATH = Path("data/processed/vinyl_dw.sqlite")


def backup_db(db_path: Path) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    bak = db_path.with_name(db_path.name + f".bak.{ts}")
    shutil.copy2(db_path, bak)
    return bak


def query(con: sqlite3.Connection, sql: str):
    cur = con.execute(sql)
    return cur.fetchall()


def print_rows(rows):
    for r in rows:
        print(r)


def run_fix():
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}")
        sys.exit(2)

    bak = backup_db(DB_PATH)
    print(f"Backed up DB to: {bak}")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    try:
        print("\n-- Duplicate reviewid summary before fixes --")
        dups = query(
            con, "SELECT reviewid, COUNT(*) c FROM pitchfork_reviews GROUP BY reviewid HAVING c>1"
        )
        print_rows(dups)

        if dups:
            dup_ids = [f"'{r[0]}'" for r in dups]
            ids_sql = ",".join(dup_ids)
            print("\n-- Duplicate rows (examples) --")
            rows = query(
                con,
                f"SELECT rowid, reviewid, artist, score, pub_year FROM pitchfork_reviews WHERE reviewid IN ({ids_sql}) ORDER BY reviewid, rowid",
            )
            print_rows(rows)

        print("\n-- Blank artist rows before fixes --")
        blanks = query(
            con,
            "SELECT rowid, reviewid, artist, score, pub_year FROM pitchfork_reviews WHERE artist IS NULL OR TRIM(artist) = ''",
        )
        print_rows(blanks)

        # Apply fixes inside a transaction
        print(
            "\nApplying fixes: filling blank artists from bridge, then deduplicating by keeping earliest rowid"
        )
        cur = con.cursor()
        cur.execute("BEGIN")

        # Fill blank artists from pitchfork_review_artists when available
        cur.execute("""
            UPDATE pitchfork_reviews
            SET artist = (
                SELECT pra.artist FROM pitchfork_review_artists pra
                WHERE pra.reviewid = pitchfork_reviews.reviewid AND TRIM(pra.artist) <> ''
                LIMIT 1
            )
            WHERE artist IS NULL OR TRIM(artist) = ''
            """)
        filled = cur.rowcount
        print(f"Filled blank artist rows: {filled}")

        # Deduplicate: keep the row with the smallest rowid per reviewid
        cur.execute("""
            DELETE FROM pitchfork_reviews
            WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM pitchfork_reviews GROUP BY reviewid
            )
            """)
        deleted = cur.rowcount
        print(f"Deleted duplicate rows: {deleted}")

        cur.execute("COMMIT")

        # Re-run quick checks
        print("\n-- Post-fix check --")
        dups_after = query(
            con, "SELECT reviewid, COUNT(*) c FROM pitchfork_reviews GROUP BY reviewid HAVING c>1"
        )
        print("Duplicates remaining:")
        print_rows(dups_after)

        blanks_after = query(
            con, "SELECT COUNT(*) FROM pitchfork_reviews WHERE artist IS NULL OR TRIM(artist) = ''"
        )
        print("Blank artist count after:", blanks_after[0][0] if blanks_after else 0)

    finally:
        con.close()

    # Run the validation script
    print("\nRunning validation: python scripts/validate_dw.py")
    subprocess.run([sys.executable, "scripts/validate_dw.py"], check=False)

    # Run tests
    print("\nRunning pytest -v")
    subprocess.run([sys.executable, "-m", "pytest", "tests/", "-q"], check=False)


if __name__ == "__main__":
    run_fix()
