"""repair_warehouse.py

Small, readable maintenance utility for the local warehouse sqlite DB.

Features:
- create a DB backup before making changes
- record actions into a simple `maintenance_changelog` table
- fill blank `artist` values in `pitchfork_reviews` when the bridge provides
  a single unambiguous artist for that reviewid
- deduplicate `pitchfork_reviews` by keeping the row with the most
  non-null fields (tie-break: lowest rowid)
- run `scripts/validate_dw.py` at the end to sanity-check the warehouse

This script is designed to be safe and idempotent: running it again will
not re-apply the same changes because we record operations in the changelog
and only act on remaining problems.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

DB_PATH = Path("data/processed/vinyl_dw.sqlite")


@dataclass
class ChangeRecord:
    action: str
    details: dict


def backup_db(db_path: Path) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    bak = db_path.with_name(db_path.name + f".bak.{ts}")
    shutil.copy2(db_path, bak)
    return bak


def ensure_changelog(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS maintenance_changelog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            action TEXT NOT NULL,
            details TEXT NOT NULL
        )
        """)


def log_change(conn: sqlite3.Connection, rec: ChangeRecord) -> None:
    conn.execute(
        "INSERT INTO maintenance_changelog (ts, action, details) VALUES (?, ?, ?)",
        (
            datetime.utcnow().isoformat() + "Z",
            rec.action,
            json.dumps(rec.details, ensure_ascii=False),
        ),
    )


def get_duplicate_reviewids(conn: sqlite3.Connection) -> List[Tuple[str, int]]:
    rows = conn.execute(
        "SELECT reviewid, COUNT(*) c FROM pitchfork_reviews GROUP BY reviewid HAVING c > 1"
    ).fetchall()
    return [(r[0], int(r[1])) for r in rows]


def get_blank_artist_reviewids(conn: sqlite3.Connection) -> List[int]:
    rows = conn.execute(
        "SELECT DISTINCT reviewid FROM pitchfork_reviews WHERE artist IS NULL OR TRIM(artist) = ''"
    ).fetchall()
    return [r[0] for r in rows]


def choose_row_to_keep(conn: sqlite3.Connection, reviewid: int) -> Optional[int]:
    """Choose the best rowid to keep for a duplicated reviewid.

    Strategy: compute number of non-null, non-empty fields per row and pick the
    row with the highest score. Tie-breaker: smallest rowid.
    """
    rows = conn.execute(
        "SELECT rowid, * FROM pitchfork_reviews WHERE reviewid = ?",
        (reviewid,),
    ).fetchall()
    if not rows:
        return None

    best_rowid = None
    best_score = -1
    for r in rows:
        # Count non-null/non-empty across a small set of informative columns
        score = 0
        for col in ("artist", "score", "pub_year", "album", "label"):
            if col in r.keys():
                v = r[col]
                if v is not None and (not isinstance(v, str) or v.strip() != ""):
                    score += 1
        rid = r["rowid"]
        if score > best_score or (score == best_score and (best_rowid is None or rid < best_rowid)):
            best_score = score
            best_rowid = rid

    return best_rowid


def fill_blank_artists(conn: sqlite3.Connection, reviewids: Iterable[int]) -> List[ChangeRecord]:
    """For each reviewid with blank artist, if the bridge provides exactly one
    non-blank artist, fill it. Return a list of ChangeRecord objects describing
    applied changes.
    """
    changes: List[ChangeRecord] = []
    cur = conn.cursor()
    for rid in reviewids:
        artists = cur.execute(
            "SELECT DISTINCT TRIM(artist) FROM pitchfork_review_artists WHERE reviewid = ? AND TRIM(artist) <> ''",
            (rid,),
        ).fetchall()
        distinct = [a[0] for a in artists]
        if len(distinct) == 1:
            artist = distinct[0]
            cur.execute(
                "UPDATE pitchfork_reviews SET artist = ? WHERE reviewid = ? AND (artist IS NULL OR TRIM(artist) = '')",
                (artist, rid),
            )
            if cur.rowcount:
                changes.append(
                    ChangeRecord(
                        "fill_artist_from_bridge",
                        {"reviewid": rid, "artist": artist, "rows_updated": cur.rowcount},
                    )
                )
    return changes


def deduplicate_reviewid(conn: sqlite3.Connection, reviewid: int) -> Optional[ChangeRecord]:
    cur = conn.cursor()
    keep = choose_row_to_keep(conn, reviewid)
    if keep is None:
        return None
    # Delete other rows with same reviewid but different rowid
    cur.execute("DELETE FROM pitchfork_reviews WHERE reviewid = ? AND rowid != ?", (reviewid, keep))
    deleted = cur.rowcount
    if deleted:
        return ChangeRecord(
            "deduplicate_reviewid", {"reviewid": reviewid, "kept_rowid": keep, "deleted": deleted}
        )
    return None


def run_validation():
    print("Running validation: scripts/validate_dw.py")
    subprocess.run([sys.executable, "scripts/validate_dw.py"], check=False)


def run_tests():
    print("Running tests: pytest tests/")
    subprocess.run([sys.executable, "-m", "pytest", "tests/", "-q"], check=False)


def main(dry_run: bool = True) -> int:
    if not DB_PATH.exists():
        print(f"error: DB not found at {DB_PATH}")
        return 2

    bak = backup_db(DB_PATH)
    print(f"Backed up DB to {bak}")

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        ensure_changelog(conn)

        dupes = get_duplicate_reviewids(conn)
        blanks = get_blank_artist_reviewids(conn)

        print(f"Duplicate reviewids: {len(dupes)}")
        print(f"Blank-artist reviewids: {len(blanks)}")

        if dry_run:
            print("Dry-run mode: no changes will be written. Use --apply to modify the DB.")
        changes: List[ChangeRecord] = []

        if not dry_run:
            cur = conn.cursor()
            # Fill blanks
            changes += fill_blank_artists(conn, blanks)

            # Deduplicate
            for reviewid, _ in dupes:
                rec = deduplicate_reviewid(conn, reviewid)
                if rec:
                    changes.append(rec)

            # Persist changelog
            for ch in changes:
                log_change(conn, ch)

    # Summary
    if changes:
        print("Applied changes:")
        for ch in changes:
            print(f"- {ch.action}: {ch.details}")
    else:
        print("No changes applied.")

    # Run validation and tests regardless of dry-run so you can verify expected outcome
    run_validation()
    run_tests()

    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="Apply changes (default: dry-run)")
    args = p.parse_args()
    rc = main(dry_run=not args.apply)
    sys.exit(rc)
