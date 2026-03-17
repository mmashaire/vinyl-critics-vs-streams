from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path("data/processed/vinyl_dw.sqlite")


def main():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    rows = con.execute(
        "SELECT rowid, reviewid, artist, score, pub_year FROM pitchfork_reviews WHERE artist IS NULL OR TRIM(artist) = ''"
    ).fetchall()
    if not rows:
        print("No blank artist rows found")
        return

    for r in rows:
        print("---")
        print(dict(r))
        pra = con.execute(
            "SELECT rowid, artist FROM pitchfork_review_artists WHERE reviewid = ?",
            (r["reviewid"],),
        ).fetchall()
        print("Bridge rows:")
        for p in pra:
            print(dict(p))

    con.close()


if __name__ == "__main__":
    main()
