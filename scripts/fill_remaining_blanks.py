from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path("data/processed/vinyl_dw.sqlite")


def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    # Fill specific remaining blank artist reviewids discovered during debug
    reviewids = (18989, 19032)
    cur.execute("BEGIN")
    cur.execute("""
        UPDATE pitchfork_reviews
        SET artist = 'UNKNOWN-' || reviewid
        WHERE reviewid IN ({ids})
        """.replace("{ids}", ",".join(str(i) for i in reviewids)))
    print("Updated rows:", cur.rowcount)
    cur.execute("COMMIT")
    con.close()


if __name__ == "__main__":
    main()
