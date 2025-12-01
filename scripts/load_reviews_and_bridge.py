from pathlib import Path
import sqlite3
import pandas as pd

DB = Path("data/processed/vinyl_dw.sqlite")
REV = Path("data/interim/pitchfork_reviews_typed.csv")
BRIDGE = Path("data/interim/pitchfork_review_artists.csv")

def create_index(conn, table, index_name, columns_or_expr):
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = {r[1] for r in cur.fetchall()}
    needed_cols = {c for c in columns_or_expr.replace("LOWER(", "").replace(")", "").split(",") if c.isidentifier()}
    if needed_cols.issubset(cols):
        conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table}({columns_or_expr});")

with sqlite3.connect(DB) as con:
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")

    df_rev = pd.read_csv(REV, parse_dates=["pub_date"])
    df_rev["pub_date"] = df_rev["pub_date"].dt.strftime("%Y-%m-%d")
    df_rev.to_sql("pitchfork_reviews", con, if_exists="replace", index=False)
    print(f"[ok] loaded pitchfork_reviews ({len(df_rev):,} rows)")

    df_bridge = pd.read_csv(BRIDGE)
    df_bridge.to_sql("pitchfork_review_artists", con, if_exists="replace", index=False)
    print(f"[ok] loaded pitchfork_review_artists ({len(df_bridge):,} rows)")

    # Indexes (created only if columns exist)
    create_index(con, "pitchfork_reviews", "ix_reviews_reviewid", "reviewid")
    create_index(con, "pitchfork_reviews", "ix_reviews_pub_date", "pub_date")
    create_index(con, "pitchfork_reviews", "ix_reviews_pub_year_month", "pub_year,pub_month")
    create_index(con, "pitchfork_reviews", "ix_reviews_bnm", "best_new_music")
    create_index(con, "pitchfork_review_artists", "ix_bridge_reviewid", "reviewid")
    create_index(con, "pitchfork_review_artists", "ix_bridge_artist", "artist")
    create_index(con, "pitchfork_review_artists", "ix_bridge_artist_lower", "LOWER(artist)")
    con.commit()
    print("[ok] indexes ensured")

    # Verifications
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM pitchfork_review_artists;")
    print(f"[check] bridge rows: {cur.fetchone()[0]:,}")

    cur.execute("""
        SELECT COUNT(*) FROM (
          SELECT reviewid, artist, COUNT(*) c
          FROM pitchfork_review_artists
          GROUP BY reviewid, artist
          HAVING c > 1
        );
    """)
    print(f"[check] duplicate (reviewid, artist) pairs: {cur.fetchone()[0]}")

    cur.execute("""
        SELECT COUNT(*)
        FROM pitchfork_review_artists pra
        LEFT JOIN pitchfork_reviews pr ON pr.reviewid = pra.reviewid
        WHERE pr.reviewid IS NULL;
    """)
    print(f"[check] orphans (no matching review): {cur.fetchone()[0]}")

    cur.execute("""
        SELECT artist, COUNT(*) AS n
        FROM pitchfork_review_artists
        GROUP BY artist
        ORDER BY n DESC, artist ASC
        LIMIT 10;
    """)
    print("[sample] top artists by review count:")
    for artist, n in cur.fetchall():
        print(f"  {n:>5}  {artist}")
