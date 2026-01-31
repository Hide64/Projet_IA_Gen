import os
import time
import argparse
import requests
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

load_dotenv()

TMDB_KEY = os.environ["TMDB_API_KEY"]
TMDB_BASE = "https://api.themoviedb.org/3"

DB = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "videotheque"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
)

USER_ID = 1  # Hide

def tmdb_get(path, params=None):
    params = params or {}
    params["api_key"] = TMDB_KEY
    r = requests.get(f"{TMDB_BASE}{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--sleep", type=float, default=0.2)
    args = ap.parse_args()

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    with conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT *
            FROM import_watchlist_sc
            WHERE match_status='MATCHED'
              AND tmdb_id IS NOT NULL
              AND (match_note IS NULL OR match_note NOT LIKE '%%applied%%')
            ORDER BY import_id
            LIMIT %s;
        """, (args.limit,))
        rows = cur.fetchall()

        print(f"[apply_watchlist] items: {len(rows)}")

        for r in rows:
            try:
                details = tmdb_get(f"/movie/{r['tmdb_id']}", {"language": "fr-FR"})

                # upsert film
                cur.execute("""
                    INSERT INTO film (tmdb_id, title, original_title, release_date, year, runtime_min, overview)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (tmdb_id) DO UPDATE SET
                      title=EXCLUDED.title,
                      original_title=EXCLUDED.original_title,
                      release_date=EXCLUDED.release_date,
                      year=EXCLUDED.year,
                      runtime_min=EXCLUDED.runtime_min,
                      overview=EXCLUDED.overview,
                      updated_at=now()
                    RETURNING film_id;
                """, (
                    details["id"],
                    details.get("title"),
                    details.get("original_title"),
                    details.get("release_date"),
                    int(details["release_date"][:4]) if details.get("release_date") else None,
                    details.get("runtime"),
                    details.get("overview"),
                ))
                film_id = cur.fetchone()[0]

                # upsert user_film as WANT, but do NOT overwrite SEEN
                cur.execute("""
                    INSERT INTO user_film (user_id, film_id, status, updated_at)
                    VALUES (%s,%s,'WANT', now())
                    ON CONFLICT (user_id, film_id) DO UPDATE SET
                      status = CASE
                        WHEN user_film.status = 'SEEN' THEN user_film.status
                        ELSE 'WANT'
                      END,
                      updated_at = now();
                """, (USER_ID, film_id))

                # mark applied
                cur.execute("""
                    UPDATE import_watchlist_sc
                    SET match_note = COALESCE(match_note,'') || ' | applied'
                    WHERE import_id=%s;
                """, (r["import_id"],))

                conn.commit()
                time.sleep(args.sleep)

            except Exception as e:
                conn.rollback()
                cur.execute("""
                    UPDATE import_watchlist_sc
                    SET match_status='ERROR', match_note=%s
                    WHERE import_id=%s;
                """, (str(e)[:900], r["import_id"]))
                conn.commit()

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
