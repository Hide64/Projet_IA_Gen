import os
import time
import argparse
import requests
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
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

USER_ID = 1  # ton user Hide

def tmdb_get(path, params=None):
    params = params or {}
    params["api_key"] = TMDB_KEY
    r = requests.get(f"{TMDB_BASE}{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def normalize_date(d):
    if not d or str(d) == "1970-01-01":
        return None
    return d

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
            FROM import_seen_sc
            WHERE match_status='MATCHED'
              AND tmdb_id IS NOT NULL
              AND (match_note IS NULL OR match_note NOT LIKE '%%applied%%')
            ORDER BY import_seen_id
            LIMIT %s;
        """, (args.limit,))
        rows = cur.fetchall()

        print(f"[apply_seen] items: {len(rows)}")

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
                      overview=EXCLUDED.overview
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

                watched = normalize_date(r["watched_date"])

                # upsert user_film
                cur.execute("""
                    INSERT INTO user_film
                      (user_id, film_id, status, rating_10, first_seen_at, last_seen_at, rewatch_count)
                    VALUES (%s,%s,'SEEN',%s,%s,%s,0)
                    ON CONFLICT (user_id, film_id) DO UPDATE SET
                      status='SEEN',
                      rating_10=EXCLUDED.rating_10,
                      last_seen_at=EXCLUDED.last_seen_at,
                      updated_at=now();
                """, (
                    USER_ID,
                    film_id,
                    r["rating_10"],
                    watched,
                    watched,
                ))

                # insert watch_event
                if watched:
                    cur.execute("""
                        INSERT INTO watch_event
                          (user_id, film_id, watched_at, rating_10, context)
                        VALUES (%s,%s,%s,%s,'senscritique_import');
                    """, (
                        USER_ID,
                        film_id,
                        datetime.combine(watched, datetime.min.time()),
                        r["rating_10"],
                    ))

                cur.execute("""
                    UPDATE import_seen_sc
                    SET match_note = COALESCE(match_note,'') || ' | applied'
                    WHERE import_seen_id=%s;
                """, (r["import_seen_id"],))

                conn.commit()
                time.sleep(args.sleep)

            except Exception as e:
                conn.rollback()
                cur.execute("""
                    UPDATE import_seen_sc
                    SET match_status='ERROR', match_note=%s
                    WHERE import_seen_id=%s;
                """, (str(e)[:900], r["import_seen_id"]))
                conn.commit()

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
