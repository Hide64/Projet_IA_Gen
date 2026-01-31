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

def tmdb_get(path, params=None, retry=3):
    params = params or {}
    params["api_key"] = TMDB_KEY
    for i in range(retry):
        r = requests.get(f"{TMDB_BASE}{path}", params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(1.5 + i)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"TMDb rate-limited too long on {path}")

def upsert_person(cur, p):
    cur.execute("""
        INSERT INTO person (tmdb_person_id, name)
        VALUES (%s, %s)
        ON CONFLICT (tmdb_person_id) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING person_id;
    """, (p["id"], p["name"]))
    return cur.fetchone()[0]

def link_director(cur, film_id, person_id):
    cur.execute("""
        INSERT INTO film_credit (film_id, person_id, department, job)
        VALUES (%s, %s, 'Directing', 'Director')
        ON CONFLICT DO NOTHING;
    """, (film_id, person_id))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--sleep", type=float, default=0.25)
    args = ap.parse_args()

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    with conn, conn.cursor(cursor_factory=DictCursor) as cur:

        # films NAS sans réalisateur encore importé
        cur.execute("""
            SELECT DISTINCT f.film_id, f.tmdb_id
            FROM film f
            JOIN film_source fs ON fs.film_id = f.film_id
            JOIN source s ON s.source_id = fs.source_id
            LEFT JOIN film_credit fc
                   ON fc.film_id = f.film_id
                  AND fc.job = 'Director'
            WHERE s.code = 'NAS'
              AND fc.film_id IS NULL
            ORDER BY f.film_id
            LIMIT %s;
        """, (args.limit,))
        films = cur.fetchall()

        for film in films:
            film_id = film["film_id"]
            tmdb_id = film["tmdb_id"]

            try:
                credits = tmdb_get(f"/movie/{tmdb_id}/credits", params={"language": "fr-FR"})
                directors = [
                    c for c in credits.get("crew", [])
                    if c.get("job") == "Director"
                ]

                for d in directors:
                    person_id = upsert_person(cur, d)
                    link_director(cur, film_id, person_id)

                conn.commit()
                time.sleep(args.sleep)

            except Exception as e:
                conn.rollback()
                print(f"[ERROR] film_id={film_id} tmdb_id={tmdb_id} -> {e}")

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
