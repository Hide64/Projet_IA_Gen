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

def ensure_source(cur, code, label):
    cur.execute("""
      INSERT INTO source (code, label)
      VALUES (%s, %s)
      ON CONFLICT (code) DO UPDATE SET label=EXCLUDED.label
      RETURNING source_id;
    """, (code, label))
    return cur.fetchone()[0]

def upsert_film(cur, d):
    tmdb_id = d["id"]
    imdb_id = d.get("imdb_id")
    title = d.get("title")
    original_title = d.get("original_title")
    release_date = d.get("release_date") or None
    year = int(release_date[:4]) if release_date else None
    runtime_min = d.get("runtime")
    overview = d.get("overview")
    original_language = d.get("original_language")
    poster_path = d.get("poster_path")
    backdrop_path = d.get("backdrop_path")
    popularity = d.get("popularity")
    vote_avg = d.get("vote_average")
    vote_count = d.get("vote_count")

    cur.execute("""
      INSERT INTO film (tmdb_id, imdb_id, title, original_title, release_date, year, runtime_min,
                        overview, original_language, poster_path, backdrop_path,
                        tmdb_popularity, tmdb_vote_avg, tmdb_vote_count)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      ON CONFLICT (tmdb_id) DO UPDATE SET
        imdb_id=EXCLUDED.imdb_id,
        title=EXCLUDED.title,
        original_title=EXCLUDED.original_title,
        release_date=EXCLUDED.release_date,
        year=EXCLUDED.year,
        runtime_min=EXCLUDED.runtime_min,
        overview=EXCLUDED.overview,
        original_language=EXCLUDED.original_language,
        poster_path=EXCLUDED.poster_path,
        backdrop_path=EXCLUDED.backdrop_path,
        tmdb_popularity=EXCLUDED.tmdb_popularity,
        tmdb_vote_avg=EXCLUDED.tmdb_vote_avg,
        tmdb_vote_count=EXCLUDED.tmdb_vote_count,
        updated_at=now()
      RETURNING film_id;
    """, (tmdb_id, imdb_id, title, original_title, release_date, year, runtime_min,
          overview, original_language, poster_path, backdrop_path, popularity, vote_avg, vote_count))
    return cur.fetchone()[0]

def upsert_genres(cur, film_id, genres):
    for g in genres:
        cur.execute("""
          INSERT INTO genre (tmdb_genre_id, name)
          VALUES (%s, %s)
          ON CONFLICT (tmdb_genre_id) DO UPDATE SET name=EXCLUDED.name
          RETURNING genre_id;
        """, (g["id"], g["name"]))
        genre_id = cur.fetchone()[0]
        cur.execute("""
          INSERT INTO film_genre (film_id, genre_id)
          VALUES (%s, %s)
          ON CONFLICT DO NOTHING;
        """, (film_id, genre_id))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--sleep", type=float, default=0.25)
    args = ap.parse_args()

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    with conn, conn.cursor(cursor_factory=DictCursor) as cur:
        # s'assure que NAS existe dans source
        nas_source_id = ensure_source(cur, "NAS", "NAS")

        cur.execute("""
            SELECT import_nas_id, tmdb_id, raw_file_path, raw_file, raw_title
            FROM import_nas
            WHERE match_status='MATCHED'
                AND tmdb_id IS NOT NULL
                AND (match_note IS NULL OR match_note NOT LIKE %s)
            ORDER BY import_nas_id
            LIMIT %s;
        """, ("%applied%", args.limit,))
        items = cur.fetchall()

        for it in items:
            iid = it["import_nas_id"]
            tmdb_id = it["tmdb_id"]

            try:
                details = tmdb_get(f"/movie/{tmdb_id}", params={"language": "fr-FR"})
                film_id = upsert_film(cur, details)
                upsert_genres(cur, film_id, details.get("genres", []))

                # lien source NAS
                cur.execute("""
                  INSERT INTO film_source (film_id, source_id, is_available)
                  VALUES (%s, %s, TRUE)
                  ON CONFLICT (film_id, source_id) DO NOTHING;
                """, (film_id, nas_source_id))

                # fichier NAS (évite doublons par path unique)
                cur.execute("""
                  INSERT INTO nas_asset (film_id, path, scanned_at)
                  VALUES (%s, %s, now())
                  ON CONFLICT (path) DO UPDATE SET
                    film_id=EXCLUDED.film_id,
                    scanned_at=now();
                """, (film_id, it["raw_file_path"]))

                # marquer appliqué
                cur.execute("""
                    UPDATE import_nas
                    SET match_status='APPLIED',
                        match_note = COALESCE(match_note,'') || ' | applied'
                    WHERE import_nas_id=%s
                """, (iid,))

                conn.commit()
                time.sleep(args.sleep)

            except Exception as e:
                conn.rollback()
                cur.execute("""
                  UPDATE import_nas
                  SET match_status='ERROR', match_note=%s
                  WHERE import_nas_id=%s
                """, (f"apply: {str(e)[:900]}", iid))
                conn.commit()

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
