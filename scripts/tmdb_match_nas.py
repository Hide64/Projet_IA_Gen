import os
import re
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

def norm_name(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9àâäçéèêëîïôöùûüÿñ\s'-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

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

def tmdb_search_movie(title, year=None):
    params = {"query": title, "include_adult": "false", "language": "fr-FR"}
    if year:
        params["year"] = year
    data = tmdb_get("/search/movie", params=params)
    return data.get("results", [])

def tmdb_directors_for_movie(tmdb_id: int):
    credits = tmdb_get(f"/movie/{tmdb_id}/credits", params={"language": "fr-FR"})
    directors = []
    for crew in credits.get("crew", []):
        if crew.get("job") == "Director":
            directors.append(crew.get("name", ""))
    return [norm_name(d) for d in directors if d]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100, help="Nb de lignes à traiter par run")
    ap.add_argument("--sleep", type=float, default=0.25, help="Pause entre appels TMDb")
    args = ap.parse_args()

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    with conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT import_nas_id, raw_title, raw_year, raw_director
            FROM import_nas
            WHERE match_status='PENDING'
            ORDER BY import_nas_id
            LIMIT %s;
        """, (args.limit,))
        items = cur.fetchall()

        for it in items:
            iid = it["import_nas_id"]
            title = it["raw_title"]
            year = it["raw_year"]
            raw_dir = norm_name(it["raw_director"] or "")

            try:
                results = tmdb_search_movie(title, year)

                if not results:
                    cur.execute("""
                        UPDATE import_nas
                        SET match_status='NOT_FOUND', match_note=%s
                        WHERE import_nas_id=%s
                    """, ("no result", iid))
                    conn.commit()
                    continue

                # On garde les 5 premiers candidats
                candidates = results[:5]

                # 1) Si un seul candidat => MATCHED
                if len(candidates) == 1:
                    tmdb_id = candidates[0]["id"]
                    cur.execute("""
                        UPDATE import_nas
                        SET tmdb_id=%s, match_status='MATCHED', match_note=%s
                        WHERE import_nas_id=%s
                    """, (tmdb_id, "single result", iid))
                    conn.commit()
                    time.sleep(args.sleep)
                    continue

                # 2) Si on a un réalisateur brut, on s'en sert pour départager
                if raw_dir:
                    scored = []
                    for c in candidates:
                        cid = c["id"]
                        dirs = tmdb_directors_for_movie(cid)
                        # score simple : match exact du nom normalisé
                        score = 1 if raw_dir in dirs else 0
                        scored.append((score, cid, dirs))
                        time.sleep(args.sleep)

                    scored.sort(reverse=True, key=lambda x: x[0])
                    best_score, best_id, best_dirs = scored[0]

                    if best_score == 1:
                        cur.execute("""
                            UPDATE import_nas
                            SET tmdb_id=%s, match_status='MATCHED', match_note=%s
                            WHERE import_nas_id=%s
                        """, (best_id, f"director match: {best_dirs}", iid))
                        conn.commit()
                        continue

                # 3) Sinon -> AMBIGUOUS (on stocke un peu d'info)
                cand_ids = [str(c["id"]) for c in candidates]
                cur.execute("""
                    UPDATE import_nas
                    SET match_status='AMBIGUOUS', match_note=%s
                    WHERE import_nas_id=%s
                """, (f"candidates={','.join(cand_ids)}", iid))
                conn.commit()

            except Exception as e:
                conn.rollback()
                cur.execute("""
                    UPDATE import_nas
                    SET match_status='ERROR', match_note=%s
                    WHERE import_nas_id=%s
                """, (str(e)[:1000], iid))
                conn.commit()

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
