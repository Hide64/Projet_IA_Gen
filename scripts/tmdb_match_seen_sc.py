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
    raise RuntimeError("TMDb rate limit")

def norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\[[^\]]*\]", "", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def score_candidate(title_q, year_q, director_q, cand):
    s = 0
    title = norm(cand.get("title"))
    orig = norm(cand.get("original_title"))

    if title == title_q or orig == title_q:
        s += 5
    elif title_q in title or title_q in orig:
        s += 2

    if year_q:
        rd = cand.get("release_date") or ""
        if rd.startswith(str(year_q)):
            s += 3

    return s

def get_directors(tmdb_id):
    credits = tmdb_get(f"/movie/{tmdb_id}/credits")
    return [norm(c["name"]) for c in credits.get("crew", []) if c.get("job") == "Director"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--sleep", type=float, default=0.25)
    args = ap.parse_args()

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    with conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT import_seen_id, raw_title, raw_year, raw_directors
            FROM import_seen_sc
            WHERE match_status='PENDING'
            ORDER BY import_seen_id
            LIMIT %s;
        """, (args.limit,))
        rows = cur.fetchall()

        print(f"[match_seen] items: {len(rows)}")

        for r in rows:
            iid = r["import_seen_id"]
            title_q = norm(r["raw_title"])
            year_q = r["raw_year"]
            director_q = norm(r["raw_directors"])

            try:
                search = tmdb_get("/search/movie", {
                    "query": r["raw_title"],
                    "language": "fr-FR",
                    "include_adult": "false",
                })
                results = search.get("results", [])

                if not results:
                    cur.execute("""
                        UPDATE import_seen_sc
                        SET match_status='NOT_FOUND', match_note=%s
                        WHERE import_seen_id=%s;
                    """, (f"no result for '{r['raw_title']}'", iid))
                    conn.commit()
                    continue

                scored = [(score_candidate(title_q, year_q, director_q, c), c) for c in results[:10]]
                scored.sort(key=lambda x: x[0], reverse=True)
                best_score, best = scored[0]

                ambiguous = len(scored) > 1 and scored[1][0] == best_score

                chosen = best
                note = f"score={best_score}"

                if ambiguous:
                    for _, cand in scored[:3]:
                        dirs = get_directors(cand["id"])
                        if any(director_q in d or d in director_q for d in dirs):
                            chosen = cand
                            ambiguous = False
                            note += " | director_match"
                            break

                if ambiguous:
                    cur.execute("""
                        UPDATE import_seen_sc
                        SET match_status='AMBIGUOUS', tmdb_id=%s, match_note=%s
                        WHERE import_seen_id=%s;
                    """, (chosen["id"], note, iid))
                else:
                    cur.execute("""
                        UPDATE import_seen_sc
                        SET match_status='MATCHED', tmdb_id=%s, match_note=%s
                        WHERE import_seen_id=%s;
                    """, (chosen["id"], note, iid))

                conn.commit()
                time.sleep(args.sleep)

            except Exception as e:
                conn.rollback()
                cur.execute("""
                    UPDATE import_seen_sc
                    SET match_status='ERROR', match_note=%s
                    WHERE import_seen_id=%s;
                """, (str(e)[:900], iid))
                conn.commit()

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
