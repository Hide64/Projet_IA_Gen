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
    raise RuntimeError(f"TMDb rate-limited too long on {path}")

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def director_hint(row):
    # Prefer raw_creators, else first+last
    if row.get("raw_creators"):
        return norm(row["raw_creators"])
    fn = row.get("raw_first_name") or ""
    ln = row.get("raw_last_name") or ""
    full = (fn + " " + ln).strip()
    return norm(full) if full else ""

def score_candidate(q_title, q_year, q_dir, cand):
    # simple heuristic scoring
    s = 0
    title = norm(cand.get("title") or "")
    orig = norm(cand.get("original_title") or "")
    if title == q_title or orig == q_title:
        s += 5
    elif q_title and (q_title in title or q_title in orig):
        s += 2

    if q_year:
        rd = cand.get("release_date") or ""
        if rd.startswith(str(q_year)):
            s += 3

    # director: requires extra call (credits) -> we only do if ambiguous
    # here we just return base score
    return s

def get_directors_for_tmdb_id(tmdb_id: int) -> list[str]:
    credits = tmdb_get(f"/movie/{tmdb_id}/credits", params={"language": "en-US"})
    return [norm(c.get("name")) for c in credits.get("crew", []) if c.get("job") == "Director"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--sleep", type=float, default=0.25)
    args = ap.parse_args()

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    with conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT import_br_id, raw_title_clean, publish_date, raw_creators, raw_first_name, raw_last_name
            FROM import_br
            WHERE match_status IN ('NOT_FOUND','ERROR')
              AND (tmdb_id IS NULL OR tmdb_id = 0)
              AND raw_title_clean IS NOT NULL AND raw_title_clean <> ''
            ORDER BY import_br_id
            LIMIT %s;
        """, (args.limit,))
        items = cur.fetchall()

        print(f"Items to match: {len(items)}")

        for it in items:
            iid = it["import_br_id"]
            title_clean = (it["raw_title_clean"] or "").strip()
            #year = it["publish_date"].year if it["publish_date"] else None
            year = None  # publish_date = date d'édition BR, pas l'année du film
            dir_hint = director_hint(it)

            try:
                # Search TMDb
                search = tmdb_get("/search/movie", params={
                    "query": title_clean,
                    "language": "fr-FR",
                    "include_adult": "false",
                    #**({"year": year} if year else {})
                })
                results = search.get("results", []) or []

                if not results:
                    cur.execute("""
                        UPDATE import_br
                        SET match_status='NOT_FOUND', match_note=%s
                        WHERE import_br_id=%s;
                    """, (f"no result for '{title_clean}'", iid))
                    conn.commit()
                    time.sleep(args.sleep)
                    continue

                # Score candidates
                q_title = norm(title_clean)
                scored = [(score_candidate(q_title, year, dir_hint, r), r) for r in results[:10]]
                scored.sort(key=lambda x: x[0], reverse=True)
                best_score, best = scored[0]

                # If clearly best, accept
                # If multiple close, use director to disambiguate
                top = scored[:3]
                ambiguous = len(top) >= 2 and top[0][0] == top[1][0]

                chosen = best
                note = f"best_score={best_score}"

                if ambiguous and dir_hint:
                    # fetch directors for top candidates only
                    for _, cand in top:
                        tmdb_id = cand["id"]
                        dirs = get_directors_for_tmdb_id(tmdb_id)
                        if any(dir_hint in d or d in dir_hint for d in dirs):
                            chosen = cand
                            note += f" | dir_match={dir_hint}"
                            ambiguous = False
                            break

                if ambiguous:
                    # store first candidate but flag as ambiguous
                    cur.execute("""
                        UPDATE import_br
                        SET match_status='AMBIGUOUS', tmdb_id=%s, match_note=%s
                        WHERE import_br_id=%s;
                    """, (chosen["id"], f"ambiguous | {note}", iid))
                else:
                    cur.execute("""
                        UPDATE import_br
                        SET match_status='MATCHED', tmdb_id=%s, match_note=%s
                        WHERE import_br_id=%s;
                    """, (chosen["id"], f"single result | {note}", iid))

                conn.commit()
                time.sleep(args.sleep)

            except Exception as e:
                conn.rollback()
                cur.execute("""
                    UPDATE import_br
                    SET match_status='ERROR', match_note=%s
                    WHERE import_br_id=%s;
                """, (f"match: {str(e)[:900]}", iid))
                conn.commit()

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
