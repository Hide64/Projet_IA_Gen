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

STOP = {"le","la","les","un","une","the","a","an","of"}
def simplify_title(title: str) -> str:
    t = (title or "").lower()
    t = re.sub(r"\[[^\]]*\]", "", t)
    t = re.split(r"[:–\-]", t)[0]
    words = [w for w in re.findall(r"[a-z0-9]+", t) if w not in STOP]
    return " ".join(words[:5]).strip()

def get_directors(tmdb_id: int):
    credits = tmdb_get(f"/movie/{tmdb_id}/credits")
    return [norm(c.get("name","")) for c in credits.get("crew", []) if c.get("job") == "Director"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--sleep", type=float, default=0.25)
    args = ap.parse_args()

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    with conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT import_id, raw_title, raw_year, raw_directors
            FROM import_watchlist_sc
            WHERE match_status='PENDING'
            ORDER BY import_id
            LIMIT %s;
        """, (args.limit,))
        items = cur.fetchall()

        print(f"[match_watchlist] items: {len(items)}")

        for it in items:
            iid = it["import_id"]
            raw_title = it["raw_title"]
            year_q = it["raw_year"]
            director_q = norm(it.get("raw_directors"))

            try:
                # 2 passes: raw title then simplified
                def do_search(q):
                    return tmdb_get("/search/movie", {
                        "query": q,
                        "language": "fr-FR",
                        "include_adult": "false",
                    }).get("results", [])

                results = do_search(raw_title)
                used_q = raw_title
                if not results:
                    q2 = simplify_title(raw_title)
                    if q2 and q2 != raw_title:
                        results = do_search(q2)
                        used_q = q2

                if not results:
                    cur.execute("""
                        UPDATE import_watchlist_sc
                        SET match_status='NOT_FOUND', match_note=%s
                        WHERE import_id=%s;
                    """, (f"no result | q={used_q}", iid))
                    conn.commit()
                    continue

                # pick best with simple scoring
                title_q = norm(raw_title)
                best = None
                best_score = -1
                for cand in results[:10]:
                    s = 0
                    t = norm(cand.get("title"))
                    ot = norm(cand.get("original_title"))
                    if t == title_q or ot == title_q:
                        s += 5
                    elif title_q in t or title_q in ot:
                        s += 2
                    if year_q and (cand.get("release_date") or "").startswith(str(year_q)):
                        s += 3
                    if s > best_score:
                        best_score = s
                        best = cand

                chosen_id = best["id"]
                note = f"score={best_score} | q={used_q}"

                # resolve ambiguity with director
                ambiguous = len(results) > 1
                if ambiguous and director_q:
                    # try top 5 to find director match
                    picked = None
                    for cand in results[:5]:
                        try:
                            dirs = get_directors(cand["id"])
                        except Exception:
                            dirs = []
                        if any(director_q in d or d in director_q for d in dirs):
                            picked = cand["id"]
                            note += " | director_match"
                            break
                    if picked:
                        chosen_id = picked
                        ambiguous = False
                    else:
                        # remain ambiguous
                        ambiguous = True

                cur.execute("""
                    UPDATE import_watchlist_sc
                    SET tmdb_id=%s,
                        match_status=%s,
                        match_note=%s
                    WHERE import_id=%s;
                """, (chosen_id, "AMBIGUOUS" if ambiguous else "MATCHED", note, iid))
                conn.commit()
                time.sleep(args.sleep)

            except Exception as e:
                conn.rollback()
                cur.execute("""
                    UPDATE import_watchlist_sc
                    SET match_status='ERROR', match_note=%s
                    WHERE import_id=%s;
                """, (str(e)[:900], iid))
                conn.commit()

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
