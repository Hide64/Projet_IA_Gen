import os, re, time, argparse, requests
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

def get_directors(tmdb_id: int):
    credits = tmdb_get(f"/movie/{tmdb_id}/credits")
    return [norm(c.get("name","")) for c in credits.get("crew", []) if c.get("job") == "Director"]

def simplify_title(title: str) -> str:
    # agressif mais efficace pour NOT_FOUND
    stop = {"le","la","les","un","une","the","a","an","of"}
    t = title.lower()
    t = re.sub(r"\[[^\]]*\]", "", t)
    t = re.split(r"[:–\-]", t)[0]
    words = [w for w in re.findall(r"[a-z0-9]+", t) if w not in stop]
    return " ".join(words[:5]).strip()

def search_candidates(raw_title: str):
    # 2 passes : titre brut, puis titre simplifié
    q1 = raw_title
    r1 = tmdb_get("/search/movie", {"query": q1, "language": "fr-FR", "include_adult": "false"}).get("results", [])
    if r1:
        return r1[:8], q1
    q2 = simplify_title(raw_title)
    r2 = tmdb_get("/search/movie", {"query": q2, "language": "fr-FR", "include_adult": "false"}).get("results", [])
    return r2[:8], q2

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--statuses", default="ERROR,AMBIGUOUS,NOT_FOUND")
    ap.add_argument("--sleep", type=float, default=0.2)
    args = ap.parse_args()

    wanted = tuple(s.strip().upper() for s in args.statuses.split(",") if s.strip())

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    with conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT import_seen_id, raw_title, raw_year, raw_directors, tmdb_id, match_status, match_note
            FROM import_seen_sc
            WHERE match_status = ANY(%s)
            ORDER BY import_seen_id
            LIMIT %s;
        """, (list(wanted), args.limit))
        rows = cur.fetchall()

        print(f"[resolve] items: {len(rows)} statuses={wanted}")

        for r in rows:
            iid = r["import_seen_id"]
            title = r["raw_title"]
            year = r["raw_year"]
            director_q = norm(r["raw_directors"])
            print("\n" + "="*80)
            print(f"#{iid} [{r['match_status']}] {title} ({year}) | director: {r['raw_directors']}")
            if r["match_note"]:
                print(f"note: {r['match_note'][:160]}")

            cands, used_query = search_candidates(title)
            print(f"TMDb query used: {used_query!r}")
            if not cands:
                print("No candidates found. Enter 's' to skip or a TMDb id if you already know it.")
            else:
                for idx, c in enumerate(cands, start=1):
                    tmdb_id = c["id"]
                    rel = c.get("release_date") or ""
                    t = c.get("title") or ""
                    ot = c.get("original_title") or ""
                    dirs = []
                    try:
                        dirs = get_directors(tmdb_id)
                    except Exception:
                        pass
                    dmatch = "DIRECTOR_MATCH" if any(director_q in d or d in director_q for d in dirs) else ""
                    print(f"{idx:>2}. id={tmdb_id} | {t} / {ot} | {rel} {dmatch}")

            choice = input("Choose: [1-8]=pick, 'id:<tmdbid>'=set id, 'n'=mark NOT_FOUND, 's'=skip : ").strip().lower()

            if choice == "s" or choice == "":
                continue
            if choice == "n":
                cur.execute("""
                    UPDATE import_seen_sc
                    SET match_status='NOT_FOUND',
                        match_note=COALESCE(match_note,'') || ' | manual_not_found'
                    WHERE import_seen_id=%s;
                """, (iid,))
                conn.commit()
                continue

            chosen_id = None
            if choice.startswith("id:"):
                try:
                    chosen_id = int(choice.split(":",1)[1].strip())
                except Exception:
                    print("Invalid id: format.")
                    continue
            else:
                try:
                    k = int(choice)
                    if 1 <= k <= len(cands):
                        chosen_id = int(cands[k-1]["id"])
                except Exception:
                    print("Invalid choice.")
                    continue

            if not chosen_id:
                print("No id chosen.")
                continue

            cur.execute("""
                UPDATE import_seen_sc
                SET tmdb_id=%s,
                    match_status='MATCHED',
                    match_note=COALESCE(match_note,'') || ' | manual_fix'
                WHERE import_seen_id=%s;
            """, (chosen_id, iid))
            conn.commit()
            time.sleep(args.sleep)

    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
