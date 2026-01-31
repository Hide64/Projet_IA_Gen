import os, re, argparse, requests
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

def tmdb_get(path, params=None):
    params = params or {}
    params["api_key"] = TMDB_KEY
    r = requests.get(f"{TMDB_BASE}{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def directors(tmdb_id: int):
    credits = tmdb_get(f"/movie/{tmdb_id}/credits", params={"language":"fr-FR"})
    return [c["name"] for c in credits.get("crew", []) if c.get("job") == "Director"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", type=int, help="import_nas_id (optional). If omitted, lists all ambiguous.")
    args = ap.parse_args()

    conn = psycopg2.connect(**DB)
    with conn, conn.cursor(cursor_factory=DictCursor) as cur:
        if args.id:
            cur.execute("""SELECT * FROM import_nas WHERE import_nas_id=%s""", (args.id,))
        else:
            cur.execute("""SELECT * FROM import_nas WHERE match_status='AMBIGUOUS' ORDER BY import_nas_id""")
        rows = cur.fetchall()

    for row in rows:
        iid = row["import_nas_id"]
        title = row["raw_title"]
        year = row["raw_year"]
        raw_dir = row["raw_director"]
        note = row["match_note"] or ""
        print("\n" + "="*80)
        print(f"import_nas_id={iid} | {title} ({year}) | director_raw={raw_dir}")
        print(f"match_note={note}")

        m = re.search(r"candidates=([0-9,]+)", note)
        if not m:
            print("No candidates list found in match_note.")
            continue

        ids = [int(x) for x in m.group(1).split(",") if x.strip().isdigit()]
        for tmdb_id in ids:
            d = tmdb_get(f"/movie/{tmdb_id}", params={"language":"fr-FR"})
            rel = d.get("release_date") or ""
            print(f"\nTMDb {tmdb_id}: {d.get('title')} | release={rel} | original={d.get('original_title')}")
            print("Directors:", ", ".join(directors(tmdb_id)) or "(none)")
            print("Overview:", (d.get("overview") or "")[:200].replace("\n"," ") + ("..." if d.get("overview") and len(d.get("overview"))>200 else ""))

if __name__ == "__main__":
    main()
