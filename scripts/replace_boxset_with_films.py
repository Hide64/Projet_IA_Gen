import os
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

def tmdb_get(path, params=None):
    params = params or {}
    params["api_key"] = TMDB_KEY
    r = requests.get(f"{TMDB_BASE}{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def directors(tmdb_id: int):
    c = tmdb_get(f"/movie/{tmdb_id}/credits", {"language": "en-US"})
    return [x["name"] for x in c.get("crew", []) if x.get("job") == "Director"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--boxset-id", type=int, required=True, help="import_br_id du coffret")
    ap.add_argument("--ids", nargs="+", required=True, help="liste de tmdb_id des films du coffret")
    ap.add_argument("--delete-boxset", action="store_true", help="supprime la ligne coffret après remplacement")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    film_ids = [int(x) for x in args.ids]

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    with conn, conn.cursor(cursor_factory=DictCursor) as cur:
        # 1) charger la ligne coffret
        cur.execute("""
            SELECT *
            FROM import_br
            WHERE import_br_id = %s
        """, (args.boxset_id,))
        box = cur.fetchone()
        if not box:
            raise SystemExit(f"Boxset import_br_id={args.boxset_id} introuvable")

        print("="*80)
        print(f"BOXSET #{args.boxset_id}: {box['raw_title_clean']}")
        print("formats:", box["formats"], "| copies:", box["copies"], "| discs:", box["number_of_discs"])
        print("="*80)

        # 2) preview TMDb + confirmation
        to_insert = []
        for tmdb_id in film_ids:
            d = tmdb_get(f"/movie/{tmdb_id}", {"language": "fr-FR"})
            title = d.get("title") or ""
            release_date = d.get("release_date") or ""
            year = release_date[:4] if release_date else ""
            dirs = directors(tmdb_id)
            print(f"- TMDb {tmdb_id}: {title} ({year}) | Dir: {', '.join(dirs) if dirs else '(?)'}")

            ans = input("  -> importer ce film dans staging BR ? [y/N] ").strip().lower()
            if ans != "y":
                continue

            to_insert.append((tmdb_id, title, d.get("overview")))

        if not to_insert:
            print("Aucun film confirmé, fin.")
            conn.rollback()
            return

        if args.dry_run:
            print("DRY RUN: aucune insertion/suppression effectuée.")
            conn.rollback()
            return

        # 3) insérer une ligne import_br par film confirmé (copie des infos physiques du coffret)
        for (tmdb_id, title, overview) in to_insert:
            cur.execute("""
                INSERT INTO import_br (
                  item_type,
                  raw_title,
                  raw_creators,
                  description,
                  publish_date,
                  tags,
                  notes,
                  price,
                  length_min,
                  number_of_discs,
                  ensemble,
                  aspect_ratio,
                  rating,
                  review,
                  review_date,
                  status,
                  began,
                  completed,
                  added,
                  copies,
                  raw_title_clean,
                  is_physical,
                  formats,
                  tmdb_id,
                  match_status,
                  match_note
                ) VALUES (
                  'movie',
                  %s,
                  NULL,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  NULL,
                  %s,
                  NULL,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  TRUE,
                  %s,
                  %s,
                  'MATCHED',
                  %s
                );
            """, (
                title,                               # raw_title
                overview,                            # description (TMDb)
                box["publish_date"],
                box["tags"],
                (box["notes"] or "") + f" | from_boxset:{box['import_br_id']}",
                box["price"],
                box["number_of_discs"],              # on duplique (cf remarque)
                box["aspect_ratio"],
                box["rating"],
                box["review"],
                box["review_date"],
                box["status"],
                box["began"],
                box["completed"],
                box["added"],
                box["copies"],
                title,                               # raw_title_clean
                box["formats"],
                tmdb_id,
                f"manual_boxset_replace:{box['raw_title_clean']}"
            ))

        # 4) supprimer ou archiver la ligne coffret
        if args.delete_boxset:
            cur.execute("DELETE FROM import_br WHERE import_br_id=%s;", (args.boxset_id,))
        else:
            cur.execute("""
                UPDATE import_br
                SET match_status='REPLACED',
                    match_note = COALESCE(match_note,'') || ' | replaced_by_manual_ids'
                WHERE import_br_id=%s;
            """, (args.boxset_id,))

        conn.commit()
        print("OK: films ajoutés au staging et coffret traité.")

    conn.close()

if __name__ == "__main__":
    main()
