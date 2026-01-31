import os
import re
import csv
import argparse
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

load_dotenv()

DB = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "videotheque"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
)

def norm_name(s: str) -> str:
    """Normalize names for comparison (casefold + remove punctuation/extra spaces)."""
    if s is None:
        return ""
    s = s.strip().casefold()
    # keep letters/numbers/space/'- to be permissive with accents
    s = re.sub(r"[^\w\s'-]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="reports/director_mismatches_nas.csv", help="Output CSV path")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of mismatches printed (0 = all)")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    conn = psycopg2.connect(**DB)

    # 1) Pull NAS-linked films and their raw_director (staging) via path join
    # We use DISTINCT ON (path) to avoid duplicates if staging accidentally has duplicates.
    # If you want the newest staging row per path, ORDER BY created_at DESC.
    sql = """
    WITH nas_map AS (
      SELECT DISTINCT ON (i.raw_file_path)
        i.raw_file_path AS path,
        i.import_nas_id,
        i.raw_title,
        i.raw_year,
        i.raw_director
      FROM import_nas i
      WHERE i.raw_file_path IS NOT NULL
      ORDER BY i.raw_file_path, i.created_at DESC
    ),
    directors AS (
      SELECT
        fc.film_id,
        array_agg(DISTINCT p.name) AS directors_db
      FROM film_credit fc
      JOIN person p ON p.person_id = fc.person_id
      WHERE fc.job = 'Director'
      GROUP BY fc.film_id
    )
    SELECT
      f.film_id,
      f.tmdb_id,
      f.title,
      f.year,
      na.path,
      nm.import_nas_id,
      nm.raw_director,
      d.directors_db
    FROM nas_asset na
    JOIN film f ON f.film_id = na.film_id
    LEFT JOIN nas_map nm ON nm.path = na.path
    LEFT JOIN directors d ON d.film_id = f.film_id
    ORDER BY f.film_id;
    """

    mismatches = []
    with conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

        for r in rows:
            raw_dir = (r["raw_director"] or "").strip()
            dirs_db = r["directors_db"] or []

            # If no raw director, skip (cannot compare)
            if not raw_dir:
                continue

            raw_n = norm_name(raw_dir)
            db_norms = [norm_name(x) for x in dirs_db if x]

            # If DB has no director imported yet, count as mismatch (optional)
            # Here: we flag it as mismatch because it indicates missing credits.
            if not db_norms:
                mismatches.append({
                    "film_id": r["film_id"],
                    "tmdb_id": r["tmdb_id"],
                    "title": r["title"],
                    "year": r["year"],
                    "path": r["path"],
                    "import_nas_id": r["import_nas_id"],
                    "raw_director": raw_dir,
                    "directors_db": "",
                    "reason": "NO_DIRECTOR_IN_DB"
                })
                continue

            # Match if raw director is contained in any DB director (or exact match)
            # (helps when raw has "Nom Prénom" and DB has same; we keep it strict-ish)
            match = any(raw_n == dn for dn in db_norms)

            if not match:
                mismatches.append({
                    "film_id": r["film_id"],
                    "tmdb_id": r["tmdb_id"],
                    "title": r["title"],
                    "year": r["year"],
                    "path": r["path"],
                    "import_nas_id": r["import_nas_id"],
                    "raw_director": raw_dir,
                    "directors_db": " | ".join(dirs_db),
                    "reason": "DIRECTOR_MISMATCH"
                })

    conn.close()

    # Write CSV
    fieldnames = ["film_id", "tmdb_id", "title", "year", "path", "import_nas_id", "raw_director", "directors_db", "reason"]
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for m in mismatches:
            w.writerow(m)

    # Print summary + a preview
    print(f"Found {len(mismatches)} mismatches. CSV written to: {args.out}")
    if mismatches:
        print("\nPreview:")
        n = len(mismatches) if args.limit == 0 else min(args.limit, len(mismatches))
        for i in range(n):
            m = mismatches[i]
            print(f"- film_id={m['film_id']} tmdb_id={m['tmdb_id']} | {m['title']} ({m['year']})")
            print(f"  raw_director: {m['raw_director']}")
            print(f"  directors_db: {m['directors_db']}")
            print(f"  path: {m['path']}")
            print(f"  reason: {m['reason']}\n")

if __name__ == "__main__":
    main()
