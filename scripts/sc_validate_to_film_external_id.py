# -*- coding: utf-8 -*-
import os
import re
import argparse
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

RE_SC_ID = re.compile(r"/(\d+)(?:\?|$)")

def extract_sc_id(url: str):
    if not url or not isinstance(url, str):
        return None
    m = RE_SC_ID.search(url.strip())
    return m.group(1) if m else None

def get_conn(host: str, port: int, db: str, user: str, pwd: str):
    return psycopg2.connect(
        host=host, port=port, dbname=db, user=user, password=pwd,
        options="-c client_encoding=UTF8"
    )

def has_pg_trgm(cur) -> bool:
    cur.execute("SELECT 1 FROM pg_extension WHERE extname='pg_trgm'")
    return cur.fetchone() is not None

SQL_IS_ALREADY_MAPPED = """
SELECT 1
FROM film_external_id
WHERE provider = %(provider)s AND external_key = %(external_key)s
LIMIT 1;
"""

SQL_SUGGEST_TRGM = """
SELECT film_id, title, year, similarity(title, %(title)s) AS sim
FROM film
WHERE (%(year)s IS NULL OR year BETWEEN %(year)s - 1 AND %(year)s + 1)
ORDER BY similarity(title, %(title)s) DESC
LIMIT 5;
"""

SQL_SUGGEST_ILIKE = """
SELECT film_id, title, year,
       CASE
         WHEN title = %(title)s THEN 1.0
         WHEN title ILIKE %(title_like)s THEN 0.7
         ELSE 0.4
       END AS sim
FROM film
WHERE title ILIKE %(title_like)s
  AND (%(year)s IS NULL OR year BETWEEN %(year)s - 1 AND %(year)s + 1)
ORDER BY sim DESC, year DESC NULLS LAST
LIMIT 5;
"""

SQL_UPSERT_EXTERNAL_ID = """
INSERT INTO film_external_id (film_id, provider, external_key, external_url)
VALUES (%(film_id)s, %(provider)s, %(external_key)s, %(external_url)s)
ON CONFLICT (provider, external_key)
DO UPDATE SET
  film_id = EXCLUDED.film_id,
  external_url = EXCLUDED.external_url;
"""

def prompt_choice():
    return input("Choix (1-5 / s=skip / q=quit) > ").strip().lower()

def main():
    parser = argparse.ArgumentParser(description="Validate SC URL -> film_id and fill film_external_id (local CLI).")
    parser.add_argument("--csv", required=True, help="Chemin CSV SensCritique")
    parser.add_argument("--provider", default="senscritique", help="Valeur provider à écrire (default senscritique)")
    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "localhost"))
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PG_PORT", "5432")))
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "videotheque"))
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--pg-password", default=os.getenv("PG_PASSWORD", "postgres"))
    parser.add_argument("--limit", type=int, default=0, help="Limiter le nb de lignes (0=all)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)

    required = {"title", "year", "film_url", "annotation"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"[ERROR] Colonnes manquantes CSV: {missing}. Colonnes={list(df.columns)}")

    df["annotation"] = df["annotation"].fillna("").astype(str)
    df = df[df["annotation"].str.strip() != ""].copy()  # ✅ uniquement annotation non vide
    df["sc_id"] = df["film_url"].apply(extract_sc_id)
    df = df[df["sc_id"].notna()].copy()

    if args.limit and args.limit > 0:
        df = df.head(args.limit)

    if df.empty:
        print("[INFO] Aucun enregistrement avec annotation + sc_id.")
        return

    print(f"[INFO] Lignes à valider: {len(df)} (annotation non vide)")
    print(f"[INFO] provider={args.provider}")

    conn = get_conn(args.pg_host, args.pg_port, args.pg_db, args.pg_user, args.pg_password)
    conn.autocommit = False

    saved = skipped = already = 0

    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                trgm = has_pg_trgm(cur)
                print(f"[INFO] pg_trgm={'ON' if trgm else 'OFF'}")

                for idx, row in df.iterrows():
                    title = str(row["title"]).strip()
                    year_raw = row["year"]
                    year = int(year_raw) if pd.notna(year_raw) and str(year_raw).strip() != "" else None
                    film_url = str(row["film_url"]).strip()
                    sc_id = str(row["sc_id"]).strip()
                    ann = str(row["annotation"]).strip()

                    # Already mapped?
                    cur.execute(SQL_IS_ALREADY_MAPPED, {"provider": args.provider, "external_key": sc_id})
                    if cur.fetchone():
                        already += 1
                        continue

                    # Suggestions
                    if trgm:
                        cur.execute(SQL_SUGGEST_TRGM, {"title": title, "year": year})
                    else:
                        cur.execute(SQL_SUGGEST_ILIKE, {"title": title, "title_like": f"%{title}%", "year": year})
                    suggestions = cur.fetchall()

                    print("\n" + "="*90)
                    print(f"[{idx}] SC id={sc_id} | {title} ({year})")
                    print(f"URL: {film_url}")
                    print("Annotation (aperçu):")
                    print(ann[:300] + ("..." if len(ann) > 300 else ""))

                    if not suggestions:
                        print("\n(Aucune suggestion trouvée) -> s pour passer / q pour quitter")
                        choice = prompt_choice()
                        if choice == "q":
                            print("[INFO] Quit demandé.")
                            break
                        skipped += 1
                        continue

                    print("\nPropositions (film) :")
                    for i, s in enumerate(suggestions, start=1):
                        sim = float(s.get("sim") or 0.0)
                        print(f"  {i}) film_id={int(s['film_id'])} — {s['title']} ({s.get('year')}) [score={sim:.2f}]")

                    choice = prompt_choice()
                    if choice == "q":
                        print("[INFO] Quit demandé.")
                        break
                    if choice == "s" or choice == "":
                        skipped += 1
                        continue

                    if choice.isdigit():
                        k = int(choice)
                        if 1 <= k <= len(suggestions):
                            film_id = int(suggestions[k-1]["film_id"])
                            if not args.dry_run:
                                cur.execute(SQL_UPSERT_EXTERNAL_ID, {
                                    "film_id": film_id,
                                    "provider": args.provider,
                                    "external_key": sc_id,
                                    "external_url": film_url
                                })
                            saved += 1
                            print(f"[OK] Enregistré: ({args.provider}, {sc_id}) -> film_id={film_id}")
                        else:
                            print("[WARN] Choix hors plage -> skip")
                            skipped += 1
                    else:
                        print("[WARN] Choix invalide -> skip")
                        skipped += 1

        print("\n[DONE]")
        print(f"saved={saved}")
        print(f"skipped={skipped}")
        print(f"already_mapped={already}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
