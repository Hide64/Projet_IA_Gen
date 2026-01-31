import os
import argparse
import pandas as pd
import psycopg2

from dotenv import load_dotenv
load_dotenv()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to NAS_export_videotheque_*.csv")
    ap.add_argument("--host", default=os.getenv("POSTGRES_HOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432")))
    ap.add_argument("--db", default=os.getenv("POSTGRES_DB", "criminalite"))
    ap.add_argument("--user", default=os.getenv("POSTGRES_USER", "postgres"))
    ap.add_argument("--password", default=os.getenv("POSTGRES_PASSWORD", "postgres"))
    ap.add_argument("--truncate", action="store_true", help="Empty import_nas before import")
    args = ap.parse_args()

    # Lecture CSV NAS (ton export est en ; et souvent en utf-8-sig)
    df = pd.read_csv(args.csv, sep=";", encoding="utf-8-sig")

    # Colonnes attendues dans ton export
    # title;year;director;language;actors;synopsis;poster_url;file;file_path;date_added
    for col in ["title", "year", "director", "language", "actors", "synopsis", "poster_url", "file", "file_path", "date_added"]:
        if col not in df.columns:
            raise ValueError(f"Colonne manquante dans le CSV: {col}. Colonnes trouvées: {list(df.columns)}")

    # Normalisation
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce")

    def norm_lang(x):
        if isinstance(x, str):
            x = x.strip()
            return x[:2].lower() if len(x) >= 2 else None
        return None

    rows = []
    for r in df.itertuples(index=False):
        rows.append((
            r.title,
            int(r.year) if pd.notna(r.year) else None,
            r.director if isinstance(r.director, str) else None,
            norm_lang(r.language),
            r.actors if isinstance(r.actors, str) else None,
            r.synopsis if isinstance(r.synopsis, str) else None,
            r.poster_url if isinstance(r.poster_url, str) else None,
            r.file if isinstance(r.file, str) else None,
            r.file_path if isinstance(r.file_path, str) else None,
            r.date_added.to_pydatetime() if pd.notna(r.date_added) else None
        ))

    conn = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.db, user=args.user, password=args.password
    )

    with conn, conn.cursor() as cur:
        if args.truncate:
            cur.execute("TRUNCATE TABLE import_nas;")

        cur.executemany("""
            INSERT INTO import_nas (
                raw_title, raw_year, raw_director, raw_language, raw_actors,
                raw_synopsis, raw_poster_url, raw_file, raw_file_path, date_added
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)

    conn.commit()
    conn.close()
    print(f"OK - import_nas rempli: {len(rows)} lignes")

if __name__ == "__main__":
    main()
