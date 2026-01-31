# scripts/import_seen_sc.py
import os
import re
import argparse
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DB = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "videotheque"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
)

def detect_sep_and_encoding(csv_path: str):
    # detect delimiter from first bytes; handle BOM
    with open(csv_path, "rb") as f:
        raw = f.read(4096)
    sample = raw.decode("utf-8-sig", errors="replace")

    # rough delimiter detection
    counts = {
        "\t": sample.count("\t"),
        ";": sample.count(";"),
        ",": sample.count(","),
    }
    sep = max(counts, key=counts.get) if max(counts.values()) > 0 else ","
    return sep, "utf-8-sig"

def norm_col(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = s.replace("-", "_")
    return s

def clean_title(title: str) -> str:
    t = (title or "").strip()
    # remove bracket blocks like [BR], [4K + DVD], etc.
    t = re.sub(r"\[[^\]]*\]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def parse_year(x):
    if x is None or pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    m = re.search(r"\b(18|19|20)\d{2}\b", s)
    return int(m.group(0)) if m else None

def parse_rating_10(x):
    if x is None or pd.isna(x):
        return None
    s = str(x).strip().replace(",", ".")
    if not s:
        return None
    try:
        v = float(s)
        return v
    except Exception:
        return None

def parse_date(x):
    if x is None or pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    d = pd.to_datetime(s, dayfirst=True, errors="coerce")
    return None if pd.isna(d) else d.date()

def pick_col(df_cols_norm, *candidates):
    """Return actual df column name that matches any normalized candidate."""
    for cand in candidates:
        c = norm_col(cand)
        if c in df_cols_norm:
            return df_cols_norm[c]
    return None

def get_table_columns(cur, table_name: str):
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        ORDER BY ordinal_position;
    """, (table_name,))
    return [r[0] for r in cur.fetchall()]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Chemin du CSV SensCritique (films vus)")
    ap.add_argument("--truncate", action="store_true", help="Vide import_seen_sc avant import")
    ap.add_argument("--limit", type=int, default=None, help="Limiter le nombre de lignes importées")
    args = ap.parse_args()

    sep, enc = detect_sep_and_encoding(args.csv)
    df = pd.read_csv(args.csv, sep=sep, encoding=enc, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]

    # map normalized -> actual
    df_cols_norm = {norm_col(c): c for c in df.columns}

    # Guess SensCritique columns (robust to variations)
    title_col = pick_col(df_cols_norm, "title", "titre")
    year_col = pick_col(df_cols_norm, "year", "annee", "année")
    director_col = pick_col(df_cols_norm, "directors", "director", "realisateur", "réalisateur")
    rating_col = pick_col(df_cols_norm, "rating10", "rating_10", "note10", "note_10", "rating", "note", "score")
    seen_date_col = pick_col(df_cols_norm, "watcheddate", "watched_date", "watchedat", "watched_at", "date", "seen_date", "vu_le")
    notes_col = pick_col(df_cols_norm, "notes", "comment", "commentaire", "review", "critique")  # (sera None ici)


    print(f"[import_seen_sc] Detected separator: {repr(sep)} | columns={list(df.columns)[:12]} ...")
    print("[import_seen_sc] Mapped columns:",
          {"title": title_col, "year": year_col, "rating": rating_col, "notes": notes_col, "seen_date": seen_date_col})

    if not title_col:
        raise SystemExit("Impossible de trouver une colonne titre. Renomme une colonne en 'title'/'titre' ou adapte le mapping.")

    if args.limit:
        df = df.head(args.limit)

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    with conn, conn.cursor() as cur:
        table = "import_seen_sc"
        table_cols = get_table_columns(cur, table)
        table_colset = set(table_cols)

        if args.truncate:
            cur.execute(f"TRUNCATE TABLE {table};")

        # Build row dicts with common staging fields
        rows = []
        for _, r in df.iterrows():
            raw_title = (r.get(title_col) or "").strip()
            if not raw_title:
                continue

            row = {
                "raw_title": raw_title,
                "raw_year": parse_year(r.get(year_col)) if year_col else None,
                "raw_directors": (r.get(director_col) or "").strip() if director_col else None,
                "rating_10": parse_rating_10(r.get(rating_col)) if rating_col else None,
                "raw_notes": (r.get(notes_col) or "").strip() if notes_col else None,   # sera vide pour ce CSV
                "watched_date": parse_date(r.get(seen_date_col)) if seen_date_col else None,
                "match_status": "PENDING",
                "match_note": None,
            }

            # Keep only columns that exist in your table
            filtered = {k: v for k, v in row.items() if k in table_colset}
            rows.append(filtered)

        if not rows:
            conn.rollback()
            print("Aucune ligne importable (titre vide ?).")
            return

        # Determine insert columns = union of keys (stable order by table definition)
        insert_cols = [c for c in table_cols if c in rows[0].keys() or any(c in rr for rr in rows)]
        # Ensure we don't insert columns that are entirely missing
        insert_cols = [c for c in insert_cols if any(c in rr for rr in rows)]

        values = []
        for rr in rows:
            values.append(tuple(rr.get(c) for c in insert_cols))

        sql = f"INSERT INTO {table} ({', '.join(insert_cols)}) VALUES %s;"
        execute_values(cur, sql, values, page_size=1000)
        conn.commit()

        print(f"OK - import_seen_sc rempli: {len(values)} lignes")

    conn.close()

if __name__ == "__main__":
    main()
