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

def sniff_sep(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        head = f.read(4096)
    if head.count(";") > head.count(","):
        return ";"
    return ","

def norm_col(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def pick_col(cols_norm_map: dict, *cands):
    for c in cands:
        k = norm_col(c)
        if k in cols_norm_map:
            return cols_norm_map[k]
    return None

def parse_int(x):
    if pd.isna(x) or x is None:
        return None
    try:
        return int(str(x).strip())
    except Exception:
        return None

def clean_title(t: str) -> str:
    t = (t or "").strip()
    # enlève crochets (formats etc.) si jamais
    t = re.sub(r"\[[^\]]*\]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

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
    ap.add_argument("--csv", required=True)
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    sep = sniff_sep(args.csv)
    df = pd.read_csv(args.csv, sep=sep, dtype=str).fillna(pd.NA)

    print(f"[import_watchlist_sc] Detected separator: {sep!r} | columns={list(df.columns)[:12]} ...")

    cols_norm_map = {norm_col(c): c for c in df.columns}

    title_col = pick_col(cols_norm_map, "Title", "Titre", "raw_title")
    year_col = pick_col(cols_norm_map, "Year", "Annee", "Année", "raw_year")
    director_col = pick_col(cols_norm_map, "Directors", "Director", "Realisateur", "Réalisateur", "Creators", "creators")
    notes_col = pick_col(cols_norm_map, "Notes", "Comment", "Commentaire", "Review", "Critique", "Description")

    mapped = {"title": title_col, "year": year_col, "directors": director_col, "notes": notes_col}
    print(f"[import_watchlist_sc] Mapped columns: {mapped}")

    if not title_col:
        raise SystemExit("Could not map a title column from CSV.")

    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    with conn, conn.cursor() as cur:
        table = "import_watchlist_sc"
        table_cols = get_table_columns(cur, table)

        # colonnes candidates côté table (on ne remplit que si ça existe)
        col_raw_title = "raw_title" if "raw_title" in table_cols else None
        col_raw_year = "raw_year" if "raw_year" in table_cols else None
        col_raw_directors = "raw_directors" if "raw_directors" in table_cols else ("raw_director" if "raw_director" in table_cols else None)
        col_raw_notes = "raw_notes" if "raw_notes" in table_cols else ("notes" if "notes" in table_cols else None)
        col_match_status = "match_status" if "match_status" in table_cols else None
        col_match_note = "match_note" if "match_note" in table_cols else None
        col_tmdb_id = "tmdb_id" if "tmdb_id" in table_cols else None

        insert_cols = [c for c in [col_raw_title, col_raw_year, col_raw_directors, col_raw_notes, col_tmdb_id, col_match_status, col_match_note] if c]
        if not insert_cols:
            raise SystemExit("No compatible columns found in import_watchlist_sc.")

        if args.truncate:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY;")
            conn.commit()

        rows = []
        for _, r in df.iterrows():
            raw_title = clean_title(r.get(title_col))
            if not raw_title:
                continue
            raw_year = parse_int(r.get(year_col)) if year_col else None
            raw_directors = (r.get(director_col) or "").strip() if director_col else None
            raw_notes = (r.get(notes_col) or "").strip() if notes_col else None

            values = []
            for c in insert_cols:
                if c == col_raw_title:
                    values.append(raw_title)
                elif c == col_raw_year:
                    values.append(raw_year)
                elif c == col_raw_directors:
                    values.append(raw_directors if raw_directors else None)
                elif c == col_raw_notes:
                    values.append(raw_notes if raw_notes else None)
                elif c == col_tmdb_id:
                    values.append(None)
                elif c == col_match_status:
                    values.append("PENDING")
                elif c == col_match_note:
                    values.append(None)
                else:
                    values.append(None)
            rows.append(tuple(values))

        sql = f"INSERT INTO {table} ({', '.join(insert_cols)}) VALUES %s"
        execute_values(cur, sql, rows, page_size=1000)
        conn.commit()

        print(f"OK - import_watchlist_sc rempli: {len(rows)} lignes")

    conn.close()

if __name__ == "__main__":
    main()
