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

# --- parsing formats from brackets ------------------------------------------------

def extract_brackets(title: str) -> list[str]:
    """Return tokens found inside all [...] occurrences."""
    if not title:
        return []
    return re.findall(r"\[([^\]]+)\]", title)

def normalize_tokens(tokens: list[str]) -> list[str]:
    out = []
    for block in tokens:
        # split by + / , / / / &
        parts = re.split(r"[+,/&]| et ", block, flags=re.IGNORECASE)
        for p in parts:
            t = p.strip().upper()
            if not t:
                continue
            # common variants
            t = t.replace("BLU-RAY", "BR").replace("BLURAY", "BR")
            if t in ("4K", "UHD", "ULTRA HD", "ULTRAHD"):
                out.append("UHD")
            elif t in ("BR", "BD"):
                out.append("BR")
            elif t == "DVD":
                out.append("DVD")
    # uniq while preserving order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq

def clean_title(title: str) -> str:
    if not title:
        return title
    # remove trailing bracket groups like " [BR]" or " [BR + DVD]" etc. (one or more)
    t = re.sub(r"\s*(\[[^\]]+\]\s*)+$", "", title).strip()
    t = re.sub(r"\s+", " ", t).strip()
    return t

def as_text_ean(x):
    if pd.isna(x) or x == "":
        return None
    s = str(x).strip()
    if re.match(r"^\d+(\.\d+)?E\+\d+$", s, flags=re.IGNORECASE):
        try:
            return f"{int(float(s))}"
        except Exception:
            return s
    try:
        if re.match(r"^\d+(\.0+)?$", s):
            return str(int(float(s)))
    except Exception:
        pass
    return s

def parse_date(s):
    if s is None or pd.isna(s) or str(s).strip() == "":
        return None
    txt = str(s).strip()
    # Si format ISO YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", txt):
        d = pd.to_datetime(txt, format="%Y-%m-%d", errors="coerce")
    else:
        d = pd.to_datetime(txt, dayfirst=True, errors="coerce")
    return None if pd.isna(d) else d.date()

def parse_int(s):
    if s is None or pd.isna(s) or str(s).strip() == "":
        return None
    try:
        return int(float(s))
    except Exception:
        return None

def parse_num(s):
    if s is None or pd.isna(s) or str(s).strip() == "":
        return None
    try:
        return float(s)
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    # auto-detect delimiter + handle BOM
    with open(args.csv, "rb") as f:
        raw = f.read(4096)

    # default guess
    sep = "\t"
    sample = raw.decode("utf-8-sig", errors="replace")

    if sample.count(";") > sample.count("\t") and sample.count(";") > sample.count(","):
        sep = ";"
    elif sample.count(",") > sample.count("\t") and sample.count(",") > sample.count(";"):
        sep = ","

    df = pd.read_csv(args.csv, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    print(f"[import_br] Detected separator: {repr(sep)} | columns={list(df.columns)[:8]} ...")

    def col(name):
        return df[name] if name in df.columns else [""] * len(df)

    rows = []
    for i in range(len(df)):
        raw_title = col("title")[i]
        tokens = extract_brackets(raw_title)
        formats = normalize_tokens(tokens)
        title_clean = clean_title(raw_title)

        rows.append((
            col("item_type")[i] or None,
            raw_title,
            col("creators")[i] or None,
            col("first_name")[i] or None,
            col("last_name")[i] or None,
            as_text_ean(col("ean_isbn13")[i]),
            col("upc_isbn10")[i] or None,
            col("description")[i] or None,
            col("publisher")[i] or None,
            parse_date(col("publish_date")[i]),
            col("raw_group")[i] or None,
            col("tags")[i] or None,
            col("notes")[i] or None,
            parse_num(col("price")[i]),
            parse_int(col("length")[i]),
            parse_int(col("number_of_discs")[i]),
            parse_int(col("number_of_players")[i]),
            col("age_group")[i] or None,
            col("ensemble")[i] or None,
            col("aspect_ratio")[i] or None,
            col("esrb")[i] or None,
            col("rating")[i] or None,
            col("review")[i] or None,
            parse_date(col("review_date")[i]),
            col("status")[i] or None,
            parse_date(col("began")[i]),
            parse_date(col("completed")[i]),
            parse_date(col("added")[i]),
            parse_int(col("copies")[i]),
            title_clean,
            True,          # is_physical
            formats,       # formats text[]
        ))

    conn = psycopg2.connect(**DB)
    with conn, conn.cursor() as cur:
        if args.truncate:
            cur.execute("TRUNCATE TABLE import_br;")

        sql = """
        INSERT INTO import_br (
          item_type, raw_title, raw_creators, raw_first_name, raw_last_name,
          ean_isbn13, upc_isbn10, description, publisher, publish_date, raw_group,
          tags, notes, price, length_min, number_of_discs, number_of_players,
          age_group, ensemble, aspect_ratio, esrb, rating, review, review_date,
          status, began, completed, added, copies,
          raw_title_clean, is_physical, formats
        ) VALUES %s
        """
        execute_values(cur, sql, rows, page_size=500)

    conn.close()
    print(f"OK - import_br rempli: {len(rows)} lignes")

if __name__ == "__main__":
    main()
