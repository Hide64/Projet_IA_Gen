# -*- coding: utf-8 -*-
import os
import re
import argparse
from datetime import datetime
from typing import Optional, Tuple, List

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor


# -----------------------------
# Parsing helpers
# -----------------------------
RE_REWATCH = re.compile(r"(?i)\brevu\s+le\s+(\d{1,2})/(\d{1,2})/(\d{4})")
RE_SC_ID = re.compile(r"/(\d+)(?:\?|$)")


def extract_sc_id(url: str) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None
    m = RE_SC_ID.search(url.strip())
    return m.group(1) if m else None


def parse_annotation(annotation: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (rewatch_iso_date 'YYYY-MM-DD' or None, cleaned_comment or None)
    - Extracts 'Revu le dd/mm/yyyy'
    - Removes that marker from the comment text
    """
    if annotation is None:
        return None, None
    txt = str(annotation).strip()
    if not txt:
        return None, None

    rewatch_iso = None
    m = RE_REWATCH.search(txt)
    if m:
        dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
        rewatch_iso = f"{int(yyyy):04d}-{int(mm):02d}-{int(dd):02d}"
        # Remove the matched part and leading punctuation/newlines
        start, end = m.span()
        txt = (txt[:start] + txt[end:]).strip()
        txt = re.sub(r"^[\s\.\-\:\n\r]+", "", txt).strip()

    # Clean doubled quotes from CSV exports
    txt = txt.replace('""', '"').strip()
    if not txt:
        txt = None

    return rewatch_iso, txt


# -----------------------------
# DB + Ollama helpers
# -----------------------------
def get_conn(host: str, port: int, db: str, user: str, pwd: str):
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=pwd,
        options="-c client_encoding=UTF8",
    )


def ollama_embed(ollama_url: str, model: str, text: str, timeout: int = 120) -> List[float]:
    r = requests.post(
        f"{ollama_url}/api/embeddings",
        json={"model": model, "prompt": text},
        timeout=timeout,
    )
    r.raise_for_status()
    data = r.json()
    emb = data.get("embedding")
    if not emb or not isinstance(emb, list):
        raise RuntimeError(f"Embedding invalide: {data}")
    return emb


# -----------------------------
# SQL (assumes you created film_external_id)
# -----------------------------
SQL_GET_FILM_ID = """
SELECT film_id
FROM film_external_id
WHERE provider = %(provider)s
  AND external_key = %(external_key)s
LIMIT 1;
"""

SQL_INSERT_WATCH_EVENT = """
INSERT INTO watch_event (user_id, film_id, watched_at, context, notes)
VALUES (%(user_id)s, %(film_id)s, %(watched_at)s::timestamp, %(context)s, %(notes)s);
"""

# robust upsert: ensure row exists + update last_seen_at + rewatch_count
SQL_UPSERT_USER_FILM_REWATCH = """
INSERT INTO user_film (user_id, film_id, status, last_seen_at, rewatch_count)
VALUES (%(user_id)s, %(film_id)s, 'SEEN', %(seen_at)s::date, 1)
ON CONFLICT (user_id, film_id)
DO UPDATE SET
  last_seen_at = GREATEST(user_film.last_seen_at, EXCLUDED.last_seen_at),
  rewatch_count = COALESCE(user_film.rewatch_count, 0) + 1;
"""

# user_comment table (we'll create if missing)
SQL_CREATE_USER_COMMENT = """
CREATE TABLE IF NOT EXISTS user_comment (
  comment_id      bigserial PRIMARY KEY,
  user_id         bigint NOT NULL REFERENCES app_user(user_id) ON DELETE CASCADE,
  film_id         bigint NOT NULL REFERENCES film(film_id) ON DELETE CASCADE,
  source          text   NOT NULL DEFAULT 'senscritique',
  comment_text    text   NOT NULL,
  raw_annotation  text,
  created_at      timestamp without time zone DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_user_comment_user ON user_comment(user_id);
CREATE INDEX IF NOT EXISTS idx_user_comment_film ON user_comment(film_id);
"""

SQL_INSERT_COMMENT = """
INSERT INTO user_comment (user_id, film_id, source, comment_text, raw_annotation)
VALUES (%(user_id)s, %(film_id)s, %(source)s, %(comment_text)s, %(raw_annotation)s)
RETURNING comment_id;
"""

# pgvector extension + embedding table (vector(dim))
# We'll create with dim inferred at runtime the first time if needed.
SQL_CREATE_VECTOR_EXT = "CREATE EXTENSION IF NOT EXISTS vector;"

def sql_create_comment_embedding(dim: int) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS comment_embedding (
  comment_id bigserial PRIMARY KEY REFERENCES user_comment(comment_id) ON DELETE CASCADE,
  model      text NOT NULL,
  embedding  vector({dim}) NOT NULL,
  created_at timestamp without time zone DEFAULT now()
);
-- index approximate cosine
CREATE INDEX IF NOT EXISTS idx_comment_embedding_ivfflat
ON comment_embedding USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
"""


SQL_INSERT_EMBEDDING = """
INSERT INTO comment_embedding (comment_id, model, embedding)
VALUES (%(comment_id)s, %(model)s, %(embedding)s);
"""


# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Import SC annotations: rewatch + comments + pgvector embeddings (local run).")
    parser.add_argument("--csv", required=True, help="Chemin vers le CSV SensCritique")
    parser.add_argument("--user-id", type=int, default=1, help="user_id cible")
    parser.add_argument("--provider", default="senscritique", help="provider clé dans film_external_id")
    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "localhost"))
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PG_PORT", "5432")))
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "videotheque"))
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--pg-password", default=os.getenv("PG_PASSWORD", "postgres"))
    parser.add_argument("--ollama-url", default=os.getenv("OLLAMA_URL", "http://localhost:11434"))
    parser.add_argument("--embed-model", default=os.getenv("EMBED_MODEL", "nomic-embed-text"))
    parser.add_argument("--dry-run", action="store_true", help="Ne rien écrire en base (debug)")
    parser.add_argument("--limit", type=int, default=0, help="Limiter le nombre de lignes traitées (0=all)")
    args = parser.parse_args()

    csv_path = args.csv
    user_id = args.user_id

    print(f"[INFO] CSV: {csv_path}")
    df = pd.read_csv(csv_path)

    required = {"title", "film_url", "annotation"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"[ERROR] Colonnes manquantes: {missing}. Colonnes={list(df.columns)}")

    df["annotation"] = df["annotation"].fillna("").astype(str)
    df = df[df["annotation"].str.strip() != ""].copy()  # ✅ seulement annotation non vide
    df["sc_id"] = df["film_url"].apply(extract_sc_id)
    df = df[df["sc_id"].notna()].copy()

    if args.limit and args.limit > 0:
        df = df.head(args.limit)

    if df.empty:
        print("[INFO] Rien à traiter (annotation vide ou sc_id absent).")
        return

    print(f"[INFO] Lignes à traiter: {len(df)}")

    conn = get_conn(args.pg_host, args.pg_port, args.pg_db, args.pg_user, args.pg_password)
    conn.autocommit = False

    created_embedding_table = False
    embedding_dim = None

    imported = 0
    comments_inserted = 0
    embeddings_inserted = 0
    rewatch_events = 0
    not_found = 0
    skipped = 0

    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Ensure tables/ext exist
                cur.execute(SQL_CREATE_VECTOR_EXT)
                cur.execute(SQL_CREATE_USER_COMMENT)

                for row in df.itertuples(index=False):
                    sc_id = str(getattr(row, "sc_id")).strip()
                    film_url = str(getattr(row, "film_url")).strip()
                    annotation = str(getattr(row, "annotation")).strip()

                    # Find film_id via film_external_id
                    cur.execute(SQL_GET_FILM_ID, {"provider": args.provider, "external_key": sc_id})
                    r = cur.fetchone()
                    if not r:
                        not_found += 1
                        continue
                    film_id = int(r["film_id"])

                    rewatch_iso, comment_text = parse_annotation(annotation)

                    # 1) Rewatch -> watch_event + user_film aggregate
                    if rewatch_iso:
                        if not args.dry_run:
                            cur.execute(SQL_INSERT_WATCH_EVENT, {
                                "user_id": user_id,
                                "film_id": film_id,
                                "watched_at": f"{rewatch_iso} 00:00:00",
                                "context": "rewatch (senscritique)",
                                "notes": "parsed from annotation"
                            })
                            cur.execute(SQL_UPSERT_USER_FILM_REWATCH, {
                                "user_id": user_id,
                                "film_id": film_id,
                                "seen_at": rewatch_iso
                            })
                        rewatch_events += 1

                    # 2) Comment -> user_comment + embedding
                    if comment_text:
                        if not args.dry_run:
                            cur.execute(SQL_INSERT_COMMENT, {
                                "user_id": user_id,
                                "film_id": film_id,
                                "source": args.provider,
                                "comment_text": comment_text,
                                "raw_annotation": annotation
                            })
                            comment_id = int(cur.fetchone()["comment_id"])
                        else:
                            comment_id = -1

                        comments_inserted += 1

                        # embedding
                        try:
                            emb = ollama_embed(args.ollama_url, args.embed_model, comment_text, timeout=120)

                            if embedding_dim is None:
                                embedding_dim = len(emb)
                                # Create embedding table with right dim (once)
                                if not args.dry_run:
                                    cur.execute(sql_create_comment_embedding(embedding_dim))
                                created_embedding_table = True

                            # Insert embedding as pgvector literal: '[0.1,0.2,...]'
                            emb_str = "[" + ",".join(f"{float(x):.8f}" for x in emb) + "]"

                            if not args.dry_run:
                                cur.execute(SQL_INSERT_EMBEDDING, {
                                    "comment_id": comment_id,
                                    "model": args.embed_model,
                                    "embedding": emb_str
                                })
                            embeddings_inserted += 1
                        except Exception as e:
                            print(f"[WARN] embedding failed sc_id={sc_id} film_id={film_id}: {e}")

                    imported += 1

                    if imported % 50 == 0:
                        print(f"[INFO] progress imported={imported} comments={comments_inserted} emb={embeddings_inserted} rewatch={rewatch_events} not_found={not_found}")

                # Improve ivfflat stats
                if not args.dry_run and created_embedding_table:
                    cur.execute("ANALYZE comment_embedding;")

        print("[DONE]")
        print(f"imported_rows={imported}")
        print(f"rewatch_events={rewatch_events}")
        print(f"comments_inserted={comments_inserted}")
        print(f"embeddings_inserted={embeddings_inserted}")
        print(f"film_not_found={not_found}")
        print(f"skipped={skipped}")
        if embedding_dim:
            print(f"embedding_dim={embedding_dim} model={args.embed_model}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
