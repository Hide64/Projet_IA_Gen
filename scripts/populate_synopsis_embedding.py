import os
import psycopg2
import requests
import json
from psycopg2.extras import RealDictCursor

# --- CONFIGURATION ---
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_DB   = os.getenv("POSTGRES_DB", "videotheque")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PWD  = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
EMBED_MODEL = "nomic-embed-text"

def get_conn():
    return psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PWD, port=PG_PORT
    )

def ollama_embed(text):
    """Génère l'embedding d'un texte via Ollama."""
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/embeddings",
            json={"model": EMBED_MODEL, "prompt": text},
            timeout=60
        )
        r.raise_for_status()
        return r.json().get("embedding")
    except Exception as e:
        print(f"Erreur Ollama : {e}")
        return None

def to_pgvector_literal(vec):
    """Convertit une liste de float en format literal PGVector."""
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"

def main():
    print(f"🚀 Démarrage de l'indexation avec {EMBED_MODEL}...")
    conn = get_conn()
    
    # 1. Identifier les films sans embedding
    query_todo = """
        SELECT f.film_id, f.overview 
        FROM film f 
        LEFT JOIN film_embedding fe ON f.film_id = fe.film_id 
        WHERE f.overview IS NOT NULL 
          AND f.overview <> '' 
          AND fe.film_id IS NULL;
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query_todo)
        films_to_process = cur.fetchall()
    
    total = len(films_to_process)
    print(f"📈 {total} films à traiter.")

    if total == 0:
        print("✅ Tout est déjà à jour.")
        return

    # 2. Boucle de traitement
    count = 0
    for film in films_to_process:
        film_id = film['film_id']
        overview = film['overview']
        
        # Nettoyage rapide du texte (limite Ollama)
        clean_text = overview.replace("\x00", "").strip()
        
        embedding = ollama_embed(clean_text)
        
        if embedding:
            vec_lit = to_pgvector_literal(embedding)
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO film_embedding (film_id, embedding) VALUES (%s, %s::vector)",
                    (film_id, vec_lit)
                )
            conn.commit()
            count += 1
            if count % 10 == 0:
                print(f"🔄 Progress: {count}/{total} films indexés...")
        else:
            print(f"⚠️ Échec pour le film {film_id}")

    conn.close()
    print(f"✨ Terminé ! {count} embeddings ajoutés à la table film_embedding.")

if __name__ == "__main__":
    main()