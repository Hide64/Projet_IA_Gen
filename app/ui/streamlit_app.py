# -*- coding: utf-8 -*-
import os
import requests
import streamlit as st
import psycopg2
import pandas as pd
import numpy as np
import json
from psycopg2.extras import RealDictCursor

# ===============================
# CONFIGURATION
# ===============================
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_DB   = os.getenv("POSTGRES_DB", "videotheque")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PWD  = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

DEFAULT_USER_ID = int(os.getenv("APP_USER_ID", "1"))
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")

# ===============================
# SQL : FILTRAGE STRICT ET PROFIL
# ===============================

SQL_GENRE_PROFILE = """
WITH rated_seen AS (
  SELECT film_id, rating_10 FROM user_film
  WHERE user_id = %(user_id)s AND status = 'SEEN' AND rating_10 IS NOT NULL
)
SELECT fg.genre_id, AVG(rs.rating_10) AS avg_rating
FROM rated_seen rs JOIN film_genre fg ON fg.film_id = rs.film_id
GROUP BY fg.genre_id;
"""

SQL_HYBRID_SEARCH = """
WITH semantic_search AS (
    SELECT film_id, (1.0 - (embedding <=> %(qvec)s::vector)) AS similarity
    FROM film_embedding
    ORDER BY similarity DESC LIMIT 150
)
SELECT DISTINCT f.film_id, f.title, f.year, f.runtime_min, f.overview, s.similarity
FROM film f
JOIN semantic_search s ON f.film_id = s.film_id
JOIN film_genre fg ON f.film_id = fg.film_id
JOIN genre g ON fg.genre_id = g.genre_id
LEFT JOIN user_film uf ON uf.film_id = f.film_id AND uf.user_id = %(user_id)s
WHERE (uf.status IS NULL OR uf.status != 'SEEN')
  AND (uf.last_seen_at IS NULL OR uf.last_seen_at < NOW() - INTERVAL '6 months')
  AND f.title ~ '^[\\x00-\\x7F]+$' 
  AND g.name = ANY(%(genres)s)
"""

# ===============================
# UTILS & OLLAMA
# ===============================

def get_conn():
    return psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PWD, port=PG_PORT)

def fetch_df(conn, sql, params=None):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params or {})
        return pd.DataFrame(cur.fetchall())

def ollama_embed(text: str):
    r = requests.post(f"{OLLAMA_URL}/api/embeddings", json={"model": EMBED_MODEL, "prompt": text}, timeout=90)
    return r.json().get("embedding")

def extract_intent(user_text, available_genres):
    # Prompt avec instruction de secours
    prompt = f"""
    Analyse : "{user_text}"
    Extraire en JSON uniquement :
    {{
      "genres": [], 
      "max_duration": null, 
      "style": "ambiance en 3 mots"
    }}
    Genres autorisés : {available_genres}
    """
    # Valeurs par défaut au cas où le JSON est incomplet
    default_intent = {"genres": [], "max_duration": None, "style": user_text}
    
    try:
        r = requests.post(f"{OLLAMA_URL}/api/chat", json={
            "model": OLLAMA_MODEL, "messages": [{"role": "user", "content": prompt}], 
            "stream": False, "format": "json"
        }, timeout=30)
        
        extracted = json.loads(r.json()["message"]["content"])
        # On fusionne avec les défauts pour garantir la présence des clés
        default_intent.update(extracted)
        return default_intent
    except:
        return default_intent

def generate_narrative(title, year, overview, query):
    prompt = f"""
    Explique pourquoi recommander '{title}' ({year}) pour la demande "{query}".
    Synopsis : "{overview}"
    CONSIGNES :
    1. Une phrase sur pourquoi ça matche l'ambiance.
    2. Une phrase résumant l'intrigue (sans spoiler).
    3. Pas de mention du réalisateur.
    4. Ton chaleureux, 3 phrases max.
    """
    try:
        r = requests.post(f"{OLLAMA_URL}/api/chat", json={
            "model": OLLAMA_MODEL, "messages": [{"role": "user", "content": prompt}], "stream": False
        }, timeout=30)
        return r.json()["message"]["content"].strip()
    except:
        return "Ce film correspond à vos critères et propose une intrigue captivante."

# ===============================
# INTERFACE STREAMLIT
# ===============================
st.set_page_config(page_title="Ciné-Assistant", layout="centered")
st.title("🎬 Votre Assistant Vidéothèque Personnel")

@st.cache_data
def load_base_data():
    conn = get_conn()
    g_df = fetch_df(conn, "SELECT name FROM genre")
    fg_df = fetch_df(conn, "SELECT film_id, genre_id FROM film_genre")
    profile = fetch_df(conn, SQL_GENRE_PROFILE, {"user_id": DEFAULT_USER_ID})
    conn.close()
    
    return {
        "genre_names": [r['name'] for r in g_df.to_dict('records')],
        "film_genres": fg_df.groupby("film_id")["genre_id"].apply(list).to_dict(),
        "profile": {int(r['genre_id']): float(r['avg_rating']) for r in profile.to_dict('records')}
    }

data = load_base_data()

if prompt := st.chat_input("Ex: Une comédie légère et courte..."):
    with st.chat_message("user"): st.write(prompt)

    with st.spinner("Recherche dans votre collection..."):
        # 1. Extraction d'intention
        intent = extract_intent(prompt, data["genre_names"])
        style_query = intent.get('style') or prompt # Fallback sur le texte original si vide
        qvec = ollama_embed(style_query)
        
        # 2. SQL avec filtrage strict (Genre)
        genres_to_filter = intent['genres'] if intent['genres'] else data["genre_names"]
        conn = get_conn()
        results = fetch_df(conn, SQL_HYBRID_SEARCH, {
            "user_id": DEFAULT_USER_ID, 
            "qvec": "[" + ",".join(map(str, qvec)) + "]",
            "genres": genres_to_filter
        })
        conn.close()

        # 3. Scoring Hybride et protection KeyError
        rows = []
        top_recos = pd.DataFrame() # Initialisation vide par sécurité

        if not results.empty:
            # Filtre additionnel Durée
            if intent['max_duration']:
                results = results[results['runtime_min'].fillna(999) <= intent['max_duration']]
            
            for r in results.itertuples():
                # Score Genre (Profil historique) - 70%
                g_ids = data["film_genres"].get(r.film_id, [])
                s_gen = np.mean([data["profile"].get(gid, 5.0) for gid in g_ids]) if g_ids else 5.0
                
                # Score Sémantique (0 à 10) - 30%
                s_sem = float(r.similarity) * 10.0
                
                final_score = (0.7 * s_gen) + (0.3 * s_sem)
                
                rows.append({
                    "title": r.title, "year": r.year, "runtime": r.runtime_min, 
                    "overview": r.overview, "score": final_score
                })

            if rows:
                top_recos = pd.DataFrame(rows).sort_values("score", ascending=False).head(3)

    # 4. Affichage des résultats
    with st.chat_message("assistant"):
        if top_recos.empty:
            st.warning("⚠️ Aucun film ne correspond (essayez d'enlever des filtres de durée ou de genre).")
        else:
            for r in top_recos.itertuples():
                with st.container():
                    st.subheader(f"{r.title} ({int(r.year)})")
                    st.caption(f"⏱️ {int(r.runtime) if pd.notnull(r.runtime) else '??'} min | ⭐ Score : {r.score:.1f}/10")
                    
                    with st.spinner(f"Réflexion sur {r.title}..."):
                        desc = generate_narrative(r.title, r.year, r.overview, prompt)
                    st.write(desc)
                    st.divider()