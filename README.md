### 🎬 Movie Mood Recommender
Application de recommandation de films basée sur :
- les goûts utilisateurs,
- l’historique de visionnage,
- des métadonnées cinéma,
- des embeddings vectoriels (PostgreSQL + pgvector).

→ L’application est fournie sous forme Dockerisée et entièrement reproductible.

### 🧱 Architecture technique
| Layer | Solution |
| ------ | ------ |
| Frontend / App | Streamlit (Python) |
| Base de données | PostgreSQL 16 + pgvector |
| LLM local | Ollama |
| Orchestration | Docker Compose |

### 📁 Structure du projet

<img width="431" height="407" alt="image" src="https://github.com/user-attachments/assets/39c34bab-ebfb-4a9e-8642-1ad16eb554fe" />

### ⚙️ Pré-requis
1. Docker Desktop (ou Docker + Docker Compose)
2. Aucune dépendance Python locale requise

### 🚀 Démarrage rapide
#### 1️⃣ Configuration de l’environnement
Bien qu'une grande partie des valeurs par défaut aient été conservées, merci de coller le fichier .env communiqué parrallèlement et contenant les variables d'environnement dans le repository.
 
#### 2️⃣ Reconstruction automatique de la base PostgreSQL (premier lancement)
📌 La base de données n’est pas versionnée dans le repo.
Elle est reconstruite automatiquement à partir d’une sauvegarde PostgreSQL fournie séparément.

Étapes :
1. Télécharger la sauvegarde videotheque.dump (lien fourni séparément)
2. Copier le fichier dans le dossier suivant :
3. postgres/seed/videotheque.dump

#### 3️⃣ Lancer l’application
```sh
docker compose up -d --build
```

➡️ Au premier lancement uniquement (si la base est absente) :
1. PostgreSQL initialise la base
2. les extensions nécessaires sont créées (pgvector)
3. la sauvegarde est automatiquement restaurée

#### 4️⃣ Accéder à l’application
Interface Streamlit :
```sh
👉 http://localhost:8501
```
🔁 Réinitialisation complète (si nécessaire)
Pour supprimer la base et relancer l’import depuis la sauvegarde :
```s
docker compose down -v
docker compose up -d --build
```

### 🗄️ Détails sur la base de données
- **Nom de la base** : videotheque
- **Moteur** : PostgreSQL 16
- **Extension vectorielle** : pgvector
La restauration est conditionnelle : elle ne s’exécute que si le volume PostgreSQL est vide. Aucun écrasement en cas de redémarrage classique

### 🔐 Sécurité & bonnes pratiques

Le fichier .env n’est pas versionné. Mais livré via un autre moyen.
Un fichier .env.example lui est fourni.
Les sauvegardes (.dump, .sql) ne sont pas incluses dans Git.
