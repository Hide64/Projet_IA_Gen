CREATE TYPE film_status AS ENUM (
    'WANT',
    'SEEN',
    'SKIPPED',
    'ABANDONED'
);

CREATE TYPE source_code AS ENUM (
    'NAS',
    'BR',
    'DVD',
    'STREAM'
);

CREATE TYPE physical_format AS ENUM (
    'DVD',
    'BLURAY',
    'UHD'
);

CREATE TABLE film (
    film_id            BIGSERIAL PRIMARY KEY,
    tmdb_id            INTEGER UNIQUE,
    imdb_id            TEXT UNIQUE,
    title              TEXT NOT NULL,
    original_title     TEXT,
    release_date       DATE,
    year               INTEGER,
    runtime_min        INTEGER,
    overview           TEXT,
    original_language  CHAR(2),
    poster_path        TEXT,
    backdrop_path      TEXT,
    tmdb_popularity    REAL,
    tmdb_vote_avg      REAL,
    tmdb_vote_count    INTEGER,
    created_at         TIMESTAMP DEFAULT now(),
    updated_at         TIMESTAMP DEFAULT now()
);

CREATE TABLE genre (
    genre_id       SERIAL PRIMARY KEY,
    tmdb_genre_id  INTEGER UNIQUE NOT NULL,
    name           TEXT NOT NULL
);

CREATE TABLE film_genre (
    film_id   BIGINT REFERENCES film(film_id) ON DELETE CASCADE,
    genre_id  INTEGER REFERENCES genre(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (film_id, genre_id)
);

CREATE TABLE person (
    person_id         BIGSERIAL PRIMARY KEY,
    tmdb_person_id    INTEGER UNIQUE,
    name              TEXT NOT NULL,
    gender            INTEGER,
    profile_path      TEXT
);

CREATE TABLE film_credit (
    film_credit_id    BIGSERIAL PRIMARY KEY,
    film_id           BIGINT REFERENCES film(film_id) ON DELETE CASCADE,
    person_id         BIGINT REFERENCES person(person_id) ON DELETE CASCADE,
    department        TEXT NOT NULL, -- Acting, Directing, Writing…
    job               TEXT,
    character_name    TEXT,
    billing_order     INTEGER
);

CREATE TABLE source (
    source_id   SERIAL PRIMARY KEY,
    code        source_code UNIQUE NOT NULL,
    label       TEXT NOT NULL
);

CREATE TABLE film_source (
    film_id       BIGINT REFERENCES film(film_id) ON DELETE CASCADE,
    source_id     INTEGER REFERENCES source(source_id) ON DELETE CASCADE,
    is_available  BOOLEAN DEFAULT TRUE,
    added_at      TIMESTAMP DEFAULT now(),
    notes         TEXT,
    PRIMARY KEY (film_id, source_id)
);

CREATE TABLE nas_asset (
    nas_asset_id     BIGSERIAL PRIMARY KEY,
    film_id          BIGINT REFERENCES film(film_id) ON DELETE CASCADE,
    path             TEXT NOT NULL,
    container        TEXT,
    video_codec      TEXT,
    audio_codec      TEXT,
    resolution       INTEGER,
    hdr              BOOLEAN,
    audio_langs      JSONB,
    sub_langs        JSONB,
    file_size_mb     INTEGER,
    hash             TEXT,
    scanned_at       TIMESTAMP DEFAULT now(),
    UNIQUE (path)
);

CREATE TABLE physical_copy (
    physical_copy_id BIGSERIAL PRIMARY KEY,
    film_id          BIGINT REFERENCES film(film_id) ON DELETE CASCADE,
    format           physical_format NOT NULL,
    edition          TEXT,
    region_code      TEXT,
    purchase_date    DATE,
    condition        TEXT,
    notes            TEXT
);

CREATE TABLE app_user (
    user_id       BIGSERIAL PRIMARY KEY,
    display_name  TEXT NOT NULL
);

CREATE TABLE user_film (
    user_id         BIGINT REFERENCES app_user(user_id) ON DELETE CASCADE,
    film_id         BIGINT REFERENCES film(film_id) ON DELETE CASCADE,
    status          film_status NOT NULL,
    rating_10       NUMERIC(3,1),
    liked           BOOLEAN,
    first_seen_at   DATE,
    last_seen_at    DATE,
    rewatch_count   INTEGER DEFAULT 0,
    personal_notes  TEXT,
    updated_at      TIMESTAMP DEFAULT now(),
    PRIMARY KEY (user_id, film_id)
);

CREATE TABLE watch_event (
    watch_event_id  BIGSERIAL PRIMARY KEY,
    user_id         BIGINT REFERENCES app_user(user_id) ON DELETE CASCADE,
    film_id         BIGINT REFERENCES film(film_id) ON DELETE CASCADE,
    watched_at      TIMESTAMP NOT NULL,
    context         TEXT,
    rating_10       NUMERIC(3,1),
    notes           TEXT
);

CREATE TABLE tag (
    tag_id   SERIAL PRIMARY KEY,
    name     TEXT UNIQUE NOT NULL
);

CREATE TABLE user_film_tag (
    user_id  BIGINT REFERENCES app_user(user_id) ON DELETE CASCADE,
    film_id  BIGINT REFERENCES film(film_id) ON DELETE CASCADE,
    tag_id   INTEGER REFERENCES tag(tag_id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, film_id, tag_id)
);

CREATE TABLE list (
    list_id     BIGSERIAL PRIMARY KEY,
    user_id     BIGINT REFERENCES app_user(user_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    is_ranked   BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE list_item (
    list_id   BIGINT REFERENCES list(list_id) ON DELETE CASCADE,
    film_id   BIGINT REFERENCES film(film_id) ON DELETE CASCADE,
    rank      INTEGER,
    comment   TEXT,
    added_at  TIMESTAMP DEFAULT now(),
    PRIMARY KEY (list_id, film_id)
);

CREATE INDEX idx_film_tmdb ON film(tmdb_id);
CREATE INDEX idx_film_title ON film USING gin (title gin_trgm_ops);
CREATE INDEX idx_user_film_status ON user_film(status);
CREATE INDEX idx_watch_event_user ON watch_event(user_id);

CREATE EXTENSION IF NOT EXISTS pg_trgm; 
