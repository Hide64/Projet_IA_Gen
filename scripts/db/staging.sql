INSERT INTO source(code, label) VALUES
('NAS','NAS'),
('BR','Blu-ray'),
('STREAM','Streaming'),
('DVD','DVD')
ON CONFLICT (code) DO NOTHING;

INSERT INTO app_user(display_name) VALUES ('Hide') RETURNING user_id;

CREATE TABLE IF NOT EXISTS import_nas (
  import_nas_id BIGSERIAL PRIMARY KEY,
  raw_title TEXT NOT NULL,
  raw_year INTEGER,
  raw_director TEXT,
  raw_language CHAR(2),
  raw_actors TEXT,
  raw_synopsis TEXT,
  raw_poster_url TEXT,
  raw_file TEXT,
  raw_file_path TEXT,
  date_added TIMESTAMP,
  tmdb_id INTEGER,
  match_status TEXT DEFAULT 'PENDING',
  match_note TEXT,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS import_br (
  import_br_id BIGSERIAL PRIMARY KEY,
  raw_title TEXT NOT NULL,
  raw_year INTEGER,
  raw_director TEXT,
  raw_format TEXT,
  raw_notes TEXT,
  tmdb_id INTEGER,
  match_status TEXT DEFAULT 'PENDING',
  match_note TEXT,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS import_seen_sc (
  import_seen_id BIGSERIAL PRIMARY KEY,
  raw_title TEXT NOT NULL,
  raw_year INTEGER,
  raw_directors TEXT,
  rating_10 NUMERIC(3,1),
  watched_date DATE,
  tmdb_id INTEGER,
  match_status TEXT DEFAULT 'PENDING',
  match_note TEXT,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS import_watchlist_sc (
  import_id BIGSERIAL PRIMARY KEY,
  raw_title TEXT NOT NULL,
  raw_year INTEGER,
  raw_directors TEXT,
  tmdb_id INTEGER,
  match_status TEXT DEFAULT 'PENDING',
  match_note TEXT,
  created_at TIMESTAMP DEFAULT now()
);