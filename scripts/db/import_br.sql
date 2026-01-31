CREATE TABLE IF NOT EXISTS import_br (
  import_br_id       BIGSERIAL PRIMARY KEY,

  -- BR CSV fields (brut)
  item_type          TEXT,
  raw_title          TEXT NOT NULL,
  raw_creators       TEXT,
  raw_first_name     TEXT,
  raw_last_name      TEXT,
  ean_isbn13         TEXT,
  upc_isbn10         TEXT,
  description        TEXT,
  publisher          TEXT,
  publish_date       DATE,
  raw_group            TEXT,
  tags               TEXT,
  notes              TEXT,
  price              NUMERIC,
  length_min         INTEGER,
  number_of_discs    INTEGER,
  number_of_players  INTEGER,
  age_group          TEXT,
  ensemble           TEXT,       -- cast
  aspect_ratio       TEXT,
  esrb               TEXT,
  rating             TEXT,
  review             TEXT,
  review_date        DATE,
  status             TEXT,
  began              DATE,
  completed          DATE,
  added              DATE,
  copies             INTEGER,

  -- dérivés / métier
  raw_title_clean    TEXT,       -- titre nettoyé sans [BR] etc.
  is_physical        BOOLEAN NOT NULL DEFAULT TRUE,
  formats            TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],  -- ex: {UHD,BR} / {BR,DVD}
  split_group_key    TEXT,
  -- matching/apply
  tmdb_id            INTEGER,
  match_status       TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING/MATCHED/APPLIED/AMBIGUOUS/NOT_FOUND/ERROR/BOXSET
  match_note         TEXT,
  created_at         TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_import_br_status ON import_br(match_status);
CREATE INDEX IF NOT EXISTS idx_import_br_tmdb   ON import_br(tmdb_id);
