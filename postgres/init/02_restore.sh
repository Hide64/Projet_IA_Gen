#!/usr/bin/env bash
set -euo pipefail

DUMP_PATH="/seed/videotheque.dump"

echo "[init] Checking for dump at: ${DUMP_PATH}"

if [ -f "${DUMP_PATH}" ]; then
  echo "[init] Dump found. Restoring into database: ${POSTGRES_DB}"

  # Restauration (format custom)
  pg_restore \
    --no-owner \
    --role="${POSTGRES_USER}" \
    --dbname="${POSTGRES_DB}" \
    --clean --if-exists \
    "${DUMP_PATH}"

  echo "[init] Restore completed."
else
  echo "[init] No dump found. Skipping restore."
  echo "[init] Expected: postgres/seed/videotheque.dump (mounted to /seed/videotheque.dump)"
fi