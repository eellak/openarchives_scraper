#!/usr/bin/env bash
set -euo pipefail

# Post-EDM retry orchestrator
# - Waits for a given PID to finish (current full run)
# - Backs up the EDM metadata parquet safely with verification
# - Launches a resume+retry run with updated settings

ROOT="/mnt/data/openarchive_scraper"
OUT_PARQUET="$ROOT/openarchives_results/edm_metadata_full.parquet"
ALL_LINKS="$ROOT/openarchives_results/all_links.parquet"
LOG_DIR="$ROOT/logs"
BACKUP_DIR="$ROOT/openarchives_results/backups"

PID_TO_WAIT_FOR="${1:-}"
if [[ -z "${PID_TO_WAIT_FOR}" ]]; then
  echo "Usage: $0 <PID>" >&2
  exit 1
fi

timestamp() { date +"%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(timestamp)] $*"; }

mkdir -p "$LOG_DIR" "$BACKUP_DIR"

log "Waiting for PID ${PID_TO_WAIT_FOR} to finish..."
while kill -0 "$PID_TO_WAIT_FOR" 2>/dev/null; do
  sleep 5
done
log "PID ${PID_TO_WAIT_FOR} has exited. Proceeding to backup."

if [[ ! -f "$OUT_PARQUET" ]]; then
  log "ERROR: Output parquet not found at $OUT_PARQUET"
  exit 2
fi

TS=$(date +%Y%m%d_%H%M%S)
BACKUP_PATH="$BACKUP_DIR/edm_metadata_full_${TS}_pid${PID_TO_WAIT_FOR}.parquet"

src_size=$(stat -c%s "$OUT_PARQUET")
log "Backing up $OUT_PARQUET (size=${src_size} bytes) -> $BACKUP_PATH"
cp -f "$OUT_PARQUET" "$BACKUP_PATH"

dst_size=$(stat -c%s "$BACKUP_PATH")
if [[ "$src_size" -ne "$dst_size" ]]; then
  log "ERROR: Backup size mismatch (src=${src_size}, dst=${dst_size})"
  exit 3
fi

if command -v sha256sum >/dev/null 2>&1; then
  log "Verifying backup integrity via sha256sum..."
  src_sha=$(sha256sum "$OUT_PARQUET" | awk '{print $1}')
  dst_sha=$(sha256sum "$BACKUP_PATH" | awk '{print $1}')
  if [[ "$src_sha" != "$dst_sha" ]]; then
    log "ERROR: Backup checksum mismatch (src=$src_sha dst=$dst_sha)"
    exit 4
  fi
  log "Backup verified (sha256=$src_sha)"
else
  log "sha256sum not available; verified by size only."
fi

log "Starting resume+retry run (retries=1, conc=15, wait=1s, timeout=1s)"

if [[ ! -d "$ROOT/.venv" ]]; then
  log "ERROR: Python venv not found at $ROOT/.venv"
  exit 5
fi

# Launch the retry run
RUN_TS=$(date +%Y%m%d_%H%M%S)
CONSOLE_LOG="$LOG_DIR/console_edm_retry_${RUN_TS}.log"
PID_FILE="$LOG_DIR/edm_retry_${RUN_TS}.pid"

# Notes:
# - --resume ensures we do not lose prior results
# - merge logic in fetch_edm_metadata prioritizes 200 over non-200 and keeps latest among non-200
# - This run will overwrite only with better outcomes

set +e
source "$ROOT/.venv/bin/activate"
set -e

nohup python -m scraper.fast_scraper.fetch_edm_metadata \
  --in-links "$ALL_LINKS" \
  --out "$OUT_PARQUET" \
  --limit 0 \
  --concurrency 15 \
  --timeout 1 \
  --wait 1 \
  --retries 1 \
  --resume \
  --retry-only \
  --progress \
  --compression snappy \
  --log-dir "$LOG_DIR" \
  --metrics-chunk 5000 \
  --project-total 830313 \
  > "$CONSOLE_LOG" 2>&1 &

NEW_PID=$!
echo "$NEW_PID" > "$PID_FILE"
log "Launched retry run. PID=$NEW_PID"
log "Console: $CONSOLE_LOG"
log "PID file: $PID_FILE"

exit 0




