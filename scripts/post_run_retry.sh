#!/usr/bin/env bash
set -euo pipefail

ROOT="/mnt/data/openarchive_scraper"
cd "$ROOT"

# Activate venv if present
if [ -f .venv/bin/activate ]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate || true
fi

LOG_DIR="$ROOT/logs"
OUT_DIR="$ROOT/openarchives_results"
IN_PARQUET="$ROOT/openarchives_data/collections.parquet"

mkdir -p "$LOG_DIR"

# Find latest running collector PID file, if any, and wait for it to exit
LATEST_PID_FILE="$(ls -1t "$LOG_DIR"/collect_all_*.pid 2>/dev/null | head -n1 || true)"
if [ -n "${LATEST_PID_FILE}" ] && [ -f "${LATEST_PID_FILE}" ]; then
  PID="$(cat "${LATEST_PID_FILE}" || true)"
  if [ -n "${PID}" ]; then
    echo "Waiting for PID ${PID} from ${LATEST_PID_FILE} to finish..."
    while kill -0 "${PID}" 2>/dev/null; do
      sleep 30
    done
    echo "Previous run finished."
  fi
fi

# Backup current results
TS="$(date +%Y%m%d_%H%M%S)"
BKP_DIR="$ROOT/openarchives_results_backups/${TS}"
mkdir -p "$BKP_DIR"
for f in all_links.parquet pages.parquet collections_links_summary.parquet; do
  if [ -f "$OUT_DIR/$f" ]; then
    cp -p "$OUT_DIR/$f" "$BKP_DIR/$f"
  fi
done
echo "Backed up current parquets to $BKP_DIR"

# Launch retry run with slower settings and retries for prior non-200 pages
RTS="$(date +%Y%m%d_%H%M%S)"
CONSOLE_LOG="$LOG_DIR/console_collect_all_${RTS}_retry.log"
PID_OUT="$LOG_DIR/collect_all_${RTS}_retry.pid"

nohup python -m scraper.fast_scraper.collect_all_links \
  --in "$IN_PARQUET" \
  --out-dir "$OUT_DIR" \
  --concurrency 3 \
  --timeout 3 \
  --max-500-ratio 0.99 \
  --threshold-start 1000 \
  --progress --pages-estimate 25979 \
  --wait 1 \
  --resume \
  --retries 1 \
  --log-dir "$LOG_DIR" > "$CONSOLE_LOG" 2>&1 & echo $! > "$PID_OUT"

echo "Retry launched. Console: $CONSOLE_LOG PID: $(cat "$PID_OUT")"























