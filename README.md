OpenArchives External PDFs Scraper

Overview
- Collects OpenArchives collections and item pages, fetches EDM metadata, and crawls external item pages to extract in-order PDF links.
- Scales via parallel orchestration with safe per-collection shards and an atomic master parquet merge.

Requirements
- Python 3.10+
- pip install: pandas, pyarrow, httpx, beautifulsoup4 (for collection parsing)

Setup
- Create a virtualenv and install deps:
  python -m venv .venv
  . .venv/bin/activate
  pip install -U pip
  pip install pandas pyarrow httpx beautifulsoup4

Data layout (ignored by git)
- openarchives_data/: intermediate inputs (collections, all_links, pages)
- openarchives_results/: master and shards for external page crawl
  - external_pdfs.parquet (master)
  - shards/external_pdfs__{slug}__{YYYYMMDD_HHMMSS}.parquet
  - backups/
- logs/: metrics CSVs per collection + orchestrator logs

Pipeline
1) Collect collections (institutions → collections with item_count)
   python -m scraper.fast_scraper.collect_collections \
     --out openarchives_data/collections.parquet

2) Collect item page links per collection
   python -m scraper.fast_scraper.collect_all_links \
     --collections openarchives_data/collections.parquet \
     --out-dir openarchives_data

3) Fetch EDM metadata for items (edm_url, external_link, type, language)
   python -m scraper.fast_scraper.fetch_edm_metadata \
     --links openarchives_data/all_links.parquet \
     --out openarchives_results/edm_metadata_full.parquet \
     --out-labeled openarchives_results/edm_metadata_labeled.parquet

4) Crawl external item pages and extract PDF links (parallel)
  .venv/bin/python scripts/parallel_adaptive.py \
    --edm openarchives_results/edm_metadata_labeled.parquet \
    --collections openarchives_results/collections_links_enriched_ge1k_sorted.parquet \
    --out openarchives_results/external_pdfs.parquet \
    --max-parallel 10 --per-concurrency 15 --retries 0 \
    --ua-mode random --force-http1 --rolling

   Notes:
   - Writes per-collection shards to openarchives_results/shards/
   - Merges into external_pdfs.parquet at the end
   - Status codes: 200 ok; 404 not found; 5xx server error; -1 timeout; -2 HTTP/transport error; -3 skipped (baseline)

Reduced follow-up run (gentle)
- Starts after the main run completes (and after master > all shards), excludes known rate-limited slugs, runs with lower per-child concurrency and 5s timeout:
  nohup scripts/trigger_reduced_run.sh -c 2 -p 4 -t 5 >/dev/null 2>&1 &

Merge semantics
- Page-level identity key: [collection_slug, page_number, edm_url]
- Prefer 200 rows over non-200; among non-200, keep the most recent (higher retries)
- Master merge is atomic (tmp + replace)

Operational tips
- Tail orchestrator: tail -f logs/parallel_adaptive_*.log
- Metrics per collection: logs/metrics_{slug}_{timestamp}.csv (t_epoch, completed, failed, fail_pct, speed_ps, eta_s, remaining)
- Common env knobs for orchestrator: STATUS_INTERVAL_S, PARQUET_REFRESH_S

Per-collection overrides (advanced)
- Run different collections with different settings in one orchestration using a JSON overrides file:
  overrides.json:
  {
    "deltion":  {"per_concurrency":4, "wait":0.3, "timeout":12, "retries":1, "ua_mode":"random", "force_http1":true},
    "helios":   {"per_concurrency":8, "wait":0.2, "timeout":10, "retries":0},
    "Pandemos": {"per_concurrency":2, "wait":0.3, "timeout":12, "retries":1, "ua_mode":"random", "force_http1":true},
    "dias":     {"per_concurrency":3, "wait":0.2, "timeout":12, "retries":1, "ua_mode":"random", "force_http1":true}
  }

  .venv/bin/python scripts/parallel_adaptive.py \
    --edm openarchives_results/edm_metadata_labeled.parquet \
    --collections openarchives_results/collections_links_enriched_ge1k_sorted.parquet \
    --out openarchives_results/external_pdfs.parquet \
    --max-parallel 6 --per-concurrency 4 --retries 0 \
    --ua-mode random --force-http1 --rolling --merge-each-wave \
    --overrides-json overrides.json \
    --only-slugs deltion ekke JHVMS geosociety cetpe makedonika pasithee helios hellanicus rep_ihu pergamos pyxida kallipos Pandemos dias \
    --baseline openarchives_results/external_pdfs.parquet

Resampling 200/no‑PDF URLs (child)
- The crawler supports `--resample-nopdf` with `--resume-baseline` to revisit URLs that were 200 but had `pdf_links_count==0`:

  .venv/bin/python -m scraper.fast_scraper.crawl_external_pdfs \
    --edm openarchives_results/edm_metadata_labeled.parquet \
    --collections openarchives_results/collections_links_enriched_ge1k_sorted.parquet \
    --out openarchives_results/external_pdfs.parquet \
    --resume --resume-baseline openarchives_results/external_pdfs.parquet \
    --resample-nopdf --concurrency 4 --timeout 12 --wait 0.3 --random-user-agent --force-http1

License
- TBD
