"""Storage writer – accumulates records in-memory and flushes them to Parquet."""
from pathlib import Path
from typing import List, Dict
import logging

import pandas as pd


class StorageWriter:
    def __init__(self, out_path: str = "openarchives_data/openarchives_data.parquet"):
        self.records: List[Dict] = []
        self.out_path = out_path
        Path(self.out_path).parent.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger("openarchives")

    def add(self, record: Dict):
        self.records.append(record)

    def flush(self):
        if not self.records:
            self.logger.info("No new records to write – skipping flush.")
            return
        df_new = pd.DataFrame(self.records)

        # If file exists, concatenate with previous data to avoid overwriting
        if Path(self.out_path).exists():
            try:
                df_existing = pd.read_parquet(self.out_path)
                df_all = pd.concat([df_existing, df_new], ignore_index=True)
            except Exception as exc:
                self.logger.warning("Could not read existing parquet – will overwrite: %s", exc)
                df_all = df_new
        else:
            df_all = df_new

        df_all.to_parquet(self.out_path, index=False)
        self.logger.info("Wrote %d records (total %d) → %s", len(df_new), len(df_all), self.out_path)
        # Keep the list small for memory efficiency – could also keep all if desired
        self.records.clear() 