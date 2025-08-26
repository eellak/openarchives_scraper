#!/usr/bin/env python3
"""Check EDM metadata parquet file for data quality issues."""

import pandas as pd
import json

# Read the parquet file
df = pd.read_parquet('/mnt/data/openarchive_scraper/openarchives_results/edm_metadata_10k.parquet')

# Basic info
print('=== BASIC INFO ===')
print(f'Shape: {df.shape}')
print(f'Columns: {list(df.columns)}')
print()

# Check for missing values
print('=== MISSING VALUES ===')
print(df.isnull().sum())
print()

# Status code distribution
print('=== STATUS CODE DISTRIBUTION ===')
print(df['status_code'].value_counts().sort_index())
print()

# Check for duplicates
print('=== DUPLICATE CHECK ===')
print(f'Total rows: {len(df)}')
print(f'Unique EDM URLs: {df["edm_url"].nunique()}')
print(f'Duplicate EDM URLs: {len(df) - df["edm_url"].nunique()}')
print()

# External links analysis
print('=== EXTERNAL LINKS ===')
external_found = df['external_link'].notna().sum()
print(f'Records with external links: {external_found} ({external_found/len(df)*100:.1f}%)')
print()

# Metadata analysis
print('=== METADATA FIELDS ===')
metadata_stats = df['metadata_num_fields'].describe()
print(metadata_stats)
print()

# Sample problematic records
print('=== SAMPLE FAILED RECORDS ===')
failed = df[df['status_code'] != 200].head(3)
for _, row in failed.iterrows():
    print(f'URL: {row["edm_url"]}')
    print(f'Status: {row["status_code"]}, Time: {row["elapsed_s"]:.2f}s')
    print()

# Check metadata JSON validity
print('=== METADATA JSON CHECK ===')
invalid_json = 0
empty_json = 0
successful_with_data = 0
for idx, row in df.iterrows():
    try:
        meta = json.loads(row['metadata_json'])
        if not meta:
            empty_json += 1
        elif row['status_code'] == 200:
            successful_with_data += 1
    except:
        invalid_json += 1

print(f'Invalid JSON: {invalid_json}')
print(f'Empty metadata (valid but empty): {empty_json}')
print(f'Successful requests with metadata: {successful_with_data}')
print()

# Collection distribution
print('=== COLLECTION DISTRIBUTION (top 10) ===')
collection_counts = df['collection_slug'].value_counts().head(10)
print(collection_counts)
print()

# Check for anomalies in elapsed time
print('=== RESPONSE TIME ANALYSIS ===')
time_stats = df['elapsed_s'].describe()
print(time_stats)
print(f'Timeouts (status -1): {(df["status_code"] == -1).sum()}')
print(f'Other errors (status -2): {(df["status_code"] == -2).sum()}')