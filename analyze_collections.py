import pandas as pd
import math

# Load the parquet file
df = pd.read_parquet('openarchives_data/collections.parquet')

# Filter out collections with 404 status code
df_no_404 = df[df['probe_status_code'] != 404]

print(f'Total collections: {len(df)}')
print(f'Collections without 404: {len(df_no_404)}')
print(f'Collections with 404: {len(df) - len(df_no_404)}')

# Sum the item_count for collections without 404
total_items = df_no_404['item_count'].sum()
print(f'\nTotal items from collections without 404: {total_items:,}')

# Calculate expected pages (divide by 30 and round up)
expected_pages = math.ceil(total_items / 30)
print(f'Expected number of pages (total items / 30, rounded up): {expected_pages:,}')