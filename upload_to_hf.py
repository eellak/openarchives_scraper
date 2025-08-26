from huggingface_hub import HfApi
import os

api = HfApi()
repo_id = "glossAPI/openarchives.gr"

data_dir = "/mnt/data/openarchive_scraper/openarchives_results"
files = {
    "all_links": "all_links.parquet",
    "collections_summary": "collections_links_summary.parquet", 
    "edm_metadata": "edm_metadata_full.parquet",
    "pages": "pages.parquet"
}

for name, filename in files.items():
    filepath = os.path.join(data_dir, filename)
    print(f"Uploading {filename} to {repo_id}...")
    
    api.upload_file(
        path_or_fileobj=filepath,
        path_in_repo=f"data/{filename}",
        repo_id=repo_id,
        repo_type="dataset"
    )
    print(f"✓ Uploaded {filename}")

print("\nCreating dataset card...")
readme_content = """---
dataset_info:
  features:
  - name: all_links
    description: Links to all EDM records across collections
  - name: collections_summary
    description: Summary statistics for each collection
  - name: edm_metadata
    description: Full EDM metadata for all records
  - name: pages
    description: Page information for collections
---

# OpenArchives.gr Dataset

This dataset contains scraped data from OpenArchives.gr, including:
- All EDM record links across collections
- Collection summary statistics
- Full EDM metadata
- Page information

## Files
- `all_links.parquet`: 830,313 EDM record links
- `collections_summary.parquet`: 140 collection summaries
- `edm_metadata.parquet`: 830,313 full EDM metadata records
- `pages.parquet`: 27,749 page records
"""

api.upload_file(
    path_or_fileobj=readme_content.encode(),
    path_in_repo="README.md",
    repo_id=repo_id,
    repo_type="dataset"
)
print("✓ Created dataset card")
print(f"\nDataset uploaded to: https://huggingface.co/datasets/{repo_id}")