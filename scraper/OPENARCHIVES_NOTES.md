### OpenArchives exploration notes

- **Institutions index**: `https://www.openarchives.gr/aggregator-openarchives/portal/institutions`
- **First institution (observed)**: `https://www.openarchives.gr/aggregator-openarchives/portal/institutions/hnpsociety` (Hellenic Nuclear Physics Society)
- **HNPS collection**: `https://www.openarchives.gr/aggregator-openarchives/portal/collections/hnps`
- **HNPS search with explicit page**:
  - Page 1: `https://www.openarchives.gr/aggregator-openarchives/portal/collections/hnps/search?page.page=1&sortResults=SCORE`
  - Page 2: `https://www.openarchives.gr/aggregator-openarchives/portal/collections/hnps/search?page.page=2&sortResults=SCORE`

Notes:
- Pagination UI is JS-rendered; however, the backend accepts `/search?page.page=N` and stable `sortResults=SCORE`.
- Pages contain ~30 items per page.



