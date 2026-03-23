# OpenClaw Chaining Templates

## Dry Run

```text
Use $federal-register-documents-fetch.
Run:
python3 scripts/federal_register_documents_fetch.py fetch \
  --search-term "[QUERY]" \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --max-pages 2 \
  --dry-run \
  --pretty
Return only the JSON result.
```

## Fetch

```text
Use $federal-register-documents-fetch.
Run:
python3 scripts/federal_register_documents_fetch.py fetch \
  --search-term "[QUERY]" \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --agency [OPTIONAL_AGENCY_SLUG] \
  --document-type [OPTIONAL_TYPE_CODE] \
  --max-pages [N] \
  --max-records [M] \
  --output [OUTPUT_FILE] \
  --pretty
Return only the JSON result.
```
