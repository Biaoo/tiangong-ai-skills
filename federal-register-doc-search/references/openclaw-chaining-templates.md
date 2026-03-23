# OpenClaw Chaining Templates

## Recon

```text
Use $federal-register-doc-search.
Run:
python3 scripts/federal_register_doc_search.py check-config --pretty
Return only the JSON result.
```

## Dry Run

```text
Use $federal-register-doc-search.
Run:
python3 scripts/federal_register_doc_search.py search \
  --term "[QUERY_TEXT]" \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --max-pages [N] \
  --max-records [M] \
  --dry-run \
  --pretty
Return only the JSON result.
```

## Fetch

```text
Use $federal-register-doc-search.
Run:
python3 scripts/federal_register_doc_search.py search \
  --term "[QUERY_TEXT]" \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --agency [OPTIONAL_AGENCY_SLUG] \
  --document-type [OPTIONAL_TYPE] \
  --output [OUTPUT_FILE] \
  --pretty
Return only the JSON result.
```

## Validate

```text
Use $federal-register-doc-search.
Run:
python3 scripts/federal_register_doc_search.py search \
  --term "[QUERY_TEXT]" \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --max-pages 1 \
  --max-records 10 \
  --pretty
Check returned_count and validation_summary.total_issue_count.
Return JSON plus one-line pass/fail verdict.
```
