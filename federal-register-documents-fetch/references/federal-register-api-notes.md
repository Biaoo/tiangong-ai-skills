# Federal Register API Notes

## Documents Search Endpoint

- Primary endpoint used by this skill:
  - `GET /documents.json`
- Official API host:
  - `https://www.federalregister.gov/api/v1`

## Filters Used in v1

- `conditions[term]`
  - Full-text search term.
- `conditions[publication_date][is]`
  - Exact publication date, `YYYY-MM-DD`.
- `conditions[publication_date][year]`
  - Publication year, `YYYY`.
- `conditions[publication_date][gte]`
  - Published on or after date, `YYYY-MM-DD`.
- `conditions[publication_date][lte]`
  - Published on or before date, `YYYY-MM-DD`.
- `conditions[agencies][]`
  - Publishing agency slug.
- `conditions[type][]`
  - Document type code.
- `conditions[topics][]`
  - Topic slug.
- `conditions[docket_id]`
  - Agency docket ID.
- `conditions[regulation_id_number]`
  - Regulation ID Number.
- `conditions[sections][]`
  - Federal Register section slug.
- `order`
  - `relevance`, `newest`, `oldest`, `executive_order_number`
- `per_page`
  - Up to `1000` by the official API.
- `page`
  - Result page number.

## Document Type Codes

- `RULE`
  - Final Rule
- `PRORULE`
  - Proposed Rule
- `NOTICE`
  - Notice
- `PRESDOCU`
  - Presidential Document

## Common Result Fields

- `title`
- `type`
- `abstract`
- `document_number`
- `html_url`
- `pdf_url`
- `publication_date`
- `agencies`
- `excerpts`

## Pagination

- The API returns:
  - `count`
  - `total_pages`
  - `next_page_url`
  - `results`
- This skill follows `next_page_url` until:
  - no next page exists
  - `--max-pages` is reached
  - `--max-records` is reached
