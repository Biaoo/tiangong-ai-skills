---
name: usgs-water-iv-fetch
description: Fetch USGS Water Services Instantaneous Values JSON for one bounding box or explicit site list, then return structured hydrology observations with site metadata, parameter codes, timestamps, qualifiers, and validation output. Use when tasks need deterministic station-based streamflow or gage-height evidence from USGS for flood, runoff, river, or water-level verification in the United States.
---

# USGS Water IV Fetch

## Core Goal
- Fetch USGS Water Services `iv` data for one bounding box or explicit site list.
- Keep the call atomic: one request, one structured JSON payload, no follow-up site-discovery chain required.
- Return site metadata plus flattened time-series records that downstream normalization can map into hydrology observations.
- Keep runtime deterministic with retries, throttling, response-size caps, and validation summaries.

## Repository Policy
- This is the canonical USGS station-hydrology skill in this repository.
- When eco-council or OpenClaw assigns a raw artifact path, write this skill's full JSON payload to that exact path with `--output`.
- Do not treat dry-run output as collected evidence.

## Required Environment
- Configure runtime by environment variables in `references/env.md`.
- Start from `assets/config.example.env`.
- Load env values before running commands:

```bash
set -a
source assets/config.example.env
set +a
```

## Workflow
1. Validate effective configuration.

```bash
python3 scripts/usgs_water_iv_fetch.py check-config --pretty
```

2. Dry-run the query plan first.

```bash
python3 scripts/usgs_water_iv_fetch.py fetch \
  --bbox=-77.3,38.8,-77.0,39.1 \
  --period P1D \
  --parameter-code 00060 \
  --parameter-code 00065 \
  --site-type ST \
  --site-status active \
  --dry-run \
  --pretty
```

3. Run one time-window fetch and write the payload.

```bash
python3 scripts/usgs_water_iv_fetch.py fetch \
  --bbox=-77.3,38.8,-77.0,39.1 \
  --start-datetime 2026-03-21T00:00:00Z \
  --end-datetime 2026-03-22T23:59:59Z \
  --parameter-code 00060 \
  --parameter-code 00065 \
  --site-type ST \
  --site-status active \
  --output ./data/usgs-water-iv.json \
  --pretty
```

4. Optional explicit-site fetch when the mission already knows site numbers.

```bash
python3 scripts/usgs_water_iv_fetch.py fetch \
  --site 01646500 \
  --site 01646000 \
  --period P1D \
  --parameter-code 00060 \
  --parameter-code 00065 \
  --output ./data/usgs-water-iv-sites.json \
  --pretty
```

## Output Record Shape
Each item in `records` is one site-parameter-timestamp observation with fields:
- `site_number`, `site_name`, `agency_code`
- `site_type`, `state_code`, `county_code`, `huc_code`
- `latitude`, `longitude`
- `parameter_code`, `variable_name`, `variable_description`, `unit`
- `observed_at_utc`, `value`
- `qualifiers`, `provisional`
- `source_query_url`

## Scope Boundaries
- This skill targets the USGS Water Services `Instantaneous Values` endpoint only.
- This skill does not infer flood thresholds, return periods, or policy meaning.
- This skill does not geocode place names.
- This skill does not need an API key.
- This skill supports `file://` base URLs for deterministic local fixture testing.

## References
- `references/env.md`
- `references/usgs-water-iv-api-notes.md`
- `references/usgs-water-iv-limitations.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/usgs_water_iv_fetch.py`
