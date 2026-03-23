# OpenClaw Chaining Templates

Use these templates directly in OpenClaw and replace only bracketed placeholders.

## Recon

Use $usgs-water-iv-fetch.
Run:

```bash
python3 scripts/usgs_water_iv_fetch.py fetch \
  --bbox=[MIN_LON,MIN_LAT,MAX_LON,MAX_LAT] \
  --period P1D \
  --parameter-code 00060 \
  --parameter-code 00065 \
  --site-type ST \
  --site-status active \
  --dry-run \
  --pretty
```

Return only the JSON result.

## Fetch

Use $usgs-water-iv-fetch.
Run:

```bash
python3 scripts/usgs_water_iv_fetch.py fetch \
  --bbox=[MIN_LON,MIN_LAT,MAX_LON,MAX_LAT] \
  --start-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --end-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --parameter-code 00060 \
  --parameter-code 00065 \
  --site-type ST \
  --site-status active \
  --pretty
```

Return only the JSON result.
