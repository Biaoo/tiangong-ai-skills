# OpenClaw Chaining Templates

Use this skill atomically for one bounded hydrology pull.
For eco-council runs, always use the exact role-owned `raw/` artifact path from the fetch plan as `--output`.

## Recon

```text
Use $usgs-water-iv-fetch.
Run:
python3 scripts/usgs_water_iv_fetch.py fetch \
  --bbox=[MIN_LON,MIN_LAT,MAX_LON,MAX_LAT] \
  --period P1D \
  --parameter-code 00060 \
  --parameter-code 00065 \
  --site-type ST \
  --site-status active \
  --dry-run \
  --pretty
Return only the JSON result.
```

## Fetch

```text
Use $usgs-water-iv-fetch.
Run:
python3 scripts/usgs_water_iv_fetch.py fetch \
  --bbox=[MIN_LON,MIN_LAT,MAX_LON,MAX_LAT] \
  --start-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --end-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --parameter-code 00060 \
  --parameter-code 00065 \
  --site-type ST \
  --site-status active \
  --output [OUTPUT_FILE] \
  --pretty
Return the JSON result and confirm `[OUTPUT_FILE]`.
```

## Explicit Sites

```text
Use $usgs-water-iv-fetch.
Run:
python3 scripts/usgs_water_iv_fetch.py fetch \
  --site [SITE_NUMBER_1] \
  --site [SITE_NUMBER_2] \
  --period P1D \
  --parameter-code 00060 \
  --parameter-code 00065 \
  --output [OUTPUT_FILE] \
  --pretty
Return the JSON result and confirm `[OUTPUT_FILE]`.
```
