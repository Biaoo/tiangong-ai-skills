# AirNow Hourly AQ Obs Limits

## Data Caveats

- AirNow file-product data are preliminary and subject to revision.
- Data are intended for AQI reporting support and should not be treated as regulatory-grade AQS replacement data.
- Not every site reports every parameter in every hour.
- Negative raw concentrations can appear in clean-air conditions for some instruments.

## Operational Constraints

- Hourly files can be large; broad windows and wide bounding boxes can return high row counts.
- Historical completeness may vary by network and region.
- Repeated requests for large ranges should use local caching or staged extraction.

## Skill Safety Caps

Defaults from this skill:
- `AIRNOW_HOURLY_MAX_HOURS_PER_RUN=168`
- `AIRNOW_HOURLY_MAX_FILES_PER_RUN=168`
- `AIRNOW_HOURLY_MAX_ROWS_PER_FILE=250000`

Raise these only when downstream storage and processing are prepared for larger payloads.

## Scope Boundaries

- This skill does not maintain a long-lived database itself.
- This skill does not infer health guidance from AQI categories.
- This skill does not fuse with non-AirNow sources automatically.
