# AirNow Hourly AQ Obs File Notes

## Data Source

- Official AirNow file-product host:
  - `https://files.airnowtech.org`
- Hourly AQ Obs path pattern:
  - `/airnow/YYYY/YYYYMMDD/HourlyAQObs_yyyymmddhh.dat`

This skill builds URL paths from UTC hour values and fetches plain text CSV files.

## Temporal Semantics

- The hourly filename hour is in UTC/GMT.
- `ValidDate` + `ValidTime` in each row represent the beginning of the measurement hour.
- Files are updated hourly and may be revised for data completeness.

## Input Filters Used By This Skill

- Bounding box:
  - `min_lon,min_lat,max_lon,max_lat`
- UTC window:
  - `--start-datetime` and `--end-datetime` (inclusive, hour-by-hour file iteration)
- Parameters:
  - `OZONE`, `PM10`, `PM25`, `NO2`, `CO`, `SO2`

## Field Mapping Used By This Skill

Top-level site/time metadata:
- `AQSID`, `SiteName`, `Status`, `EPARegion`
- `Latitude`, `Longitude`, `CountryCode`, `StateName`
- `ValidDate`, `ValidTime`, `DataSource`, `ReportingArea_PipeDelimited`

Per-parameter value columns:
- AQI columns:
  - `OZONE_AQI`, `PM10_AQI`, `PM25_AQI`, `NO2_AQI`
- Measured flags:
  - `OZONE_Measured`, `PM10_Measured`, `PM25_Measured`, `NO2_Measured`
- Raw concentration + unit:
  - `OZONE`, `OZONE_Unit`
  - `PM10`, `PM10_Unit`
  - `PM25`, `PM25_Unit`
  - `NO2`, `NO2_Unit`
  - `CO`, `CO_Unit`
  - `SO2`, `SO2_Unit`

AQI kind mapping used by the script:
- `OZONE`, `PM10`, `PM25` => `nowcast-aqi`
- `NO2` => `hourly-aqi`
- `CO`, `SO2` => empty AQI fields
