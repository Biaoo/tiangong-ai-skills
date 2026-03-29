# USGS Water IV API Notes

- Endpoint:
  - `https://waterservices.usgs.gov/nwis/iv/`
- Recommended eco-council query style:
  - `format=json`
  - one major filter:
    - `bBox=minLon,minLat,maxLon,maxLat`
    - or `sites=site1,site2,...`
  - one time selector:
    - `period=P1D`
    - or `startDT=...` with `endDT=...`
  - hydrology parameters:
    - `parameterCd=00060` for discharge
    - `parameterCd=00065` for gage height
  - site narrowing:
    - `siteType=ST`
    - `siteStatus=active`

- The JSON response is WaterML-style JSON.
- Relevant nested fields:
  - `value.queryInfo`
  - `value.timeSeries[]`
  - `timeSeries[].sourceInfo`
  - `timeSeries[].variable`
  - `timeSeries[].values[].value[]`

- Useful site-property fields commonly present in `sourceInfo.siteProperty[]`:
  - `siteTypeCd`
  - `hucCd`
  - `stateCd`
  - `countyCd`

- Useful variable codes for the first eco-council integration:
  - `00060` -> discharge / streamflow
  - `00065` -> gage height
