# USGS Water IV Limitations

- This skill only covers the `Instantaneous Values` service.
- It does not fetch `Daily Values`, statistics, or separate site-service RDB catalogs.
- This service is best for station-based real-time or recent historical hydrology, not for global modeled background fields.
- Records can contain provisional qualifiers. Treat them as operational evidence, not final regulatory-grade adjudication.
- Site coverage is limited to USGS-served sites and the parameter codes actually available at those sites.
- Very broad bbox or long-window requests can return large payloads. Use parameter, site type, site status, and time filters aggressively.
- The current Water Services site announces decommissioning in early 2027 and migration to `api.waterdata.usgs.gov`. This skill currently targets the existing Water Services endpoint and can be migrated later.
