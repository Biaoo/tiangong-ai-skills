#!/usr/bin/env python3
"""Fetch AirNow Hourly AQ Obs file products with filtering and validation."""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError

ENV_BASE_URL = "AIRNOW_HOURLY_BASE_URL"
ENV_PATH_TEMPLATE = "AIRNOW_HOURLY_PATH_TEMPLATE"
ENV_TIMEOUT_SECONDS = "AIRNOW_HOURLY_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "AIRNOW_HOURLY_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "AIRNOW_HOURLY_RETRY_BACKOFF_SECONDS"
ENV_RETRY_BACKOFF_MULTIPLIER = "AIRNOW_HOURLY_RETRY_BACKOFF_MULTIPLIER"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "AIRNOW_HOURLY_MIN_REQUEST_INTERVAL_SECONDS"
ENV_MAX_HOURS_PER_RUN = "AIRNOW_HOURLY_MAX_HOURS_PER_RUN"
ENV_MAX_FILES_PER_RUN = "AIRNOW_HOURLY_MAX_FILES_PER_RUN"
ENV_MAX_ROWS_PER_FILE = "AIRNOW_HOURLY_MAX_ROWS_PER_FILE"
ENV_MAX_RETRY_AFTER_SECONDS = "AIRNOW_HOURLY_MAX_RETRY_AFTER_SECONDS"
ENV_USER_AGENT = "AIRNOW_HOURLY_USER_AGENT"

DEFAULT_BASE_URL = "https://files.airnowtech.org"
DEFAULT_PATH_TEMPLATE = "/airnow/{year}/{ymd}/HourlyAQObs_{yyyymmddhh}.dat"
DEFAULT_TIMEOUT_SECONDS = 45
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.4
DEFAULT_MAX_HOURS_PER_RUN = 168
DEFAULT_MAX_FILES_PER_RUN = 168
DEFAULT_MAX_ROWS_PER_FILE = 250000
DEFAULT_MAX_RETRY_AFTER_SECONDS = 120
DEFAULT_USER_AGENT = "airnow-hourly-obs-fetch/1.0"
DEFAULT_MAX_VALIDATION_ISSUES = 100
SCHEMA_VERSION = "1.0.0"

RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}
SUPPORTED_PARAMETERS = ("OZONE", "PM10", "PM25", "NO2", "CO", "SO2")
AQI_COLUMN = {
    "OZONE": "OZONE_AQI",
    "PM10": "PM10_AQI",
    "PM25": "PM25_AQI",
    "NO2": "NO2_AQI",
}
MEASURED_COLUMN = {
    "OZONE": "OZONE_Measured",
    "PM10": "PM10_Measured",
    "PM25": "PM25_Measured",
    "NO2": "NO2_Measured",
}
AQI_KIND = {
    "OZONE": "nowcast-aqi",
    "PM10": "nowcast-aqi",
    "PM25": "nowcast-aqi",
    "NO2": "hourly-aqi",
}


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    path_template: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    retry_backoff_multiplier: float
    min_request_interval_seconds: float
    max_hours_per_run: int
    max_files_per_run: int
    max_rows_per_file: int
    max_retry_after_seconds: int
    user_agent: str


@dataclass(frozen=True)
class BoundingBox:
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    def contains(self, *, latitude: float, longitude: float) -> bool:
        return self.min_lon <= longitude <= self.max_lon and self.min_lat <= latitude <= self.max_lat


@dataclass(frozen=True)
class FileFetchResult:
    url: str
    ok: bool
    status_code: int
    attempts: int
    retry_count: int
    byte_length: int
    text: str
    error: str


@dataclass
class IssueCollector:
    max_issues: int
    total_count: int = 0
    issues: list[dict[str, Any]] = field(default_factory=list)

    def add(self, issue: dict[str, Any]) -> None:
        self.total_count += 1
        if len(self.issues) < self.max_issues:
            self.issues.append(issue)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def serialize_json(data: Any, *, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def env_or_default(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


def parse_positive_int(name: str, raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")
    return value


def parse_non_negative_int(name: str, raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")
    return value


def parse_positive_float(name: str, raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")
    return value


def parse_non_negative_float(name: str, raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")
    return value


def normalize_base_url(raw: str) -> str:
    text = raw.strip().rstrip("/")
    if not text:
        raise ValueError("Base URL cannot be empty.")
    if not (text.startswith("http://") or text.startswith("https://") or text.startswith("file://")):
        raise ValueError(f"Base URL must start with http://, https://, or file://, got {text!r}")
    return text


def normalize_path_template(raw: str) -> str:
    text = raw.strip()
    required_tokens = ("{year}", "{ymd}", "{yyyymmddhh}")
    if not text:
        raise ValueError("Path template cannot be empty.")
    if not text.startswith("/"):
        raise ValueError("Path template must start with '/'.")
    missing = [token for token in required_tokens if token not in text]
    if missing:
        raise ValueError(f"Path template missing placeholders: {', '.join(missing)}")
    return text


def normalize_parameter(raw: str) -> str:
    value = raw.strip().upper()
    if value not in SUPPORTED_PARAMETERS:
        raise ValueError(f"Unsupported parameter {raw!r}. Supported: {', '.join(SUPPORTED_PARAMETERS)}")
    return value


def parse_bbox(raw: str) -> BoundingBox:
    parts = [item.strip() for item in raw.split(",")]
    if len(parts) != 4:
        raise ValueError(f"Invalid --bbox {raw!r}. Expected min_lon,min_lat,max_lon,max_lat.")
    try:
        min_lon, min_lat, max_lon, max_lat = [float(part) for part in parts]
    except ValueError as exc:
        raise ValueError(f"Invalid --bbox {raw!r}. Expected numeric coordinates.") from exc
    if min_lon >= max_lon:
        raise ValueError(f"Invalid --bbox {raw!r}. min_lon must be < max_lon.")
    if min_lat >= max_lat:
        raise ValueError(f"Invalid --bbox {raw!r}. min_lat must be < max_lat.")
    if min_lat < -90 or max_lat > 90:
        raise ValueError("Latitude bounds must stay within [-90, 90].")
    if min_lon < -180 or max_lon > 180:
        raise ValueError("Longitude bounds must stay within [-180, 180].")
    return BoundingBox(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat)


def parse_utc_datetime(name: str, raw: str) -> datetime:
    text = raw.strip()
    if not text:
        raise ValueError(f"{name} cannot be empty.")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{name} must be ISO-8601 datetime, got {raw!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include timezone, got {raw!r}")
    return parsed.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def parse_airnow_valid_datetime(valid_date: str, valid_time: str) -> datetime | None:
    date_text = valid_date.strip()
    time_text = valid_time.strip()
    if not date_text or not time_text:
        return None
    for date_fmt in ("%m/%d/%Y", "%m/%d/%y"):
        for time_fmt in ("%H:%M", "%H:%M:%S"):
            try:
                parsed = datetime.strptime(f"{date_text} {time_text}", f"{date_fmt} {time_fmt}")
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def to_rfc3339_z(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def maybe_float(value: Any) -> float | None:
    text = maybe_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def maybe_int(value: Any) -> int | None:
    text = maybe_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def parse_bool_flag(value: Any) -> bool | None:
    parsed = maybe_int(value)
    if parsed is None:
        return None
    return parsed != 0


def split_reporting_areas(raw: str) -> list[str]:
    if not raw.strip():
        return []
    return [item.strip() for item in raw.split("|") if item.strip()]


def iter_utc_hours(start: datetime, end: datetime) -> list[datetime]:
    hours: list[datetime] = []
    current = start
    while current <= end:
        hours.append(current)
        current += timedelta(hours=1)
    return hours


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    base_url = normalize_base_url(args.base_url or env_or_default(ENV_BASE_URL, DEFAULT_BASE_URL))
    path_template = normalize_path_template(args.path_template or env_or_default(ENV_PATH_TEMPLATE, DEFAULT_PATH_TEMPLATE))
    timeout_seconds = parse_positive_int(
        "--timeout-seconds",
        str(args.timeout_seconds if args.timeout_seconds is not None else env_or_default(ENV_TIMEOUT_SECONDS, str(DEFAULT_TIMEOUT_SECONDS))),
    )
    max_retries = parse_non_negative_int(
        "--max-retries",
        str(args.max_retries if args.max_retries is not None else env_or_default(ENV_MAX_RETRIES, str(DEFAULT_MAX_RETRIES))),
    )
    retry_backoff_seconds = parse_positive_float(
        "--retry-backoff-seconds",
        str(
            args.retry_backoff_seconds
            if args.retry_backoff_seconds is not None
            else env_or_default(ENV_RETRY_BACKOFF_SECONDS, str(DEFAULT_RETRY_BACKOFF_SECONDS))
        ),
    )
    retry_backoff_multiplier = parse_positive_float(
        "--retry-backoff-multiplier",
        str(
            args.retry_backoff_multiplier
            if args.retry_backoff_multiplier is not None
            else env_or_default(ENV_RETRY_BACKOFF_MULTIPLIER, str(DEFAULT_RETRY_BACKOFF_MULTIPLIER))
        ),
    )
    min_request_interval_seconds = parse_non_negative_float(
        "--min-request-interval-seconds",
        str(
            args.min_request_interval_seconds
            if args.min_request_interval_seconds is not None
            else env_or_default(ENV_MIN_REQUEST_INTERVAL_SECONDS, str(DEFAULT_MIN_REQUEST_INTERVAL_SECONDS))
        ),
    )
    max_hours_per_run = parse_positive_int(
        "--max-hours-per-run",
        str(args.max_hours_per_run if args.max_hours_per_run is not None else env_or_default(ENV_MAX_HOURS_PER_RUN, str(DEFAULT_MAX_HOURS_PER_RUN))),
    )
    max_files_per_run = parse_positive_int(
        "--max-files-per-run",
        str(args.max_files_per_run if args.max_files_per_run is not None else env_or_default(ENV_MAX_FILES_PER_RUN, str(DEFAULT_MAX_FILES_PER_RUN))),
    )
    max_rows_per_file = parse_positive_int(
        "--max-rows-per-file",
        str(args.max_rows_per_file if args.max_rows_per_file is not None else env_or_default(ENV_MAX_ROWS_PER_FILE, str(DEFAULT_MAX_ROWS_PER_FILE))),
    )
    max_retry_after_seconds = parse_positive_int(
        "--max-retry-after-seconds",
        str(
            args.max_retry_after_seconds
            if args.max_retry_after_seconds is not None
            else env_or_default(ENV_MAX_RETRY_AFTER_SECONDS, str(DEFAULT_MAX_RETRY_AFTER_SECONDS))
        ),
    )
    user_agent = maybe_text(args.user_agent if args.user_agent is not None else env_or_default(ENV_USER_AGENT, DEFAULT_USER_AGENT))
    if not user_agent:
        raise ValueError("User-Agent cannot be empty.")
    return RuntimeConfig(
        base_url=base_url,
        path_template=path_template,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        retry_backoff_multiplier=retry_backoff_multiplier,
        min_request_interval_seconds=min_request_interval_seconds,
        max_hours_per_run=max_hours_per_run,
        max_files_per_run=max_files_per_run,
        max_rows_per_file=max_rows_per_file,
        max_retry_after_seconds=max_retry_after_seconds,
        user_agent=user_agent,
    )


def mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def write_json(path: Path, payload: Any, *, pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = serialize_json(payload, pretty=pretty) + "\n"
    path.write_text(text, encoding="utf-8")


def parse_retry_after_seconds(value: str) -> int | None:
    text = value.strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    now = datetime.now(timezone.utc)
    delta = (parsed.astimezone(timezone.utc) - now).total_seconds()
    return max(0, int(delta))


def build_file_url(config: RuntimeConfig, hour_utc: datetime) -> str:
    year = hour_utc.strftime("%Y")
    ymd = hour_utc.strftime("%Y%m%d")
    yyyymmddhh = hour_utc.strftime("%Y%m%d%H")
    path = config.path_template.format(year=year, ymd=ymd, yyyymmddhh=yyyymmddhh)
    return f"{config.base_url}{path}"


def fetch_text_file(config: RuntimeConfig, url: str, logger: logging.Logger) -> FileFetchResult:
    delay = config.retry_backoff_seconds
    attempts = 0
    retries = 0
    last_error = "unknown_error"
    status_code = 0

    while True:
        attempts += 1
        req = request.Request(url, headers={"User-Agent": config.user_agent, "Accept": "text/plain,text/csv,*/*"})
        try:
            with request.urlopen(req, timeout=config.timeout_seconds) as resp:
                body = resp.read()
                status_raw = getattr(resp, "status", None)
                code = int(status_raw) if status_raw is not None else 200
                text = body.decode("utf-8", errors="replace")
                return FileFetchResult(
                    url=url,
                    ok=True,
                    status_code=code,
                    attempts=attempts,
                    retry_count=retries,
                    byte_length=len(body),
                    text=text,
                    error="",
                )
        except HTTPError as exc:
            status_code = int(exc.code)
            payload = exc.read() if hasattr(exc, "read") else b""
            last_error = f"http_{status_code}"
            if status_code in RETRIABLE_HTTP_CODES and retries < config.max_retries:
                retry_after_raw = exc.headers.get("Retry-After") if exc.headers else None
                retry_after_seconds = parse_retry_after_seconds(retry_after_raw) if retry_after_raw else None
                if retry_after_seconds is not None:
                    if retry_after_seconds > config.max_retry_after_seconds:
                        return FileFetchResult(
                            url=url,
                            ok=False,
                            status_code=status_code,
                            attempts=attempts,
                            retry_count=retries,
                            byte_length=len(payload),
                            text="",
                            error=f"retry_after_too_long:{retry_after_seconds}s",
                        )
                    sleep_seconds = float(retry_after_seconds)
                else:
                    sleep_seconds = delay
                    delay *= config.retry_backoff_multiplier
                retries += 1
                logger.warning("retrying url=%s status=%s sleep=%.3fs", url, status_code, sleep_seconds)
                time.sleep(sleep_seconds)
                continue
            return FileFetchResult(
                url=url,
                ok=False,
                status_code=status_code,
                attempts=attempts,
                retry_count=retries,
                byte_length=len(payload),
                text="",
                error=last_error,
            )
        except URLError as exc:
            last_error = f"url_error:{exc.reason}"
            if retries < config.max_retries:
                sleep_seconds = delay
                delay *= config.retry_backoff_multiplier
                retries += 1
                logger.warning("retrying url=%s reason=%s sleep=%.3fs", url, exc.reason, sleep_seconds)
                time.sleep(sleep_seconds)
                continue
            return FileFetchResult(
                url=url,
                ok=False,
                status_code=status_code,
                attempts=attempts,
                retry_count=retries,
                byte_length=0,
                text="",
                error=last_error,
            )
        except TimeoutError:
            last_error = "timeout"
            if retries < config.max_retries:
                sleep_seconds = delay
                delay *= config.retry_backoff_multiplier
                retries += 1
                logger.warning("retrying url=%s reason=timeout sleep=%.3fs", url, sleep_seconds)
                time.sleep(sleep_seconds)
                continue
            return FileFetchResult(
                url=url,
                ok=False,
                status_code=status_code,
                attempts=attempts,
                retry_count=retries,
                byte_length=0,
                text="",
                error=last_error,
            )


def parse_rows(text: str, *, max_rows: int) -> tuple[list[dict[str, str]], bool]:
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict[str, str]] = []
    truncated = False
    for index, row in enumerate(reader):
        if index >= max_rows:
            truncated = True
            break
        rows.append({str(key): (value if value is not None else "") for key, value in row.items()})
    return rows, truncated


def normalize_records_for_rows(
    *,
    rows: list[dict[str, str]],
    source_file_url: str,
    bbox: BoundingBox,
    requested_parameters: list[str],
    window_start: datetime,
    window_end: datetime,
    fallback_hour: datetime,
    issues: IssueCollector,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    records: list[dict[str, Any]] = []
    metrics = {
        "input_rows": len(rows),
        "rows_with_bad_coordinates": 0,
        "rows_outside_bbox": 0,
        "rows_outside_window": 0,
        "parameter_candidates": 0,
        "parameter_emitted": 0,
    }

    for index, row in enumerate(rows):
        lat = maybe_float(row.get("Latitude"))
        lon = maybe_float(row.get("Longitude"))
        if lat is None or lon is None:
            metrics["rows_with_bad_coordinates"] += 1
            issues.add(
                {
                    "type": "bad_coordinate",
                    "row_index": index,
                    "aqsid": maybe_text(row.get("AQSID")),
                    "latitude": maybe_text(row.get("Latitude")),
                    "longitude": maybe_text(row.get("Longitude")),
                }
            )
            continue
        if not bbox.contains(latitude=lat, longitude=lon):
            metrics["rows_outside_bbox"] += 1
            continue

        observed_dt = parse_airnow_valid_datetime(maybe_text(row.get("ValidDate")), maybe_text(row.get("ValidTime")))
        if observed_dt is None:
            observed_dt = fallback_hour
            issues.add(
                {
                    "type": "bad_valid_datetime",
                    "row_index": index,
                    "aqsid": maybe_text(row.get("AQSID")),
                    "valid_date": maybe_text(row.get("ValidDate")),
                    "valid_time": maybe_text(row.get("ValidTime")),
                    "fallback": to_rfc3339_z(fallback_hour),
                }
            )
        if observed_dt < window_start or observed_dt > window_end:
            metrics["rows_outside_window"] += 1
            continue

        for parameter in requested_parameters:
            metrics["parameter_candidates"] += 1
            raw_value = maybe_float(row.get(parameter))
            unit_value = maybe_text(row.get(f"{parameter}_Unit"))
            aqi_value = maybe_float(row.get(AQI_COLUMN.get(parameter, ""))) if parameter in AQI_COLUMN else None
            measured = parse_bool_flag(row.get(MEASURED_COLUMN.get(parameter, ""))) if parameter in MEASURED_COLUMN else None

            # Emit only when at least one meaningful value exists.
            if raw_value is None and aqi_value is None and measured is None:
                continue

            records.append(
                {
                    "aqsid": maybe_text(row.get("AQSID")),
                    "site_name": maybe_text(row.get("SiteName")),
                    "status": maybe_text(row.get("Status")),
                    "epa_region": maybe_text(row.get("EPARegion")),
                    "latitude": lat,
                    "longitude": lon,
                    "country_code": maybe_text(row.get("CountryCode")),
                    "state_name": maybe_text(row.get("StateName")),
                    "observed_at_utc": to_rfc3339_z(observed_dt),
                    "data_source": maybe_text(row.get("DataSource")),
                    "reporting_areas": split_reporting_areas(maybe_text(row.get("ReportingArea_PipeDelimited"))),
                    "parameter_name": parameter,
                    "aqi_value": aqi_value,
                    "aqi_kind": AQI_KIND.get(parameter, ""),
                    "raw_concentration": raw_value,
                    "unit": unit_value,
                    "measured": measured,
                    "source_file_url": source_file_url,
                }
            )
            metrics["parameter_emitted"] += 1
    return records, metrics


def command_check_config(args: argparse.Namespace) -> dict[str, Any]:
    config = build_runtime_config(args)
    return {
        "command": "check-config",
        "ok": True,
        "payload": {
            "base_url": config.base_url,
            "path_template": config.path_template,
            "timeout_seconds": config.timeout_seconds,
            "max_retries": config.max_retries,
            "retry_backoff_seconds": config.retry_backoff_seconds,
            "retry_backoff_multiplier": config.retry_backoff_multiplier,
            "min_request_interval_seconds": config.min_request_interval_seconds,
            "max_hours_per_run": config.max_hours_per_run,
            "max_files_per_run": config.max_files_per_run,
            "max_rows_per_file": config.max_rows_per_file,
            "max_retry_after_seconds": config.max_retry_after_seconds,
            "user_agent": mask_secret(config.user_agent),
        },
    }


def command_fetch(args: argparse.Namespace) -> dict[str, Any]:
    logger = logging.getLogger("airnow_hourly_obs_fetch")
    config = build_runtime_config(args)
    bbox = parse_bbox(args.bbox)
    window_start = parse_utc_datetime("--start-datetime", args.start_datetime)
    window_end = parse_utc_datetime("--end-datetime", args.end_datetime)
    if window_end < window_start:
        raise ValueError("--end-datetime must be >= --start-datetime.")

    requested_parameters = [normalize_parameter(item) for item in (args.parameter or list(SUPPORTED_PARAMETERS))]
    dedup_parameters: list[str] = []
    seen: set[str] = set()
    for parameter in requested_parameters:
        if parameter in seen:
            continue
        seen.add(parameter)
        dedup_parameters.append(parameter)
    requested_parameters = dedup_parameters

    hours = iter_utc_hours(window_start, window_end)
    if len(hours) > config.max_hours_per_run:
        raise ValueError(
            f"Requested {len(hours)} hours exceeds max_hours_per_run={config.max_hours_per_run}."
        )
    if len(hours) > config.max_files_per_run:
        raise ValueError(
            f"Requested {len(hours)} files exceeds max_files_per_run={config.max_files_per_run}."
        )

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "source_skill": "airnow-hourly-obs-fetch",
        "generated_at_utc": utc_now_iso(),
        "request": {
            "bbox": {
                "min_lon": bbox.min_lon,
                "min_lat": bbox.min_lat,
                "max_lon": bbox.max_lon,
                "max_lat": bbox.max_lat,
            },
            "start_datetime_utc": to_rfc3339_z(window_start),
            "end_datetime_utc": to_rfc3339_z(window_end),
            "parameter_names": requested_parameters,
            "hour_count": len(hours),
            "hourly_file_urls": [build_file_url(config, hour) for hour in hours],
        },
        "dry_run": bool(args.dry_run),
        "transport": {
            "base_url": config.base_url,
            "attempted_files": 0,
            "successful_files": 0,
            "failed_files": 0,
            "total_bytes": 0,
            "total_retries": 0,
            "status_code_counts": {},
            "failures": [],
        },
        "validation_summary": {},
        "record_count": 0,
        "records": [],
        "artifacts": {},
    }
    if args.dry_run:
        payload["validation_summary"] = {
            "ok": True,
            "total_issue_count": 0,
            "issues": [],
            "hours_planned": len(hours),
            "records_emitted": 0,
        }
        return {"command": "fetch", "ok": True, "payload": payload}

    issues = IssueCollector(max_issues=args.max_validation_issues)
    all_records: list[dict[str, Any]] = []
    total_input_rows = 0
    rows_outside_bbox = 0
    rows_outside_window = 0
    rows_bad_coordinates = 0
    parameter_candidates = 0
    parameter_emitted = 0

    last_request_at = 0.0
    for hour in hours:
        now = time.monotonic()
        sleep_for = config.min_request_interval_seconds - (now - last_request_at)
        if sleep_for > 0:
            time.sleep(sleep_for)
        last_request_at = time.monotonic()

        file_url = build_file_url(config, hour)
        result = fetch_text_file(config, file_url, logger)

        payload["transport"]["attempted_files"] += 1
        payload["transport"]["total_retries"] += result.retry_count
        payload["transport"]["total_bytes"] += result.byte_length
        status_key = str(result.status_code or 0)
        payload["transport"]["status_code_counts"][status_key] = payload["transport"]["status_code_counts"].get(status_key, 0) + 1

        if not result.ok:
            payload["transport"]["failed_files"] += 1
            payload["transport"]["failures"].append(
                {
                    "url": result.url,
                    "status_code": result.status_code,
                    "error": result.error,
                    "attempts": result.attempts,
                }
            )
            continue

        payload["transport"]["successful_files"] += 1

        rows, truncated = parse_rows(result.text, max_rows=config.max_rows_per_file)
        if truncated:
            issues.add(
                {
                    "type": "rows_truncated",
                    "url": result.url,
                    "max_rows_per_file": config.max_rows_per_file,
                }
            )

        file_records, file_metrics = normalize_records_for_rows(
            rows=rows,
            source_file_url=result.url,
            bbox=bbox,
            requested_parameters=requested_parameters,
            window_start=window_start,
            window_end=window_end,
            fallback_hour=hour,
            issues=issues,
        )
        all_records.extend(file_records)
        total_input_rows += file_metrics["input_rows"]
        rows_bad_coordinates += file_metrics["rows_with_bad_coordinates"]
        rows_outside_bbox += file_metrics["rows_outside_bbox"]
        rows_outside_window += file_metrics["rows_outside_window"]
        parameter_candidates += file_metrics["parameter_candidates"]
        parameter_emitted += file_metrics["parameter_emitted"]

    validation_ok = payload["transport"]["failed_files"] == 0 and issues.total_count == 0
    payload["validation_summary"] = {
        "ok": validation_ok,
        "total_issue_count": issues.total_count,
        "issues": issues.issues,
        "input_rows": total_input_rows,
        "rows_bad_coordinates": rows_bad_coordinates,
        "rows_outside_bbox": rows_outside_bbox,
        "rows_outside_window": rows_outside_window,
        "parameter_candidates": parameter_candidates,
        "records_emitted": parameter_emitted,
    }
    payload["record_count"] = len(all_records)
    payload["records"] = all_records

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        write_json(output_path, payload, pretty=args.pretty)
        payload["artifacts"]["full_payload_json"] = str(output_path)

    if args.fail_on_validation_error and not validation_ok:
        raise RuntimeError(
            f"Validation failed with {issues.total_count} issues and {payload['transport']['failed_files']} failed files."
        )

    return {"command": "fetch", "ok": True, "payload": payload}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch AirNow Hourly AQ Obs files for a UTC window and bounding box.")
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check-config", help="Validate runtime environment and defaults.")
    check.add_argument("--base-url", default="", help="Optional base URL override.")
    check.add_argument("--path-template", default="", help="Optional path template override.")
    check.add_argument("--timeout-seconds", type=int, default=None, help="Request timeout override.")
    check.add_argument("--max-retries", type=int, default=None, help="Retry count override.")
    check.add_argument("--retry-backoff-seconds", type=float, default=None, help="Initial retry delay override.")
    check.add_argument("--retry-backoff-multiplier", type=float, default=None, help="Retry multiplier override.")
    check.add_argument("--min-request-interval-seconds", type=float, default=None, help="Throttle interval override.")
    check.add_argument("--max-hours-per-run", type=int, default=None, help="Safety cap override.")
    check.add_argument("--max-files-per-run", type=int, default=None, help="Safety cap override.")
    check.add_argument("--max-rows-per-file", type=int, default=None, help="Safety cap override.")
    check.add_argument("--max-retry-after-seconds", type=int, default=None, help="Retry-After cap override.")
    check.add_argument("--user-agent", default=None, help="User-Agent override.")
    check.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    fetch = sub.add_parser("fetch", help="Fetch and filter AirNow Hourly AQ Obs files.")
    fetch.add_argument("--bbox", required=True, help="Bounding box: min_lon,min_lat,max_lon,max_lat.")
    fetch.add_argument("--start-datetime", required=True, help="UTC start datetime, ISO-8601 with timezone.")
    fetch.add_argument("--end-datetime", required=True, help="UTC end datetime, ISO-8601 with timezone.")
    fetch.add_argument(
        "--parameter",
        action="append",
        default=[],
        help=f"Requested parameter. Repeatable. Defaults to all supported: {', '.join(SUPPORTED_PARAMETERS)}.",
    )
    fetch.add_argument("--output", default="", help="Optional path for full JSON payload.")
    fetch.add_argument("--dry-run", action="store_true", help="Only return planned file URLs and request metadata.")
    fetch.add_argument("--fail-on-validation-error", action="store_true", help="Exit non-zero when validation is not clean.")
    fetch.add_argument("--max-validation-issues", type=int, default=DEFAULT_MAX_VALIDATION_ISSUES, help="Maximum validation issues stored in output.")
    fetch.add_argument("--base-url", default="", help="Optional base URL override.")
    fetch.add_argument("--path-template", default="", help="Optional path template override.")
    fetch.add_argument("--timeout-seconds", type=int, default=None, help="Request timeout override.")
    fetch.add_argument("--max-retries", type=int, default=None, help="Retry count override.")
    fetch.add_argument("--retry-backoff-seconds", type=float, default=None, help="Initial retry delay override.")
    fetch.add_argument("--retry-backoff-multiplier", type=float, default=None, help="Retry multiplier override.")
    fetch.add_argument("--min-request-interval-seconds", type=float, default=None, help="Throttle interval override.")
    fetch.add_argument("--max-hours-per-run", type=int, default=None, help="Safety cap override.")
    fetch.add_argument("--max-files-per-run", type=int, default=None, help="Safety cap override.")
    fetch.add_argument("--max-rows-per-file", type=int, default=None, help="Safety cap override.")
    fetch.add_argument("--max-retry-after-seconds", type=int, default=None, help="Retry-After cap override.")
    fetch.add_argument("--user-agent", default=None, help="User-Agent override.")
    fetch.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"), help="Log verbosity.")
    fetch.add_argument("--log-file", default="", help="Optional log file path.")
    fetch.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def configure_logging(level: str, log_file: str) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file:
        path = Path(log_file).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(path, encoding="utf-8"))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=handlers,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if hasattr(args, "log_level"):
        configure_logging(args.log_level, getattr(args, "log_file", ""))
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    try:
        if args.command == "check-config":
            result = command_check_config(args)
        elif args.command == "fetch":
            result = command_fetch(args)
        else:
            raise ValueError(f"Unsupported command: {args.command}")
    except Exception as exc:  # noqa: BLE001
        error = {
            "command": args.command,
            "ok": False,
            "error": str(exc),
            "error_type": type(exc).__name__,
        }
        print(serialize_json(error, pretty=getattr(args, "pretty", False)))
        return 1

    print(serialize_json(result, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
