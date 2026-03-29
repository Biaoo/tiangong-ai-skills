#!/usr/bin/env python3
"""Fetch USGS Water Services Instantaneous Values JSON with validation and logs."""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

ENV_BASE_URL = "USGS_WATER_IV_BASE_URL"
ENV_TIMEOUT_SECONDS = "USGS_WATER_IV_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "USGS_WATER_IV_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "USGS_WATER_IV_RETRY_BACKOFF_SECONDS"
ENV_RETRY_BACKOFF_MULTIPLIER = "USGS_WATER_IV_RETRY_BACKOFF_MULTIPLIER"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "USGS_WATER_IV_MIN_REQUEST_INTERVAL_SECONDS"
ENV_MAX_PARAMETER_CODES_PER_RUN = "USGS_WATER_IV_MAX_PARAMETER_CODES_PER_RUN"
ENV_MAX_SITES_PER_RUN = "USGS_WATER_IV_MAX_SITES_PER_RUN"
ENV_MAX_TIME_SERIES_PER_RUN = "USGS_WATER_IV_MAX_TIME_SERIES_PER_RUN"
ENV_MAX_VALUES_PER_SERIES = "USGS_WATER_IV_MAX_VALUES_PER_SERIES"
ENV_MAX_RESPONSE_BYTES = "USGS_WATER_IV_MAX_RESPONSE_BYTES"
ENV_MAX_RETRY_AFTER_SECONDS = "USGS_WATER_IV_MAX_RETRY_AFTER_SECONDS"
ENV_DEFAULT_SITE_TYPE = "USGS_WATER_IV_DEFAULT_SITE_TYPE"
ENV_DEFAULT_SITE_STATUS = "USGS_WATER_IV_DEFAULT_SITE_STATUS"
ENV_USER_AGENT = "USGS_WATER_IV_USER_AGENT"

DEFAULT_BASE_URL = "https://waterservices.usgs.gov/nwis/iv/"
DEFAULT_TIMEOUT_SECONDS = 45
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.4
DEFAULT_MAX_PARAMETER_CODES_PER_RUN = 8
DEFAULT_MAX_SITES_PER_RUN = 200
DEFAULT_MAX_TIME_SERIES_PER_RUN = 500
DEFAULT_MAX_VALUES_PER_SERIES = 10000
DEFAULT_MAX_RESPONSE_BYTES = 25_000_000
DEFAULT_MAX_RETRY_AFTER_SECONDS = 120
DEFAULT_SITE_TYPE = "ST"
DEFAULT_SITE_STATUS = "active"
DEFAULT_USER_AGENT = "usgs-water-iv-fetch/1.0"
DEFAULT_PARAMETER_CODES = ("00060", "00065")
SCHEMA_VERSION = "1.0.0"

RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}
SITE_STATUS_CHOICES = ("all", "active", "inactive")
MAX_VALIDATION_ISSUES = 50


@dataclass(frozen=True)
class BoundingBox:
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    def to_text(self) -> str:
        return f"{self.min_lon:.6f},{self.min_lat:.6f},{self.max_lon:.6f},{self.max_lat:.6f}"


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    retry_backoff_multiplier: float
    min_request_interval_seconds: float
    max_parameter_codes_per_run: int
    max_sites_per_run: int
    max_time_series_per_run: int
    max_values_per_series: int
    max_response_bytes: int
    max_retry_after_seconds: int
    default_site_type: str
    default_site_status: str
    user_agent: str


@dataclass(frozen=True)
class RequestSpec:
    bbox: BoundingBox | None
    sites: list[str]
    start_datetime_utc: str
    end_datetime_utc: str
    period: str
    parameter_codes: list[str]
    site_type: str
    site_status: str
    agency_code: str

    @property
    def selection_mode(self) -> str:
        return "bbox" if self.bbox is not None else "sites"

    @property
    def has_explicit_window(self) -> bool:
        return bool(self.start_datetime_utc and self.end_datetime_utc)


@dataclass(frozen=True)
class HttpJsonResponse:
    url: str
    status_code: int
    headers: dict[str, str]
    payload: dict[str, Any] | list[Any]
    byte_length: int


@dataclass
class IssueCollector:
    max_issues: int
    total_count: int = 0
    issues: list[dict[str, Any]] = field(default_factory=list)

    def add(self, *, level: str, path: str, message: str, value: Any | None = None) -> None:
        self.total_count += 1
        if len(self.issues) >= self.max_issues:
            return
        issue = {"level": level, "path": path, "message": message}
        if value is not None:
            issue["value"] = value
        self.issues.append(issue)


def env_or_default(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


def parse_positive_int(name: str, raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got: {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got: {value}")
    return value


def parse_non_negative_int(name: str, raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got: {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got: {value}")
    return value


def parse_positive_float(name: str, raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got: {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got: {value}")
    return value


def normalize_space(value: str) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(str(value))


def maybe_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = maybe_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def pretty_json(data: Any, *, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def atomic_write_text_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    except Exception:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass
        raise


def write_json(path: Path, payload: Any, *, pretty: bool) -> None:
    atomic_write_text_file(path, pretty_json(payload, pretty=pretty) + "\n")


def normalize_base_url(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError("Base URL cannot be empty.")
    if normalized.startswith("file://"):
        return normalized
    normalized = normalized.rstrip("/")
    if not normalized.startswith("http://") and not normalized.startswith("https://"):
        raise ValueError(f"Base URL must start with http://, https://, or file://, got: {normalized!r}")
    return normalized


def normalize_site_type(value: str) -> str:
    normalized = maybe_text(value).upper()
    if not normalized:
        raise ValueError("site type cannot be empty.")
    return normalized


def normalize_site_status(value: str) -> str:
    normalized = maybe_text(value).casefold()
    if normalized not in SITE_STATUS_CHOICES:
        raise ValueError(f"site status must be one of {', '.join(SITE_STATUS_CHOICES)}, got: {value!r}")
    return normalized


def normalize_parameter_code(raw: str) -> str:
    text = maybe_text(raw)
    if len(text) != 5 or not text.isdigit():
        raise ValueError(f"Invalid USGS parameter code {raw!r}. Expected a 5-digit numeric code such as 00060.")
    return text


def unique_preserve_order(items: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = maybe_text(item)
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def parse_parameter_codes(values: list[str]) -> list[str]:
    selected = values or list(DEFAULT_PARAMETER_CODES)
    return unique_preserve_order([normalize_parameter_code(item) for item in selected])


def normalize_site_number(raw: str) -> str:
    text = maybe_text(raw)
    if not text or not text.isdigit():
        raise ValueError(f"Invalid USGS site number {raw!r}.")
    return text


def parse_site_numbers(values: list[str]) -> list[str]:
    return unique_preserve_order([normalize_site_number(item) for item in values])


def parse_bbox(raw: str) -> BoundingBox:
    parts = [item.strip() for item in raw.split(",")]
    if len(parts) != 4:
        raise ValueError(
            f"Invalid --bbox value {raw!r}. Use min_lon,min_lat,max_lon,max_lat format."
        )
    try:
        min_lon, min_lat, max_lon, max_lat = (float(item) for item in parts)
    except ValueError as exc:
        raise ValueError(f"Invalid --bbox value {raw!r}. All coordinates must be numeric.") from exc
    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
        raise ValueError("Longitude values must be between -180 and 180.")
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError("Latitude values must be between -90 and 90.")
    if min_lon >= max_lon:
        raise ValueError("bbox min_lon must be strictly less than max_lon.")
    if min_lat >= max_lat:
        raise ValueError("bbox min_lat must be strictly less than max_lat.")
    return BoundingBox(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat)


def parse_rfc3339_datetime(raw: str, *, field_name: str) -> datetime:
    text = maybe_text(raw)
    if not text:
        raise ValueError(f"{field_name} cannot be empty.")
    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be ISO-8601 with timezone, got: {raw!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone, got: {raw!r}")
    return parsed.astimezone(timezone.utc)


def to_rfc3339_z(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_usgs_value_datetime(raw: Any) -> datetime | None:
    text = maybe_text(raw)
    if not text:
        return None
    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def normalize_period(raw: str) -> str:
    text = maybe_text(raw)
    if not text:
        raise ValueError("period cannot be empty.")
    if not text.upper().startswith("P"):
        raise ValueError(f"period must be an ISO-8601 duration such as P1D, got: {raw!r}")
    return text


def parse_retry_after_seconds(value: str) -> int | None:
    text = maybe_text(value)
    if not text:
        return None
    if text.isdigit():
        return int(text)
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    now = datetime.now(timezone.utc)
    delta = (parsed.astimezone(timezone.utc) - now).total_seconds()
    return max(0, int(delta))


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    return RuntimeConfig(
        base_url=normalize_base_url(args.base_url if args.base_url else env_or_default(ENV_BASE_URL, DEFAULT_BASE_URL)),
        timeout_seconds=parse_positive_int(
            ENV_TIMEOUT_SECONDS,
            str(args.timeout_seconds if args.timeout_seconds is not None else env_or_default(ENV_TIMEOUT_SECONDS, str(DEFAULT_TIMEOUT_SECONDS))),
        ),
        max_retries=parse_non_negative_int(
            ENV_MAX_RETRIES,
            str(args.max_retries if args.max_retries is not None else env_or_default(ENV_MAX_RETRIES, str(DEFAULT_MAX_RETRIES))),
        ),
        retry_backoff_seconds=parse_positive_float(
            ENV_RETRY_BACKOFF_SECONDS,
            str(
                args.retry_backoff_seconds
                if args.retry_backoff_seconds is not None
                else env_or_default(ENV_RETRY_BACKOFF_SECONDS, str(DEFAULT_RETRY_BACKOFF_SECONDS))
            ),
        ),
        retry_backoff_multiplier=parse_positive_float(
            ENV_RETRY_BACKOFF_MULTIPLIER,
            str(
                args.retry_backoff_multiplier
                if args.retry_backoff_multiplier is not None
                else env_or_default(ENV_RETRY_BACKOFF_MULTIPLIER, str(DEFAULT_RETRY_BACKOFF_MULTIPLIER))
            ),
        ),
        min_request_interval_seconds=parse_positive_float(
            ENV_MIN_REQUEST_INTERVAL_SECONDS,
            str(
                args.min_request_interval_seconds
                if args.min_request_interval_seconds is not None
                else env_or_default(ENV_MIN_REQUEST_INTERVAL_SECONDS, str(DEFAULT_MIN_REQUEST_INTERVAL_SECONDS))
            ),
        ),
        max_parameter_codes_per_run=parse_positive_int(
            ENV_MAX_PARAMETER_CODES_PER_RUN,
            str(
                args.max_parameter_codes_per_run
                if args.max_parameter_codes_per_run is not None
                else env_or_default(ENV_MAX_PARAMETER_CODES_PER_RUN, str(DEFAULT_MAX_PARAMETER_CODES_PER_RUN))
            ),
        ),
        max_sites_per_run=parse_positive_int(
            ENV_MAX_SITES_PER_RUN,
            str(args.max_sites_per_run if args.max_sites_per_run is not None else env_or_default(ENV_MAX_SITES_PER_RUN, str(DEFAULT_MAX_SITES_PER_RUN))),
        ),
        max_time_series_per_run=parse_positive_int(
            ENV_MAX_TIME_SERIES_PER_RUN,
            str(
                args.max_time_series_per_run
                if args.max_time_series_per_run is not None
                else env_or_default(ENV_MAX_TIME_SERIES_PER_RUN, str(DEFAULT_MAX_TIME_SERIES_PER_RUN))
            ),
        ),
        max_values_per_series=parse_positive_int(
            ENV_MAX_VALUES_PER_SERIES,
            str(
                args.max_values_per_series
                if args.max_values_per_series is not None
                else env_or_default(ENV_MAX_VALUES_PER_SERIES, str(DEFAULT_MAX_VALUES_PER_SERIES))
            ),
        ),
        max_response_bytes=parse_positive_int(
            ENV_MAX_RESPONSE_BYTES,
            str(
                args.max_response_bytes
                if args.max_response_bytes is not None
                else env_or_default(ENV_MAX_RESPONSE_BYTES, str(DEFAULT_MAX_RESPONSE_BYTES))
            ),
        ),
        max_retry_after_seconds=parse_positive_int(
            ENV_MAX_RETRY_AFTER_SECONDS,
            str(
                args.max_retry_after_seconds
                if args.max_retry_after_seconds is not None
                else env_or_default(ENV_MAX_RETRY_AFTER_SECONDS, str(DEFAULT_MAX_RETRY_AFTER_SECONDS))
            ),
        ),
        default_site_type=normalize_site_type(
            args.default_site_type if args.default_site_type else env_or_default(ENV_DEFAULT_SITE_TYPE, DEFAULT_SITE_TYPE)
        ),
        default_site_status=normalize_site_status(
            args.default_site_status if args.default_site_status else env_or_default(ENV_DEFAULT_SITE_STATUS, DEFAULT_SITE_STATUS)
        ),
        user_agent=maybe_text(args.user_agent if args.user_agent else env_or_default(ENV_USER_AGENT, DEFAULT_USER_AGENT)),
    )


def configure_logging(level: str, log_file: str) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file:
        log_path = Path(log_file).expanduser().resolve()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=handlers,
    )


def validate_request_spec(spec: RequestSpec, config: RuntimeConfig) -> None:
    if spec.bbox is None and not spec.sites:
        raise ValueError("Either --bbox or at least one --site is required.")
    if spec.bbox is not None and spec.sites:
        raise ValueError("Use either --bbox or --site, not both.")
    if spec.has_explicit_window == bool(spec.period):
        raise ValueError("Use either --period or both --start-datetime and --end-datetime.")
    if spec.has_explicit_window:
        start_dt = parse_rfc3339_datetime(spec.start_datetime_utc, field_name="start datetime")
        end_dt = parse_rfc3339_datetime(spec.end_datetime_utc, field_name="end datetime")
        if start_dt > end_dt:
            raise ValueError("start datetime must be <= end datetime.")
    if len(spec.parameter_codes) > config.max_parameter_codes_per_run:
        raise ValueError(
            f"Too many parameter codes: {len(spec.parameter_codes)} > {config.max_parameter_codes_per_run}"
        )
    if len(spec.sites) > config.max_sites_per_run:
        raise ValueError(f"Too many site numbers: {len(spec.sites)} > {config.max_sites_per_run}")


def build_request_spec(args: argparse.Namespace, config: RuntimeConfig) -> RequestSpec:
    bbox = parse_bbox(args.bbox) if args.bbox else None
    sites = parse_site_numbers(args.site or [])
    parameter_codes = parse_parameter_codes(args.parameter_code or [])
    period = normalize_period(args.period) if args.period else ""
    start_datetime_utc = ""
    end_datetime_utc = ""
    if args.start_datetime or args.end_datetime:
        if not args.start_datetime or not args.end_datetime:
            raise ValueError("--start-datetime and --end-datetime must be provided together.")
        start_datetime_utc = to_rfc3339_z(parse_rfc3339_datetime(args.start_datetime, field_name="start datetime"))
        end_datetime_utc = to_rfc3339_z(parse_rfc3339_datetime(args.end_datetime, field_name="end datetime"))
    spec = RequestSpec(
        bbox=bbox,
        sites=sites,
        start_datetime_utc=start_datetime_utc,
        end_datetime_utc=end_datetime_utc,
        period=period,
        parameter_codes=parameter_codes,
        site_type=normalize_site_type(args.site_type if args.site_type else config.default_site_type),
        site_status=normalize_site_status(args.site_status if args.site_status else config.default_site_status),
        agency_code=maybe_text(args.agency_code).upper(),
    )
    validate_request_spec(spec, config)
    return spec


def build_query_params(spec: RequestSpec) -> list[tuple[str, str]]:
    params: list[tuple[str, str]] = [("format", "json")]
    if spec.bbox is not None:
        params.append(("bBox", spec.bbox.to_text()))
    elif spec.sites:
        params.append(("sites", ",".join(spec.sites)))
    params.append(("parameterCd", ",".join(spec.parameter_codes)))
    if spec.has_explicit_window:
        params.append(("startDT", spec.start_datetime_utc))
        params.append(("endDT", spec.end_datetime_utc))
    else:
        params.append(("period", spec.period))
    if spec.site_type:
        params.append(("siteType", spec.site_type))
    if spec.site_status:
        params.append(("siteStatus", spec.site_status))
    if spec.agency_code:
        params.append(("agencyCd", spec.agency_code))
    return params


def build_fetch_url(base_url: str, params: list[tuple[str, str]]) -> str:
    if base_url.startswith("file://"):
        return base_url
    query = parse.urlencode(params)
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{query}"


def read_file_url(file_url: str) -> tuple[int, dict[str, str], bytes]:
    parsed = parse.urlparse(file_url)
    path = Path(parse.unquote(parsed.path)).expanduser().resolve()
    body = path.read_bytes()
    return 200, {"content-type": "application/json"}, body


def decode_response_body(headers: dict[str, str], body: bytes) -> bytes:
    content_encoding = maybe_text(headers.get("content-encoding")).casefold()
    if content_encoding == "gzip":
        return gzip.decompress(body)
    return body


def fetch_json(url: str, config: RuntimeConfig, logger: logging.Logger) -> HttpJsonResponse:
    last_attempt_started = 0.0
    for attempt in range(config.max_retries + 1):
        if attempt > 0:
            delay = config.retry_backoff_seconds * (config.retry_backoff_multiplier ** (attempt - 1))
            logger.warning("Retrying USGS Water IV fetch after %.2fs (attempt %s/%s).", delay, attempt, config.max_retries)
            time.sleep(delay)
        now = time.monotonic()
        remaining = config.min_request_interval_seconds - (now - last_attempt_started)
        if remaining > 0:
            time.sleep(remaining)
        last_attempt_started = time.monotonic()
        try:
            if url.startswith("file://"):
                status_code, headers, body = read_file_url(url)
            else:
                req = request.Request(
                    url,
                    headers={
                        "User-Agent": config.user_agent,
                        "Accept": "application/json",
                        "Accept-Encoding": "gzip",
                    },
                )
                with request.urlopen(req, timeout=config.timeout_seconds) as response:
                    status_code = int(response.status)
                    headers = {str(key).lower(): str(value) for key, value in response.headers.items()}
                    body = response.read(config.max_response_bytes + 1)
            if len(body) > config.max_response_bytes:
                raise ValueError(
                    f"Response exceeded max_response_bytes={config.max_response_bytes}: {len(body)} bytes"
                )
            decoded = decode_response_body(headers, body)
            payload = json.loads(decoded.decode("utf-8"))
            if not isinstance(payload, (dict, list)):
                raise ValueError("USGS response must decode to a JSON object or list.")
            return HttpJsonResponse(
                url=url,
                status_code=status_code,
                headers=headers,
                payload=payload,
                byte_length=len(body),
            )
        except HTTPError as exc:
            status_code = int(exc.code)
            headers = {str(key).lower(): str(value) for key, value in exc.headers.items()} if exc.headers else {}
            retry_after = parse_retry_after_seconds(headers.get("retry-after", ""))
            if status_code in RETRIABLE_HTTP_CODES and attempt < config.max_retries:
                if retry_after is not None:
                    if retry_after > config.max_retry_after_seconds:
                        raise RuntimeError(
                            f"Retry-After {retry_after}s exceeds cap {config.max_retry_after_seconds}s."
                        ) from exc
                    logger.warning("Retrying after HTTP %s with Retry-After=%ss.", status_code, retry_after)
                    time.sleep(retry_after)
                continue
            raise RuntimeError(f"USGS Water IV request failed with HTTP {status_code}.") from exc
        except URLError as exc:
            if attempt < config.max_retries:
                continue
            raise RuntimeError(f"USGS Water IV request failed: {exc.reason}") from exc


def site_property_lookup(source_info: dict[str, Any]) -> dict[str, str]:
    values: dict[str, str] = {}
    properties = source_info.get("siteProperty")
    if not isinstance(properties, list):
        return values
    for item in properties:
        if not isinstance(item, dict):
            continue
        name = maybe_text(item.get("name"))
        value = maybe_text(item.get("value"))
        if name:
            values[name] = value
    return values


def first_site_code(source_info: dict[str, Any]) -> dict[str, Any]:
    codes = source_info.get("siteCode")
    if not isinstance(codes, list):
        return {}
    for item in codes:
        if isinstance(item, dict):
            return item
    return {}


def first_variable_code(variable: dict[str, Any]) -> dict[str, Any]:
    codes = variable.get("variableCode")
    if not isinstance(codes, list):
        return {}
    for item in codes:
        if isinstance(item, dict):
            return item
    return {}


def extract_records(
    payload: dict[str, Any] | list[Any],
    *,
    spec: RequestSpec,
    config: RuntimeConfig,
    request_url: str,
    issues: IssueCollector,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    if not isinstance(payload, dict):
        issues.add(level="error", path="$", message="Top-level USGS payload must be a JSON object.")
        return [], [], {}
    root = payload.get("value")
    if not isinstance(root, dict):
        issues.add(level="error", path="$.value", message="Expected WaterML-style object at $.value.")
        return [], [], {}
    query_info = root.get("queryInfo") if isinstance(root.get("queryInfo"), dict) else {}
    time_series = root.get("timeSeries")
    if not isinstance(time_series, list):
        issues.add(level="error", path="$.value.timeSeries", message="Expected list at $.value.timeSeries.")
        return [], [], query_info if isinstance(query_info, dict) else {}
    if len(time_series) > config.max_time_series_per_run:
        raise ValueError(
            f"Parsed timeSeries count {len(time_series)} exceeds cap {config.max_time_series_per_run}."
        )

    explicit_start = parse_rfc3339_datetime(spec.start_datetime_utc, field_name="start datetime") if spec.start_datetime_utc else None
    explicit_end = parse_rfc3339_datetime(spec.end_datetime_utc, field_name="end datetime") if spec.end_datetime_utc else None
    query_url = request_url

    records: list[dict[str, Any]] = []
    series_summaries: list[dict[str, Any]] = []
    for series_index, series in enumerate(time_series):
        path = f"$.value.timeSeries[{series_index}]"
        if not isinstance(series, dict):
            issues.add(level="error", path=path, message="timeSeries item must be an object.")
            continue
        source_info = series.get("sourceInfo") if isinstance(series.get("sourceInfo"), dict) else {}
        variable = series.get("variable") if isinstance(series.get("variable"), dict) else {}
        if not source_info:
            issues.add(level="warning", path=f"{path}.sourceInfo", message="Missing sourceInfo object.")
        if not variable:
            issues.add(level="warning", path=f"{path}.variable", message="Missing variable object.")

        site_code = first_site_code(source_info)
        variable_code = first_variable_code(variable)
        props = site_property_lookup(source_info)
        geog = (
            source_info.get("geoLocation", {}).get("geogLocation")
            if isinstance(source_info.get("geoLocation"), dict)
            else {}
        )
        latitude = maybe_number(geog.get("latitude")) if isinstance(geog, dict) else None
        longitude = maybe_number(geog.get("longitude")) if isinstance(geog, dict) else None
        site_number = maybe_text(site_code.get("value"))
        site_name = maybe_text(source_info.get("siteName"))
        agency_code = maybe_text(site_code.get("agencyCode"))
        parameter_code = maybe_text(variable_code.get("value"))
        variable_name = maybe_text(variable.get("variableName"))
        variable_description = maybe_text(variable.get("variableDescription"))
        unit = maybe_text((variable.get("unit") or {}).get("unitCode")) if isinstance(variable, dict) else ""
        no_data_value = maybe_number(variable.get("noDataValue"))
        method_items = variable.get("options", {}).get("option") if isinstance(variable.get("options"), dict) else []
        statistic_code = ""
        if isinstance(method_items, list):
            for item in method_items:
                if isinstance(item, dict) and maybe_text(item.get("name")) == "Statistic":
                    statistic_code = maybe_text(item.get("optionCode"))
                    break

        values_sections = series.get("values")
        if not isinstance(values_sections, list):
            issues.add(level="error", path=f"{path}.values", message="Expected list of values sections.")
            continue

        series_record_count = 0
        provisional_record_count = 0
        first_observed = ""
        last_observed = ""
        for section_index, section in enumerate(values_sections):
            section_path = f"{path}.values[{section_index}]"
            if not isinstance(section, dict):
                issues.add(level="error", path=section_path, message="values section must be an object.")
                continue
            raw_values = section.get("value")
            if not isinstance(raw_values, list):
                issues.add(level="error", path=f"{section_path}.value", message="Expected list of value rows.")
                continue
            if len(raw_values) > config.max_values_per_series:
                raise ValueError(
                    f"Series {site_number or '<unknown>'}/{parameter_code or '<unknown>'} exceeds "
                    f"max_values_per_series={config.max_values_per_series}."
                )
            for value_index, row in enumerate(raw_values):
                row_path = f"{section_path}.value[{value_index}]"
                if not isinstance(row, dict):
                    issues.add(level="warning", path=row_path, message="Value row must be an object.")
                    continue
                value_number = maybe_number(row.get("value"))
                if value_number is None:
                    issues.add(level="warning", path=f"{row_path}.value", message="Non-numeric USGS value skipped.", value=row.get("value"))
                    continue
                if no_data_value is not None and value_number == no_data_value:
                    continue
                observed_dt = parse_usgs_value_datetime(row.get("dateTime"))
                if observed_dt is None:
                    issues.add(level="warning", path=f"{row_path}.dateTime", message="Unparseable USGS datetime skipped.", value=row.get("dateTime"))
                    continue
                if explicit_start is not None and observed_dt < explicit_start:
                    continue
                if explicit_end is not None and observed_dt > explicit_end:
                    continue
                observed_at_utc = to_rfc3339_z(observed_dt)
                if not first_observed:
                    first_observed = observed_at_utc
                last_observed = observed_at_utc
                qualifiers = [maybe_text(item) for item in row.get("qualifiers", []) if maybe_text(item)] if isinstance(row.get("qualifiers"), list) else []
                provisional = any(item.casefold() == "p" for item in qualifiers)
                provisional_record_count += 1 if provisional else 0
                records.append(
                    {
                        "site_number": site_number,
                        "site_name": site_name,
                        "agency_code": agency_code,
                        "site_type": props.get("siteTypeCd"),
                        "state_code": props.get("stateCd"),
                        "county_code": props.get("countyCd"),
                        "huc_code": props.get("hucCd"),
                        "latitude": latitude,
                        "longitude": longitude,
                        "parameter_code": parameter_code,
                        "variable_name": variable_name,
                        "variable_description": variable_description,
                        "statistic_code": statistic_code,
                        "unit": unit,
                        "observed_at_utc": observed_at_utc,
                        "value": value_number,
                        "qualifiers": qualifiers,
                        "provisional": provisional,
                        "source_query_url": query_url,
                    }
                )
                series_record_count += 1
        series_summaries.append(
            {
                "site_number": site_number,
                "site_name": site_name,
                "agency_code": agency_code,
                "site_type": props.get("siteTypeCd"),
                "state_code": props.get("stateCd"),
                "county_code": props.get("countyCd"),
                "huc_code": props.get("hucCd"),
                "latitude": latitude,
                "longitude": longitude,
                "parameter_code": parameter_code,
                "variable_name": variable_name,
                "variable_description": variable_description,
                "statistic_code": statistic_code,
                "unit": unit,
                "record_count": series_record_count,
                "provisional_record_count": provisional_record_count,
                "first_observed_at_utc": first_observed,
                "last_observed_at_utc": last_observed,
            }
        )
    return records, series_summaries, query_info if isinstance(query_info, dict) else {}


def runtime_config_payload(config: RuntimeConfig) -> dict[str, Any]:
    return {
        "base_url": config.base_url,
        "timeout_seconds": config.timeout_seconds,
        "max_retries": config.max_retries,
        "retry_backoff_seconds": config.retry_backoff_seconds,
        "retry_backoff_multiplier": config.retry_backoff_multiplier,
        "min_request_interval_seconds": config.min_request_interval_seconds,
        "max_parameter_codes_per_run": config.max_parameter_codes_per_run,
        "max_sites_per_run": config.max_sites_per_run,
        "max_time_series_per_run": config.max_time_series_per_run,
        "max_values_per_series": config.max_values_per_series,
        "max_response_bytes": config.max_response_bytes,
        "max_retry_after_seconds": config.max_retry_after_seconds,
        "default_site_type": config.default_site_type,
        "default_site_status": config.default_site_status,
        "user_agent": config.user_agent,
        "api_key_required": False,
    }


def check_config(args: argparse.Namespace) -> dict[str, Any]:
    config = build_runtime_config(args)
    return {
        "command": "check-config",
        "ok": True,
        "payload": runtime_config_payload(config),
    }


def fetch_command(args: argparse.Namespace) -> dict[str, Any]:
    config = build_runtime_config(args)
    configure_logging(args.log_level, args.log_file)
    logger = logging.getLogger("usgs_water_iv_fetch")
    spec = build_request_spec(args, config)
    query_params = build_query_params(spec)
    fetch_url = build_fetch_url(config.base_url, query_params)

    request_payload = {
        "selection_mode": spec.selection_mode,
        "bbox": spec.bbox.to_text() if spec.bbox is not None else "",
        "sites": spec.sites,
        "start_datetime_utc": spec.start_datetime_utc,
        "end_datetime_utc": spec.end_datetime_utc,
        "period": spec.period,
        "parameter_codes": spec.parameter_codes,
        "site_type": spec.site_type,
        "site_status": spec.site_status,
        "agency_code": spec.agency_code,
        "base_url": config.base_url,
        "fetch_url": fetch_url,
    }
    if args.dry_run:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "source_skill": "usgs-water-iv-fetch",
            "generated_at_utc": to_rfc3339_z(datetime.now(timezone.utc)),
            "dry_run": True,
            "request": request_payload,
            "runtime_config": runtime_config_payload(config),
            "validation_summary": {
                "ok": True,
                "total_issue_count": 0,
                "issues": [],
            },
            "series_count": 0,
            "record_count": 0,
            "series": [],
            "records": [],
            "artifacts": {},
        }
        return {"command": "fetch", "ok": True, "payload": payload}

    logger.info("Fetching USGS Water IV: %s", fetch_url)
    response = fetch_json(fetch_url, config, logger)
    issues = IssueCollector(max_issues=args.max_validation_issues)
    records, series_summaries, query_info = extract_records(
        response.payload,
        spec=spec,
        config=config,
        request_url=fetch_url,
        issues=issues,
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source_skill": "usgs-water-iv-fetch",
        "generated_at_utc": to_rfc3339_z(datetime.now(timezone.utc)),
        "dry_run": False,
        "request": request_payload,
        "transport": {
            "status_code": response.status_code,
            "headers": response.headers,
            "byte_length": response.byte_length,
        },
        "query_info": query_info,
        "validation_summary": {
            "ok": issues.total_count == 0,
            "total_issue_count": issues.total_count,
            "issues": issues.issues,
        },
        "series_count": len(series_summaries),
        "record_count": len(records),
        "series": series_summaries,
        "records": records,
        "artifacts": {},
    }
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        payload["artifacts"] = {"full_payload_json": str(output_path)}
        write_json(output_path, payload, pretty=args.pretty)
    if args.fail_on_validation_error and issues.total_count > 0:
        raise RuntimeError(f"Validation reported {issues.total_count} issue(s).")
    return {"command": "fetch", "ok": True, "payload": payload}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch USGS Water Services Instantaneous Values JSON for one bbox or site list."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    check_config_parser = sub.add_parser("check-config", help="Show effective runtime configuration.")
    check_config_parser.add_argument("--base-url", default="", help="Optional base URL override.")
    check_config_parser.add_argument("--timeout-seconds", type=int, default=None, help="Timeout override.")
    check_config_parser.add_argument("--max-retries", type=int, default=None, help="Retry count override.")
    check_config_parser.add_argument("--retry-backoff-seconds", type=float, default=None, help="Initial retry delay override.")
    check_config_parser.add_argument("--retry-backoff-multiplier", type=float, default=None, help="Retry multiplier override.")
    check_config_parser.add_argument("--min-request-interval-seconds", type=float, default=None, help="Minimum interval override.")
    check_config_parser.add_argument("--max-parameter-codes-per-run", type=int, default=None, help="Safety cap override.")
    check_config_parser.add_argument("--max-sites-per-run", type=int, default=None, help="Safety cap override.")
    check_config_parser.add_argument("--max-time-series-per-run", type=int, default=None, help="Safety cap override.")
    check_config_parser.add_argument("--max-values-per-series", type=int, default=None, help="Safety cap override.")
    check_config_parser.add_argument("--max-response-bytes", type=int, default=None, help="Safety cap override.")
    check_config_parser.add_argument("--max-retry-after-seconds", type=int, default=None, help="Retry-After cap override.")
    check_config_parser.add_argument("--default-site-type", default="", help="Default site type override.")
    check_config_parser.add_argument("--default-site-status", default="", choices=SITE_STATUS_CHOICES, help="Default site status override.")
    check_config_parser.add_argument("--user-agent", default="", help="User-Agent override.")
    check_config_parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    fetch_parser = sub.add_parser("fetch", help="Fetch USGS Water Services IV records.")
    selector = fetch_parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--bbox", help="Bounding box: min_lon,min_lat,max_lon,max_lat.")
    selector.add_argument("--site", action="append", help="USGS site number. Repeatable.")
    fetch_parser.add_argument("--start-datetime", help="UTC start datetime, ISO-8601 with timezone.")
    fetch_parser.add_argument("--end-datetime", help="UTC end datetime, ISO-8601 with timezone.")
    fetch_parser.add_argument("--period", help="ISO-8601 duration such as P1D. Use instead of explicit start/end.")
    fetch_parser.add_argument(
        "--parameter-code",
        action="append",
        help="USGS parameter code. Repeatable. Defaults to 00060 and 00065.",
    )
    fetch_parser.add_argument("--site-type", default="", help="USGS site type filter, for example ST.")
    fetch_parser.add_argument(
        "--site-status",
        default="",
        choices=SITE_STATUS_CHOICES,
        help="USGS site activity filter.",
    )
    fetch_parser.add_argument("--agency-code", default="", help="Optional agency filter, for example USGS.")
    fetch_parser.add_argument("--output", default="", help="Optional path for full JSON payload.")
    fetch_parser.add_argument("--dry-run", action="store_true", help="Only return planned request metadata.")
    fetch_parser.add_argument("--fail-on-validation-error", action="store_true", help="Exit non-zero when validation is not clean.")
    fetch_parser.add_argument("--max-validation-issues", type=int, default=MAX_VALIDATION_ISSUES, help="Maximum validation issues stored in output.")
    fetch_parser.add_argument("--base-url", default="", help="Optional base URL override.")
    fetch_parser.add_argument("--timeout-seconds", type=int, default=None, help="Timeout override.")
    fetch_parser.add_argument("--max-retries", type=int, default=None, help="Retry count override.")
    fetch_parser.add_argument("--retry-backoff-seconds", type=float, default=None, help="Initial retry delay override.")
    fetch_parser.add_argument("--retry-backoff-multiplier", type=float, default=None, help="Retry multiplier override.")
    fetch_parser.add_argument("--min-request-interval-seconds", type=float, default=None, help="Throttle interval override.")
    fetch_parser.add_argument("--max-parameter-codes-per-run", type=int, default=None, help="Safety cap override.")
    fetch_parser.add_argument("--max-sites-per-run", type=int, default=None, help="Safety cap override.")
    fetch_parser.add_argument("--max-time-series-per-run", type=int, default=None, help="Safety cap override.")
    fetch_parser.add_argument("--max-values-per-series", type=int, default=None, help="Safety cap override.")
    fetch_parser.add_argument("--max-response-bytes", type=int, default=None, help="Safety cap override.")
    fetch_parser.add_argument("--max-retry-after-seconds", type=int, default=None, help="Retry-After cap override.")
    fetch_parser.add_argument("--default-site-type", default="", help="Default site type override.")
    fetch_parser.add_argument("--default-site-status", default="", choices=SITE_STATUS_CHOICES, help="Default site status override.")
    fetch_parser.add_argument("--user-agent", default="", help="User-Agent override.")
    fetch_parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO", help="Log verbosity.")
    fetch_parser.add_argument("--log-file", default="", help="Optional log file path.")
    fetch_parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "check-config":
            result = check_config(args)
        elif args.command == "fetch":
            result = fetch_command(args)
        else:
            raise ValueError(f"Unsupported command: {args.command}")
    except Exception as exc:  # noqa: BLE001
        error = {"command": args.command, "ok": False, "error": str(exc)}
        print(pretty_json(error, pretty=True), file=sys.stderr)
        return 1
    print(pretty_json(result, pretty=bool(getattr(args, "pretty", False))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
