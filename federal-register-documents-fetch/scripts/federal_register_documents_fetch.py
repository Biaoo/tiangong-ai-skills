#!/usr/bin/env python3
"""Fetch Federal Register documents with retry, throttling, and validation."""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import re
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

ENV_BASE_URL = "FEDERAL_REGISTER_BASE_URL"
ENV_TIMEOUT_SECONDS = "FEDERAL_REGISTER_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "FEDERAL_REGISTER_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "FEDERAL_REGISTER_RETRY_BACKOFF_SECONDS"
ENV_RETRY_BACKOFF_MULTIPLIER = "FEDERAL_REGISTER_RETRY_BACKOFF_MULTIPLIER"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "FEDERAL_REGISTER_MIN_REQUEST_INTERVAL_SECONDS"
ENV_PAGE_SIZE = "FEDERAL_REGISTER_PAGE_SIZE"
ENV_MAX_PAGES_PER_RUN = "FEDERAL_REGISTER_MAX_PAGES_PER_RUN"
ENV_MAX_RECORDS_PER_RUN = "FEDERAL_REGISTER_MAX_RECORDS_PER_RUN"
ENV_MAX_RESPONSE_BYTES = "FEDERAL_REGISTER_MAX_RESPONSE_BYTES"
ENV_MAX_RETRY_AFTER_SECONDS = "FEDERAL_REGISTER_MAX_RETRY_AFTER_SECONDS"
ENV_USER_AGENT = "FEDERAL_REGISTER_USER_AGENT"

DEFAULT_BASE_URL = "https://www.federalregister.gov/api/v1"
DEFAULT_TIMEOUT_SECONDS = 45
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.4
DEFAULT_PAGE_SIZE = 20
DEFAULT_MAX_PAGES_PER_RUN = 20
DEFAULT_MAX_RECORDS_PER_RUN = 500
DEFAULT_MAX_RESPONSE_BYTES = 25_000_000
DEFAULT_MAX_RETRY_AFTER_SECONDS = 120
DEFAULT_USER_AGENT = "federal-register-documents-fetch/1.0"

DOCUMENTS_PATH = "documents.json"
RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
YEAR_PATTERN = re.compile(r"^\d{4}$")
MAX_VALIDATION_ISSUES = 40
ORDER_CHOICES = ("relevance", "newest", "oldest", "executive_order_number")
DOCUMENT_TYPE_ALIASES = {
    "RULE": "RULE",
    "FINAL_RULE": "RULE",
    "FINAL-RULE": "RULE",
    "PRORULE": "PRORULE",
    "PROPOSED_RULE": "PRORULE",
    "PROPOSED-RULE": "PRORULE",
    "NOTICE": "NOTICE",
    "PRESDOCU": "PRESDOCU",
    "PRESIDENTIAL_DOCUMENT": "PRESDOCU",
    "PRESIDENTIAL-DOCUMENT": "PRESDOCU",
}


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    retry_backoff_multiplier: float
    min_request_interval_seconds: float
    page_size: int
    max_pages_per_run: int
    max_records_per_run: int
    max_response_bytes: int
    max_retry_after_seconds: int
    user_agent: str


@dataclass(frozen=True)
class RequestSpec:
    search_term: str
    publication_date_is: str
    publication_date_year: str
    publication_date_gte: str
    publication_date_lte: str
    agencies: list[str]
    document_types: list[str]
    topics: list[str]
    docket_id: str
    regulation_id_number: str
    sections: list[str]
    fields: list[str]
    order: str
    page_size: int
    max_pages: int
    max_records: int

    @property
    def has_any_filter(self) -> bool:
        return any(
            (
                self.search_term,
                self.publication_date_is,
                self.publication_date_year,
                self.publication_date_gte,
                self.publication_date_lte,
                self.agencies,
                self.document_types,
                self.topics,
                self.docket_id,
                self.regulation_id_number,
                self.sections,
            )
        )


@dataclass(frozen=True)
class HttpJsonResponse:
    url: str
    status_code: int
    headers: dict[str, str]
    payload: dict[str, Any]
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


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def unique_preserve_order(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = maybe_text(raw)
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def normalize_base_url(value: str) -> str:
    normalized = value.strip().rstrip("/")
    if not normalized:
        raise ValueError("Base URL cannot be empty.")
    if not normalized.startswith("http://") and not normalized.startswith("https://"):
        raise ValueError(f"Base URL must start with http:// or https://, got: {normalized!r}")
    return normalized


def normalize_date(value: str, *, field_name: str) -> str:
    text = maybe_text(value)
    if not DATE_PATTERN.match(text):
        raise ValueError(f"{field_name} must be YYYY-MM-DD, got: {value!r}")
    return text


def normalize_year(value: str, *, field_name: str) -> str:
    text = maybe_text(value)
    if not YEAR_PATTERN.match(text):
        raise ValueError(f"{field_name} must be YYYY, got: {value!r}")
    return text


def normalize_document_type(value: str) -> str:
    text = maybe_text(value).replace(" ", "_").replace("/", "_").upper()
    normalized = DOCUMENT_TYPE_ALIASES.get(text)
    if not normalized:
        allowed = ", ".join(sorted(set(DOCUMENT_TYPE_ALIASES.values())))
        raise ValueError(f"Unsupported document type {value!r}. Use one of: {allowed}")
    return normalized


def normalize_order(value: str) -> str:
    text = maybe_text(value).casefold()
    if text not in ORDER_CHOICES:
        raise ValueError(f"Unsupported order {value!r}. Use one of: {', '.join(ORDER_CHOICES)}")
    return text


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


def decode_response_body(headers: dict[str, str], body: bytes) -> bytes:
    if maybe_text(headers.get("content-encoding")).casefold() == "gzip":
        return gzip.decompress(body)
    return body


def error_excerpt(body: bytes) -> str:
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return ""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return text[:400]
    if isinstance(payload, dict):
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            first = errors[0]
            if isinstance(first, dict):
                return maybe_text(first.get("message") or first.get("error") or first.get("title"))[:400]
        return maybe_text(payload.get("message") or payload.get("error") or text)[:400]
    return text[:400]


def parse_retry_after_seconds(value: str | None) -> int | None:
    text = maybe_text(value)
    if not text:
        return None
    if text.isdigit():
        return int(text)
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta = (parsed.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds()
    return max(0, int(delta))


def runtime_config_payload(config: RuntimeConfig) -> dict[str, Any]:
    return {
        "api_key_required": False,
        "base_url": config.base_url,
        "timeout_seconds": config.timeout_seconds,
        "max_retries": config.max_retries,
        "retry_backoff_seconds": config.retry_backoff_seconds,
        "retry_backoff_multiplier": config.retry_backoff_multiplier,
        "min_request_interval_seconds": config.min_request_interval_seconds,
        "page_size": config.page_size,
        "max_pages_per_run": config.max_pages_per_run,
        "max_records_per_run": config.max_records_per_run,
        "max_response_bytes": config.max_response_bytes,
        "max_retry_after_seconds": config.max_retry_after_seconds,
        "user_agent": config.user_agent,
    }


class RetryableHttpClient:
    def __init__(self, config: RuntimeConfig, logger: logging.Logger) -> None:
        self._config = config
        self._logger = logger
        self._last_request_monotonic: float | None = None

    def _throttle(self) -> None:
        if self._last_request_monotonic is None:
            return
        elapsed = time.monotonic() - self._last_request_monotonic
        sleep_seconds = self._config.min_request_interval_seconds - elapsed
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    def get_json(self, url: str) -> HttpJsonResponse:
        attempts = self._config.max_retries + 1
        for attempt in range(1, attempts + 1):
            self._throttle()
            req = request.Request(url, method="GET")
            req.add_header("User-Agent", self._config.user_agent)
            req.add_header("Accept", "application/json")
            req.add_header("Accept-Encoding", "gzip")
            self._logger.info("http-get attempt=%d/%d url=%s", attempt, attempts, url)
            try:
                with request.urlopen(req, timeout=self._config.timeout_seconds) as resp:
                    raw_body = resp.read()
                    headers = {k.lower(): v for k, v in resp.headers.items()}
                    self._last_request_monotonic = time.monotonic()
                    body = decode_response_body(headers, raw_body)
                    if len(body) > self._config.max_response_bytes:
                        raise RuntimeError(
                            f"Response exceeded max_response_bytes={self._config.max_response_bytes} for {url}."
                        )
                    content_type = maybe_text(headers.get("content-type")).lower()
                    if "json" not in content_type:
                        raise RuntimeError(f"Unexpected content-type {content_type!r} for {url}.")
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise RuntimeError(f"Response body is not valid JSON for {url}: {exc}") from exc
                    if not isinstance(payload, dict):
                        raise RuntimeError(
                            f"Response JSON root must be object, got {type(payload).__name__}."
                        )
                    return HttpJsonResponse(
                        url=url,
                        status_code=int(getattr(resp, "status", 200)),
                        headers=headers,
                        payload=payload,
                        byte_length=len(body),
                    )
            except HTTPError as exc:
                self._last_request_monotonic = time.monotonic()
                headers = {k.lower(): v for k, v in exc.headers.items()}
                body = exc.read()
                if exc.code in RETRIABLE_HTTP_CODES and attempt < attempts:
                    retry_after = parse_retry_after_seconds(headers.get("retry-after"))
                    if retry_after is not None:
                        if retry_after > self._config.max_retry_after_seconds:
                            raise RuntimeError(
                                f"Retry-After {retry_after}s exceeds cap {self._config.max_retry_after_seconds}s."
                            ) from exc
                        delay = retry_after
                    else:
                        delay = self._config.retry_backoff_seconds * (
                            self._config.retry_backoff_multiplier ** (attempt - 1)
                        )
                    self._logger.warning(
                        "Retrying Federal Register fetch after %.2fs (attempt %d/%d, status=%s).",
                        delay,
                        attempt,
                        self._config.max_retries,
                        exc.code,
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(
                    f"Federal Register request failed with HTTP {exc.code}: {error_excerpt(body) or exc.reason}"
                ) from exc
            except URLError as exc:
                self._last_request_monotonic = time.monotonic()
                if attempt < attempts:
                    delay = self._config.retry_backoff_seconds * (
                        self._config.retry_backoff_multiplier ** (attempt - 1)
                    )
                    self._logger.warning(
                        "Retrying Federal Register fetch after %.2fs (attempt %d/%d).",
                        delay,
                        attempt,
                        self._config.max_retries,
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(f"Federal Register request failed: {exc.reason}") from exc
        raise RuntimeError("Federal Register request failed after retries.")


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
        page_size=parse_positive_int(
            ENV_PAGE_SIZE,
            str(args.page_size if args.page_size is not None else env_or_default(ENV_PAGE_SIZE, str(DEFAULT_PAGE_SIZE))),
        ),
        max_pages_per_run=parse_positive_int(
            ENV_MAX_PAGES_PER_RUN,
            str(
                args.max_pages_per_run
                if args.max_pages_per_run is not None
                else env_or_default(ENV_MAX_PAGES_PER_RUN, str(DEFAULT_MAX_PAGES_PER_RUN))
            ),
        ),
        max_records_per_run=parse_positive_int(
            ENV_MAX_RECORDS_PER_RUN,
            str(
                args.max_records_per_run
                if args.max_records_per_run is not None
                else env_or_default(ENV_MAX_RECORDS_PER_RUN, str(DEFAULT_MAX_RECORDS_PER_RUN))
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
        max_retry_after_seconds=parse_non_negative_int(
            ENV_MAX_RETRY_AFTER_SECONDS,
            str(
                args.max_retry_after_seconds
                if args.max_retry_after_seconds is not None
                else env_or_default(ENV_MAX_RETRY_AFTER_SECONDS, str(DEFAULT_MAX_RETRY_AFTER_SECONDS))
            ),
        ),
        user_agent=maybe_text(args.user_agent if args.user_agent else env_or_default(ENV_USER_AGENT, DEFAULT_USER_AGENT)),
    )


def build_request_spec(args: argparse.Namespace, config: RuntimeConfig) -> RequestSpec:
    publication_date_is = normalize_date(args.publication_date_is, field_name="--publication-date-is") if args.publication_date_is else ""
    publication_date_year = normalize_year(args.publication_date_year, field_name="--publication-date-year") if args.publication_date_year else ""
    publication_date_gte = normalize_date(args.start_date, field_name="--start-date") if args.start_date else ""
    publication_date_lte = normalize_date(args.end_date, field_name="--end-date") if args.end_date else ""
    spec = RequestSpec(
        search_term=maybe_text(args.search_term),
        publication_date_is=publication_date_is,
        publication_date_year=publication_date_year,
        publication_date_gte=publication_date_gte,
        publication_date_lte=publication_date_lte,
        agencies=unique_preserve_order(args.agency or []),
        document_types=unique_preserve_order([normalize_document_type(item) for item in (args.document_type or [])]),
        topics=unique_preserve_order(args.topic or []),
        docket_id=maybe_text(args.docket_id),
        regulation_id_number=maybe_text(args.regulation_id_number),
        sections=unique_preserve_order(args.section or []),
        fields=unique_preserve_order(args.field or []),
        order=normalize_order(args.order or "newest"),
        page_size=args.page_size if args.page_size is not None else config.page_size,
        max_pages=args.max_pages if args.max_pages is not None else config.max_pages_per_run,
        max_records=args.max_records if args.max_records is not None else config.max_records_per_run,
    )
    validate_request_spec(spec, config)
    return spec


def validate_request_spec(spec: RequestSpec, config: RuntimeConfig) -> None:
    if not spec.has_any_filter:
        raise ValueError(
            "At least one search filter is required. Use a term, publication-date filter, agency, topic, docket, RIN, or section."
        )
    if spec.publication_date_is and (
        spec.publication_date_year or spec.publication_date_gte or spec.publication_date_lte
    ):
        raise ValueError("--publication-date-is cannot be combined with --publication-date-year or --start-date/--end-date.")
    if spec.publication_date_year and (spec.publication_date_gte or spec.publication_date_lte):
        raise ValueError("--publication-date-year cannot be combined with --start-date or --end-date.")
    if spec.publication_date_gte and spec.publication_date_lte and spec.publication_date_gte > spec.publication_date_lte:
        raise ValueError("--start-date must be <= --end-date.")
    if spec.page_size < 1 or spec.page_size > 1000:
        raise ValueError(f"page size must be between 1 and 1000, got: {spec.page_size}")
    if spec.max_pages < 1 or spec.max_pages > config.max_pages_per_run:
        raise ValueError(f"max pages must be between 1 and {config.max_pages_per_run}, got: {spec.max_pages}")
    if spec.max_records < 1 or spec.max_records > config.max_records_per_run:
        raise ValueError(
            f"max records must be between 1 and {config.max_records_per_run}, got: {spec.max_records}"
        )


def build_query_params(spec: RequestSpec, *, page: int) -> list[tuple[str, str]]:
    params: list[tuple[str, str]] = [
        ("per_page", str(spec.page_size)),
        ("page", str(page)),
        ("order", spec.order),
    ]
    if spec.search_term:
        params.append(("conditions[term]", spec.search_term))
    if spec.publication_date_is:
        params.append(("conditions[publication_date][is]", spec.publication_date_is))
    if spec.publication_date_year:
        params.append(("conditions[publication_date][year]", spec.publication_date_year))
    if spec.publication_date_gte:
        params.append(("conditions[publication_date][gte]", spec.publication_date_gte))
    if spec.publication_date_lte:
        params.append(("conditions[publication_date][lte]", spec.publication_date_lte))
    for agency in spec.agencies:
        params.append(("conditions[agencies][]", agency))
    for document_type in spec.document_types:
        params.append(("conditions[type][]", document_type))
    for topic in spec.topics:
        params.append(("conditions[topics][]", topic))
    if spec.docket_id:
        params.append(("conditions[docket_id]", spec.docket_id))
    if spec.regulation_id_number:
        params.append(("conditions[regulation_id_number]", spec.regulation_id_number))
    for section in spec.sections:
        params.append(("conditions[sections][]", section))
    for field in spec.fields:
        params.append(("fields[]", field))
    return params


def build_fetch_url(base_url: str, params: list[tuple[str, str]]) -> str:
    query = parse.urlencode(params)
    return f"{base_url}/{DOCUMENTS_PATH}?{query}"


def resolve_next_url(base_url: str, next_page_url: str) -> str:
    return parse.urljoin(f"{base_url}/", next_page_url)


def validate_page_payload(payload: dict[str, Any], *, issues: IssueCollector, page_number: int) -> list[dict[str, Any]]:
    results = payload.get("results")
    if not isinstance(results, list):
        issues.add(level="error", path="$.results", message="Expected list at $.results.")
        return []
    if not isinstance(payload.get("count"), int):
        issues.add(level="warning", path="$.count", message="Expected integer count field.")
    if not isinstance(payload.get("total_pages"), int):
        issues.add(level="warning", path="$.total_pages", message="Expected integer total_pages field.")
    next_page_url = payload.get("next_page_url")
    if next_page_url is not None and not isinstance(next_page_url, str):
        issues.add(level="warning", path="$.next_page_url", message="Expected string or null next_page_url field.")
    validated: list[dict[str, Any]] = []
    for index, item in enumerate(results):
        if not isinstance(item, dict):
            issues.add(
                level="warning",
                path=f"$.results[{index}]",
                message=f"Skipped non-object result on page {page_number}.",
            )
            continue
        if not maybe_text(item.get("document_number")):
            issues.add(
                level="warning",
                path=f"$.results[{index}].document_number",
                message="Missing document_number.",
            )
        if not maybe_text(item.get("title")):
            issues.add(level="warning", path=f"$.results[{index}].title", message="Missing title.")
        if maybe_text(item.get("publication_date")) and not DATE_PATTERN.match(maybe_text(item.get("publication_date"))):
            issues.add(
                level="warning",
                path=f"$.results[{index}].publication_date",
                message="publication_date is not YYYY-MM-DD.",
                value=item.get("publication_date"),
            )
        validated.append(item)
    return validated


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


def check_config(args: argparse.Namespace) -> dict[str, Any]:
    config = build_runtime_config(args)
    return {"command": "check-config", "ok": True, "payload": runtime_config_payload(config)}


def fetch_command(args: argparse.Namespace) -> dict[str, Any]:
    config = build_runtime_config(args)
    configure_logging(args.log_level, args.log_file)
    logger = logging.getLogger("federal_register_documents_fetch")
    spec = build_request_spec(args, config)
    initial_url = build_fetch_url(config.base_url, build_query_params(spec, page=1))
    request_payload = {
        "base_url": config.base_url,
        "fetch_url": initial_url,
        "search_term": spec.search_term,
        "publication_date_is": spec.publication_date_is,
        "publication_date_year": spec.publication_date_year,
        "start_date": spec.publication_date_gte,
        "end_date": spec.publication_date_lte,
        "agencies": spec.agencies,
        "document_types": spec.document_types,
        "topics": spec.topics,
        "docket_id": spec.docket_id,
        "regulation_id_number": spec.regulation_id_number,
        "sections": spec.sections,
        "fields": spec.fields,
        "order": spec.order,
        "page_size": spec.page_size,
        "max_pages": spec.max_pages,
        "max_records": spec.max_records,
    }
    if args.dry_run:
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "dry_run": True,
            "request": request_payload,
            "runtime_config": runtime_config_payload(config),
        }
        return {"command": "fetch", "ok": True, "payload": payload}

    issues = IssueCollector(max_issues=args.max_validation_issues)
    client = RetryableHttpClient(config, logger)
    results: list[dict[str, Any]] = []
    page_summaries: list[dict[str, Any]] = []
    next_url = initial_url
    page_number = 1
    stop_reason = "no-results"
    search_description = ""
    reported_count: int | None = None
    reported_total_pages: int | None = None
    last_response_headers: dict[str, str] = {}
    last_status_code = 0

    while next_url:
        response = client.get_json(next_url)
        last_response_headers = response.headers
        last_status_code = response.status_code
        payload = response.payload
        if not search_description:
            search_description = maybe_text(payload.get("description"))
        if reported_count is None and isinstance(payload.get("count"), int):
            reported_count = int(payload.get("count"))
        if reported_total_pages is None and isinstance(payload.get("total_pages"), int):
            reported_total_pages = int(payload.get("total_pages"))

        page_results = validate_page_payload(payload, issues=issues, page_number=page_number)
        remaining = spec.max_records - len(results)
        kept = page_results[:remaining]
        results.extend(kept)
        next_page_url = maybe_text(payload.get("next_page_url"))
        page_summaries.append(
            {
                "page": page_number,
                "url": response.url,
                "status_code": response.status_code,
                "byte_length": response.byte_length,
                "result_count": len(page_results),
                "kept_count": len(kept),
                "next_page_url": next_page_url,
            }
        )
        if len(results) >= spec.max_records:
            stop_reason = "max-records-reached"
            break
        if page_number >= spec.max_pages:
            stop_reason = "max-pages-reached"
            break
        if not next_page_url:
            stop_reason = "next-page-absent"
            break
        page_number += 1
        next_url = resolve_next_url(config.base_url, next_page_url)

    if not results and stop_reason == "next-page-absent":
        stop_reason = "empty-result-set"

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "dry_run": False,
        "request": request_payload,
        "transport": {
            "status_code": last_status_code,
            "headers": last_response_headers,
            "pages_fetched": len(page_summaries),
        },
        "search_metadata": {
            "description": search_description,
            "reported_count": reported_count,
            "reported_total_pages": reported_total_pages,
        },
        "stop_reason": stop_reason,
        "page_summaries": page_summaries,
        "result_count": len(results),
        "results": results,
        "validation_summary": {
            "ok": issues.total_count == 0,
            "total_issue_count": issues.total_count,
            "issues": issues.issues,
        },
        "artifacts": {},
    }
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        write_json(output_path, payload, pretty=args.pretty)
        payload["artifacts"] = {"full_payload_json": str(output_path)}
    if args.fail_on_validation_error and issues.total_count > 0:
        raise RuntimeError(f"Validation reported {issues.total_count} issue(s).")
    return {"command": "fetch", "ok": True, "payload": payload}


def add_runtime_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-url", default="", help="Optional base URL override.")
    parser.add_argument("--timeout-seconds", type=int, default=None, help="Timeout override.")
    parser.add_argument("--max-retries", type=int, default=None, help="Retry count override.")
    parser.add_argument("--retry-backoff-seconds", type=float, default=None, help="Initial retry delay override.")
    parser.add_argument("--retry-backoff-multiplier", type=float, default=None, help="Retry multiplier override.")
    parser.add_argument("--min-request-interval-seconds", type=float, default=None, help="Throttle interval override.")
    parser.add_argument("--page-size", type=int, default=None, help="Page size override.")
    parser.add_argument("--max-pages-per-run", type=int, default=None, help="Configured max-pages safety cap override.")
    parser.add_argument("--max-records-per-run", type=int, default=None, help="Configured max-records safety cap override.")
    parser.add_argument("--max-response-bytes", type=int, default=None, help="Safety cap override.")
    parser.add_argument("--max-retry-after-seconds", type=int, default=None, help="Retry-After cap override.")
    parser.add_argument("--user-agent", default="", help="User-Agent override.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch Federal Register documents with search filters and pagination.")
    sub = parser.add_subparsers(dest="command", required=True)

    check_config_parser = sub.add_parser("check-config", help="Show effective runtime configuration.")
    add_runtime_args(check_config_parser)
    check_config_parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    fetch_parser = sub.add_parser("fetch", help="Fetch Federal Register document search results.")
    fetch_parser.add_argument("--search-term", default="", help="Full-text Federal Register search term.")
    fetch_parser.add_argument("--publication-date-is", default="", help="Exact publication date, YYYY-MM-DD.")
    fetch_parser.add_argument("--publication-date-year", default="", help="Publication year, YYYY.")
    fetch_parser.add_argument("--start-date", default="", help="Publication date lower bound, YYYY-MM-DD.")
    fetch_parser.add_argument("--end-date", default="", help="Publication date upper bound, YYYY-MM-DD.")
    fetch_parser.add_argument("--agency", action="append", help="Agency slug. Repeatable.")
    fetch_parser.add_argument("--document-type", action="append", help="Document type code. Repeatable.")
    fetch_parser.add_argument("--topic", action="append", help="Topic slug. Repeatable.")
    fetch_parser.add_argument("--docket-id", default="", help="Optional docket ID filter.")
    fetch_parser.add_argument("--regulation-id-number", default="", help="Optional Regulation ID Number filter.")
    fetch_parser.add_argument("--section", action="append", help="Section slug. Repeatable.")
    fetch_parser.add_argument("--field", action="append", help="Optional Federal Register fields[] projection. Repeatable.")
    fetch_parser.add_argument("--order", default="newest", choices=ORDER_CHOICES, help="Result order.")
    fetch_parser.add_argument("--max-pages", type=int, default=None, help="Maximum pages to fetch.")
    fetch_parser.add_argument("--max-records", type=int, default=None, help="Maximum results to keep.")
    fetch_parser.add_argument("--output", default="", help="Optional output path for full JSON payload.")
    fetch_parser.add_argument("--dry-run", action="store_true", help="Only return planned request metadata.")
    fetch_parser.add_argument("--fail-on-validation-error", action="store_true", help="Exit non-zero when validation is not clean.")
    fetch_parser.add_argument("--max-validation-issues", type=int, default=MAX_VALIDATION_ISSUES, help="Maximum validation issues stored in output.")
    add_runtime_args(fetch_parser)
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
