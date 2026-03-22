#!/usr/bin/env python3
"""Validate and scaffold shared data contracts for the eco council."""

from __future__ import annotations

import argparse
import copy
import json
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
ASSETS_DIR = SKILL_DIR / "assets"
EXAMPLES_DIR = ASSETS_DIR / "examples"
DDL_PATH = ASSETS_DIR / "sqlite" / "eco_council.sql"
SCHEMA_PATH = ASSETS_DIR / "schemas" / "eco_council.schema.json"

SCHEMA_VERSION = "1.0.0"
ISO_UTC_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
SHA256_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")
ROUND_ID_PATTERN = re.compile(r"^round-\d{3}$")
ROUND_DIR_PATTERN = re.compile(r"^round_(\d{3})$")

OBJECT_KINDS = (
    "mission",
    "round-task",
    "source-selection",
    "claim",
    "observation",
    "evidence-card",
    "expert-report",
    "council-decision",
)

AGENT_ROLES = {"moderator", "sociologist", "environmentalist", "historian"}
TASK_STATUSES = {"planned", "in_progress", "completed", "blocked"}
CLAIM_TYPES = {
    "wildfire",
    "smoke",
    "flood",
    "heat",
    "drought",
    "air-pollution",
    "water-pollution",
    "policy-reaction",
    "other",
}
CLAIM_STATUSES = {"candidate", "selected", "dismissed", "validated"}
OBSERVATION_AGGREGATIONS = {"point", "window-summary", "series-summary", "event-count"}
EVIDENCE_VERDICTS = {"supports", "contradicts", "mixed", "insufficient"}
CONFIDENCE_VALUES = {"low", "medium", "high"}
REPORT_STATUSES = {"complete", "needs-more-evidence", "blocked"}
MODERATOR_STATUSES = {"continue", "complete", "blocked"}
EVIDENCE_SUFFICIENCY = {"sufficient", "partial", "insufficient"}
SOURCE_SELECTION_ROLES = {"sociologist", "environmentalist", "historian"}
SOURCE_SELECTION_STATUSES = {"pending", "complete", "blocked"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty_json(data: Any, *, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any, *, pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write(pretty_json(payload, pretty=pretty))
        handle.write("\n")


def parse_utc_datetime(value: str) -> datetime | None:
    if not isinstance(value, str) or not ISO_UTC_PATTERN.match(value):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def is_int_not_bool(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


@dataclass
class IssueCollector:
    issues: list[dict[str, Any]] = field(default_factory=list)

    def add(self, path: str, message: str, *, actual: Any | None = None) -> None:
        issue: dict[str, Any] = {"path": path, "message": message}
        if actual is not None:
            issue["actual"] = actual
        self.issues.append(issue)

    @property
    def ok(self) -> bool:
        return not self.issues

    def summary(self) -> dict[str, Any]:
        return {"ok": self.ok, "issue_count": len(self.issues), "issues": self.issues}


def require_object(value: Any, path: str, issues: IssueCollector) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        issues.add(path, "Expected an object.", actual=type(value).__name__)
        return None
    return value


def require_string(
    obj: dict[str, Any],
    key: str,
    path: str,
    issues: IssueCollector,
    *,
    allow_empty: bool = False,
) -> str | None:
    value = obj.get(key)
    field_path = f"{path}.{key}"
    if not isinstance(value, str):
        issues.add(field_path, "Expected a string.", actual=value)
        return None
    if not allow_empty and not value.strip():
        issues.add(field_path, "String must not be empty.")
        return None
    return value


def require_bool(obj: dict[str, Any], key: str, path: str, issues: IssueCollector) -> bool | None:
    value = obj.get(key)
    if not isinstance(value, bool):
        issues.add(f"{path}.{key}", "Expected a boolean.", actual=value)
        return None
    return value


def require_int(
    obj: dict[str, Any],
    key: str,
    path: str,
    issues: IssueCollector,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    value = obj.get(key)
    field_path = f"{path}.{key}"
    if not is_int_not_bool(value):
        issues.add(field_path, "Expected an integer.", actual=value)
        return None
    if minimum is not None and value < minimum:
        issues.add(field_path, f"Value must be >= {minimum}.", actual=value)
    if maximum is not None and value > maximum:
        issues.add(field_path, f"Value must be <= {maximum}.", actual=value)
    return value


def require_number(
    obj: dict[str, Any],
    key: str,
    path: str,
    issues: IssueCollector,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float | None:
    value = obj.get(key)
    field_path = f"{path}.{key}"
    if not is_number(value):
        issues.add(field_path, "Expected a number.", actual=value)
        return None
    value_float = float(value)
    if minimum is not None and value_float < minimum:
        issues.add(field_path, f"Value must be >= {minimum}.", actual=value)
    if maximum is not None and value_float > maximum:
        issues.add(field_path, f"Value must be <= {maximum}.", actual=value)
    return value_float


def require_enum(
    obj: dict[str, Any],
    key: str,
    path: str,
    issues: IssueCollector,
    *,
    allowed: set[str],
) -> str | None:
    value = require_string(obj, key, path, issues)
    if value is None:
        return None
    if value not in allowed:
        issues.add(f"{path}.{key}", f"Expected one of {sorted(allowed)}.", actual=value)
        return None
    return value


def validate_string_list(
    value: Any,
    path: str,
    issues: IssueCollector,
    *,
    allow_empty: bool = True,
) -> list[str]:
    if not isinstance(value, list):
        issues.add(path, "Expected a list.", actual=type(value).__name__)
        return []
    result: list[str] = []
    if not allow_empty and not value:
        issues.add(path, "List must not be empty.")
    for index, item in enumerate(value):
        item_path = f"{path}[{index}]"
        if not isinstance(item, str) or not item.strip():
            issues.add(item_path, "Expected a non-empty string.", actual=item)
            continue
        result.append(item)
    return result


def validate_unique_strings(values: list[str], path: str, issues: IssueCollector) -> None:
    seen: set[str] = set()
    for index, item in enumerate(values):
        key = item.casefold()
        if key in seen:
            issues.add(f"{path}[{index}]", "Duplicate string entry.", actual=item)
            continue
        seen.add(key)


def validate_schema_version(obj: dict[str, Any], path: str, issues: IssueCollector) -> str | None:
    value = require_string(obj, "schema_version", path, issues)
    if value is None:
        return None
    if not re.match(r"^\d+\.\d+\.\d+$", value):
        issues.add(f"{path}.schema_version", "Expected semantic-version-like string.", actual=value)
        return None
    return value


def validate_time_window(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    start_text = require_string(obj, "start_utc", path, issues)
    end_text = require_string(obj, "end_utc", path, issues)
    start_dt = parse_utc_datetime(start_text) if start_text is not None else None
    end_dt = parse_utc_datetime(end_text) if end_text is not None else None
    if start_text is not None and start_dt is None:
        issues.add(f"{path}.start_utc", "Expected RFC3339 UTC string with trailing Z.", actual=start_text)
    if end_text is not None and end_dt is None:
        issues.add(f"{path}.end_utc", "Expected RFC3339 UTC string with trailing Z.", actual=end_text)
    if start_dt is not None and end_dt is not None and end_dt < start_dt:
        issues.add(path, "end_utc must be >= start_utc.")


def validate_geometry(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    geometry_type = require_string(obj, "type", path, issues)
    if geometry_type is None:
        return
    if geometry_type == "Point":
        latitude = require_number(obj, "latitude", path, issues, minimum=-90, maximum=90)
        longitude = require_number(obj, "longitude", path, issues, minimum=-180, maximum=180)
        if latitude is not None and longitude is not None:
            return
    elif geometry_type == "BBox":
        west = require_number(obj, "west", path, issues, minimum=-180, maximum=180)
        south = require_number(obj, "south", path, issues, minimum=-90, maximum=90)
        east = require_number(obj, "east", path, issues, minimum=-180, maximum=180)
        north = require_number(obj, "north", path, issues, minimum=-90, maximum=90)
        if west is not None and east is not None and east <= west:
            issues.add(path, "BBox east must be greater than west.")
        if south is not None and north is not None and north <= south:
            issues.add(path, "BBox north must be greater than south.")
    else:
        issues.add(f"{path}.type", "Expected Point or BBox.", actual=geometry_type)


def validate_region_scope(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "label", path, issues)
    validate_geometry(obj.get("geometry"), f"{path}.geometry", issues)


def validate_artifact_ref(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "source_skill", path, issues)
    require_string(obj, "artifact_path", path, issues)
    if "record_locator" in obj and obj["record_locator"] is not None and not isinstance(obj["record_locator"], str):
        issues.add(f"{path}.record_locator", "Expected a string when provided.", actual=obj["record_locator"])
    if "external_id" in obj and obj["external_id"] is not None and not isinstance(obj["external_id"], str):
        issues.add(f"{path}.external_id", "Expected a string when provided.", actual=obj["external_id"])
    sha256 = obj.get("sha256")
    if sha256 is not None:
        if not isinstance(sha256, str) or not SHA256_PATTERN.match(sha256):
            issues.add(f"{path}.sha256", "Expected a 64-character hexadecimal SHA256 string.", actual=sha256)


def validate_recommendation(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_enum(obj, "assigned_role", path, issues, allowed=AGENT_ROLES)
    require_string(obj, "objective", path, issues)
    require_string(obj, "reason", path, issues)


def validate_source_decision(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "source_skill", path, issues)
    require_bool(obj, "selected", path, issues)
    require_string(obj, "reason", path, issues)


def validate_round_task_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "task_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "assigned_role", path, issues, allowed=AGENT_ROLES)
    require_string(record, "objective", path, issues)
    require_enum(record, "status", path, issues, allowed=TASK_STATUSES)
    if "depends_on" in record:
        validate_string_list(record["depends_on"], f"{path}.depends_on", issues)
    if "expected_output_kinds" in record:
        kinds = validate_string_list(record["expected_output_kinds"], f"{path}.expected_output_kinds", issues)
        for index, kind in enumerate(kinds):
            if kind not in OBJECT_KINDS:
                issues.add(
                    f"{path}.expected_output_kinds[{index}]",
                    f"Expected one of {list(OBJECT_KINDS)}.",
                    actual=kind,
                )
    if "inputs" in record and record["inputs"] is not None and not isinstance(record["inputs"], dict):
        issues.add(f"{path}.inputs", "Expected an object when provided.", actual=record["inputs"])
    if "notes" in record and record["notes"] is not None and not isinstance(record["notes"], str):
        issues.add(f"{path}.notes", "Expected a string when provided.", actual=record["notes"])


def validate_source_selection_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "selection_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "agent_role", path, issues, allowed=SOURCE_SELECTION_ROLES)
    status = require_enum(record, "status", path, issues, allowed=SOURCE_SELECTION_STATUSES)
    require_string(record, "summary", path, issues)
    task_ids = validate_string_list(record.get("task_ids"), f"{path}.task_ids", issues)
    validate_unique_strings(task_ids, f"{path}.task_ids", issues)
    allowed_sources = validate_string_list(record.get("allowed_sources"), f"{path}.allowed_sources", issues)
    validate_unique_strings(allowed_sources, f"{path}.allowed_sources", issues)
    selected_sources = validate_string_list(record.get("selected_sources"), f"{path}.selected_sources", issues)
    validate_unique_strings(selected_sources, f"{path}.selected_sources", issues)

    decisions = record.get("source_decisions")
    if not isinstance(decisions, list):
        issues.add(f"{path}.source_decisions", "Expected a list.", actual=decisions)
        decisions = []

    allowed_lookup = {item.casefold() for item in allowed_sources}
    selected_lookup = {item.casefold() for item in selected_sources}
    decision_keys: set[str] = set()
    selected_by_decision: set[str] = set()

    for index, decision in enumerate(decisions):
        item_path = f"{path}.source_decisions[{index}]"
        validate_source_decision(decision, item_path, issues)
        if not isinstance(decision, dict):
            continue
        source_skill = require_string(decision, "source_skill", item_path, issues)
        selected = require_bool(decision, "selected", item_path, issues)
        if source_skill is None:
            continue
        key = source_skill.casefold()
        if key in decision_keys:
            issues.add(f"{item_path}.source_skill", "Duplicate source_skill entry.", actual=source_skill)
        else:
            decision_keys.add(key)
        if allowed_lookup and key not in allowed_lookup:
            issues.add(f"{item_path}.source_skill", "source_skill must also appear in allowed_sources.", actual=source_skill)
        if selected is True:
            selected_by_decision.add(key)
        elif key in selected_lookup:
            issues.add(
                f"{item_path}.selected",
                "selected_sources cannot include a source whose decision has selected=false.",
                actual=source_skill,
            )

    for index, source_skill in enumerate(selected_sources):
        key = source_skill.casefold()
        if allowed_lookup and key not in allowed_lookup:
            issues.add(
                f"{path}.selected_sources[{index}]",
                "Selected source must also appear in allowed_sources.",
                actual=source_skill,
            )
        if status != "pending" and key not in decision_keys:
            issues.add(
                f"{path}.selected_sources[{index}]",
                "Selected source must also appear in source_decisions once selection is complete or blocked.",
                actual=source_skill,
            )
        if key not in selected_by_decision:
            issues.add(
                f"{path}.selected_sources[{index}]",
                "Selected source must have a matching source_decisions entry with selected=true.",
                actual=source_skill,
            )

    if status != "pending":
        for index, source_skill in enumerate(allowed_sources):
            if source_skill.casefold() not in decision_keys:
                issues.add(
                    f"{path}.allowed_sources[{index}]",
                    "Each allowed source must have one source_decisions entry once selection is complete or blocked.",
                    actual=source_skill,
                )


def validate_mission_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "topic", path, issues)
    require_string(record, "objective", path, issues)
    validate_time_window(record.get("window"), f"{path}.window", issues)
    validate_region_scope(record.get("region"), f"{path}.region", issues)
    if "hypotheses" in record:
        validate_string_list(record["hypotheses"], f"{path}.hypotheses", issues)
    constraints = require_object(record.get("constraints"), f"{path}.constraints", issues)
    if constraints is not None:
        require_int(constraints, "max_rounds", f"{path}.constraints", issues, minimum=1)
        require_int(constraints, "max_claims_per_round", f"{path}.constraints", issues, minimum=1)
        require_int(constraints, "max_tasks_per_round", f"{path}.constraints", issues, minimum=1)
    source_policy = record.get("source_policy")
    if source_policy is not None:
        policy_obj = require_object(source_policy, f"{path}.source_policy", issues)
        if policy_obj is not None:
            for field_name in ("sociologist", "environmentalist", "historian"):
                if field_name in policy_obj:
                    validate_string_list(policy_obj[field_name], f"{path}.source_policy.{field_name}", issues)


def validate_claim_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "claim_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "agent_role", path, issues, allowed=AGENT_ROLES)
    require_enum(record, "claim_type", path, issues, allowed=CLAIM_TYPES)
    require_enum(record, "status", path, issues, allowed=CLAIM_STATUSES)
    require_string(record, "summary", path, issues)
    require_string(record, "statement", path, issues)
    require_int(record, "priority", path, issues, minimum=1, maximum=5)
    require_bool(record, "needs_physical_validation", path, issues)
    validate_time_window(record.get("time_window"), f"{path}.time_window", issues)
    validate_region_scope(record.get("place_scope"), f"{path}.place_scope", issues)
    public_refs = record.get("public_refs")
    if not isinstance(public_refs, list):
        issues.add(f"{path}.public_refs", "Expected a list.", actual=public_refs)
    else:
        for index, ref in enumerate(public_refs):
            validate_artifact_ref(ref, f"{path}.public_refs[{index}]", issues)


def validate_statistics(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    for field_name in ("min", "max", "mean", "p95"):
        if field_name not in obj:
            continue
        field_value = obj[field_name]
        if field_value is not None and not is_number(field_value):
            issues.add(f"{path}.{field_name}", "Expected a number or null.", actual=field_value)


def validate_observation_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "observation_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "agent_role", path, issues, allowed=AGENT_ROLES)
    require_string(record, "source_skill", path, issues)
    require_string(record, "metric", path, issues)
    require_enum(record, "aggregation", path, issues, allowed=OBSERVATION_AGGREGATIONS)
    value = record.get("value")
    if value is not None and not is_number(value):
        issues.add(f"{path}.value", "Expected a number or null.", actual=value)
    require_string(record, "unit", path, issues)
    if "statistics" in record and record["statistics"] is not None:
        validate_statistics(record["statistics"], f"{path}.statistics", issues)
    validate_time_window(record.get("time_window"), f"{path}.time_window", issues)
    validate_region_scope(record.get("place_scope"), f"{path}.place_scope", issues)
    validate_string_list(record.get("quality_flags"), f"{path}.quality_flags", issues)
    validate_artifact_ref(record.get("provenance"), f"{path}.provenance", issues)


def validate_evidence_card_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "evidence_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_string(record, "claim_id", path, issues)
    require_enum(record, "verdict", path, issues, allowed=EVIDENCE_VERDICTS)
    require_enum(record, "confidence", path, issues, allowed=CONFIDENCE_VALUES)
    require_string(record, "summary", path, issues)
    public_refs = record.get("public_refs")
    if not isinstance(public_refs, list):
        issues.add(f"{path}.public_refs", "Expected a list.", actual=public_refs)
    else:
        for index, ref in enumerate(public_refs):
            validate_artifact_ref(ref, f"{path}.public_refs[{index}]", issues)
    validate_string_list(record.get("observation_ids"), f"{path}.observation_ids", issues)
    validate_string_list(record.get("gaps"), f"{path}.gaps", issues)


def validate_finding(value: Any, path: str, issues: IssueCollector) -> None:
    obj = require_object(value, path, issues)
    if obj is None:
        return
    require_string(obj, "finding_id", path, issues)
    require_string(obj, "title", path, issues)
    require_string(obj, "summary", path, issues)
    require_enum(obj, "confidence", path, issues, allowed=CONFIDENCE_VALUES)
    for field_name in ("claim_ids", "observation_ids", "evidence_ids"):
        if field_name in obj:
            validate_string_list(obj[field_name], f"{path}.{field_name}", issues)


def validate_expert_report_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "report_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "agent_role", path, issues, allowed=AGENT_ROLES)
    require_enum(record, "status", path, issues, allowed=REPORT_STATUSES)
    require_string(record, "summary", path, issues)
    findings = record.get("findings")
    if not isinstance(findings, list):
        issues.add(f"{path}.findings", "Expected a list.", actual=findings)
    else:
        for index, finding in enumerate(findings):
            validate_finding(finding, f"{path}.findings[{index}]", issues)
    validate_string_list(record.get("open_questions"), f"{path}.open_questions", issues)
    recommendations = record.get("recommended_next_actions")
    if not isinstance(recommendations, list):
        issues.add(
            f"{path}.recommended_next_actions",
            "Expected a list.",
            actual=recommendations,
        )
    else:
        for index, recommendation in enumerate(recommendations):
            validate_recommendation(
                recommendation,
                f"{path}.recommended_next_actions[{index}]",
                issues,
            )


def validate_council_decision_object(obj: Any, path: str, issues: IssueCollector) -> None:
    record = require_object(obj, path, issues)
    if record is None:
        return
    validate_schema_version(record, path, issues)
    require_string(record, "decision_id", path, issues)
    require_string(record, "run_id", path, issues)
    require_string(record, "round_id", path, issues)
    require_enum(record, "moderator_status", path, issues, allowed=MODERATOR_STATUSES)
    require_number(record, "completion_score", path, issues, minimum=0.0, maximum=1.0)
    require_enum(record, "evidence_sufficiency", path, issues, allowed=EVIDENCE_SUFFICIENCY)
    require_string(record, "decision_summary", path, issues)
    require_bool(record, "next_round_required", path, issues)
    validate_string_list(record.get("missing_evidence_types"), f"{path}.missing_evidence_types", issues)
    tasks = record.get("next_round_tasks")
    if not isinstance(tasks, list):
        issues.add(f"{path}.next_round_tasks", "Expected a list.", actual=tasks)
    else:
        for index, task in enumerate(tasks):
            validate_round_task_object(task, f"{path}.next_round_tasks[{index}]", issues)
    if "final_brief" in record and record["final_brief"] is not None and not isinstance(record["final_brief"], str):
        issues.add(f"{path}.final_brief", "Expected a string when provided.", actual=record["final_brief"])


VALIDATORS = {
    "mission": validate_mission_object,
    "round-task": validate_round_task_object,
    "source-selection": validate_source_selection_object,
    "claim": validate_claim_object,
    "observation": validate_observation_object,
    "evidence-card": validate_evidence_card_object,
    "expert-report": validate_expert_report_object,
    "council-decision": validate_council_decision_object,
}

EXAMPLES: dict[str, Any] = {
    "mission": read_json(EXAMPLES_DIR / "mission.json"),
    "round-task": read_json(EXAMPLES_DIR / "round_task.json"),
    "source-selection": read_json(EXAMPLES_DIR / "source_selection.json"),
    "claim": read_json(EXAMPLES_DIR / "claim.json"),
    "observation": read_json(EXAMPLES_DIR / "observation.json"),
    "evidence-card": read_json(EXAMPLES_DIR / "evidence_card.json"),
    "expert-report": read_json(EXAMPLES_DIR / "expert_report.json"),
    "council-decision": read_json(EXAMPLES_DIR / "council_decision.json"),
}


def validate_payload(kind: str, payload: Any) -> dict[str, Any]:
    issues = IssueCollector()
    validator = VALIDATORS[kind]
    if isinstance(payload, list):
        for index, item in enumerate(payload):
            validator(item, f"{kind}[{index}]", issues)
        top_level = "list"
        item_count = len(payload)
    else:
        validator(payload, kind, issues)
        top_level = "object"
        item_count = 1
    return {
        "kind": kind,
        "top_level": top_level,
        "item_count": item_count,
        "validation": issues.summary(),
    }


def load_ddl() -> str:
    return DDL_PATH.read_text(encoding="utf-8")


def parse_point(raw: str) -> dict[str, Any]:
    parts = [part.strip() for part in raw.split(",")]
    if len(parts) != 2:
        raise ValueError("--point must be in latitude,longitude format.")
    try:
        latitude = float(parts[0])
        longitude = float(parts[1])
    except ValueError as exc:
        raise ValueError("--point latitude and longitude must be numeric.") from exc
    if latitude < -90 or latitude > 90:
        raise ValueError("--point latitude must be between -90 and 90.")
    if longitude < -180 or longitude > 180:
        raise ValueError("--point longitude must be between -180 and 180.")
    return {"type": "Point", "latitude": latitude, "longitude": longitude}


def parse_bbox(raw: str) -> dict[str, Any]:
    parts = [part.strip() for part in raw.split(",")]
    if len(parts) != 4:
        raise ValueError("--bbox must be in west,south,east,north format.")
    try:
        west = float(parts[0])
        south = float(parts[1])
        east = float(parts[2])
        north = float(parts[3])
    except ValueError as exc:
        raise ValueError("--bbox coordinates must be numeric.") from exc
    if west < -180 or west > 180 or east < -180 or east > 180:
        raise ValueError("--bbox west/east must be between -180 and 180.")
    if south < -90 or south > 90 or north < -90 or north > 90:
        raise ValueError("--bbox south/north must be between -90 and 90.")
    if east <= west:
        raise ValueError("--bbox east must be greater than west.")
    if north <= south:
        raise ValueError("--bbox north must be greater than south.")
    return {"type": "BBox", "west": west, "south": south, "east": east, "north": north}


def round_dir_name(round_id: str) -> str:
    value = round_id.strip()
    if not ROUND_ID_PATTERN.match(value):
        raise ValueError(f"Unsupported round_id format: {round_id!r}. Expected round-001 style.")
    return value.replace("-", "_")


def round_id_from_dirname(dirname: str) -> str | None:
    match = ROUND_DIR_PATTERN.match(dirname.strip())
    if match is None:
        return None
    return f"round-{match.group(1)}"


def round_number(round_id: str) -> int:
    if not ROUND_ID_PATTERN.match(round_id):
        raise ValueError(f"Unsupported round_id format: {round_id!r}. Expected round-001 style.")
    return int(round_id.split("-")[-1])


def round_sort_key(round_id: str) -> tuple[int, str]:
    try:
        return (round_number(round_id), round_id)
    except ValueError:
        return (sys.maxsize, round_id)


def source_policy_for_role(mission: dict[str, Any], role: str) -> list[str]:
    policy = mission.get("source_policy")
    if not isinstance(policy, dict):
        return []
    selected = policy.get(role)
    if not isinstance(selected, list):
        return []
    values = [item for item in selected if isinstance(item, str) and item.strip()]
    return values


def expected_output_kinds_for_role(role: str) -> list[str]:
    if role == "sociologist":
        return ["source-selection", "claim", "expert-report"]
    if role == "environmentalist":
        return ["source-selection", "observation", "expert-report"]
    if role == "historian":
        return ["expert-report"]
    return ["expert-report"]


def default_round_tasks(*, mission: dict[str, Any], round_id: str) -> list[dict[str, Any]]:
    run_id = mission["run_id"]
    geometry = mission.get("region", {}).get("geometry") if isinstance(mission.get("region"), dict) else None
    mission_window = mission.get("window") if isinstance(mission.get("window"), dict) else {}

    sociologist_task = copy.deepcopy(EXAMPLES["round-task"])
    sociologist_task["task_id"] = f"task-sociologist-{round_id}-01"
    sociologist_task["run_id"] = run_id
    sociologist_task["round_id"] = round_id
    sociologist_task["assigned_role"] = "sociologist"
    sociologist_task["objective"] = (
        "Identify up to three public claims within the mission window that are worth physical validation."
    )
    sociologist_task["expected_output_kinds"] = expected_output_kinds_for_role("sociologist")

    environmental_task = copy.deepcopy(EXAMPLES["round-task"])
    environmental_task["task_id"] = f"task-environmentalist-{round_id}-01"
    environmental_task["run_id"] = run_id
    environmental_task["round_id"] = round_id
    environmental_task["assigned_role"] = "environmentalist"
    environmental_task["objective"] = (
        "Validate mission-relevant physical evidence in the same window using the configured environment skills."
    )
    environmental_task["expected_output_kinds"] = expected_output_kinds_for_role("environmentalist")
    environmental_task["inputs"] = {"mission_geometry": geometry, "mission_window": mission_window}

    return [sociologist_task, environmental_task]


def placeholder_source_selection(
    *,
    run_id: str,
    round_id: str,
    role: str,
    task_ids: list[str],
    allowed_sources: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "selection_id": f"source-selection-{role}-{round_id}",
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": role,
        "status": "pending",
        "summary": f"Pending {role} source selection.",
        "task_ids": task_ids,
        "allowed_sources": allowed_sources,
        "selected_sources": [],
        "source_decisions": [],
    }


def placeholder_report(*, run_id: str, round_id: str, role: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_id": f"report-{role}-{round_id}",
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": role,
        "status": "needs-more-evidence",
        "summary": f"Pending {role} execution.",
        "findings": [],
        "open_questions": [],
        "recommended_next_actions": [],
    }


def normalize_round_tasks(
    *,
    tasks: list[dict[str, Any]],
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for index, task in enumerate(tasks):
        if not isinstance(task, dict):
            raise ValueError(f"Task index {index} is not an object.")
        candidate = copy.deepcopy(task)
        candidate["run_id"] = run_id
        candidate["round_id"] = round_id
        result = validate_payload("round-task", candidate)
        if not result["validation"]["ok"]:
            issues = result["validation"]["issues"]
            raise ValueError(f"Task index {index} failed validation: {issues}")
        normalized.append(candidate)
    if not normalized:
        raise ValueError("At least one round-task is required to scaffold a round.")
    return normalized


def scaffold_round(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    tasks: list[dict[str, Any]],
    mission: dict[str, Any] | None,
    pretty: bool,
) -> dict[str, Any]:
    run_path = run_dir.expanduser().resolve()
    round_path = run_path / round_dir_name(round_id)
    normalized_tasks = normalize_round_tasks(tasks=tasks, run_id=run_id, round_id=round_id)
    source_selection_roles = ("sociologist", "environmentalist")
    source_selection_files = {
        role: placeholder_source_selection(
            run_id=run_id,
            round_id=round_id,
            role=role,
            task_ids=[
                item["task_id"]
                for item in normalized_tasks
                if item.get("assigned_role") == role and isinstance(item.get("task_id"), str)
            ],
            allowed_sources=source_policy_for_role(mission or {}, role),
        )
        for role in source_selection_roles
    }

    files_to_write = {
        round_path / "moderator" / "tasks.json": normalized_tasks,
        round_path / "shared" / "claims.json": [],
        round_path / "shared" / "observations.json": [],
        round_path / "shared" / "evidence_cards.json": [],
        round_path / "sociologist" / "source_selection.json": source_selection_files["sociologist"],
        round_path / "environmentalist" / "source_selection.json": source_selection_files["environmentalist"],
        round_path / "sociologist" / "sociologist_report.json": placeholder_report(
            run_id=run_id,
            round_id=round_id,
            role="sociologist",
        ),
        round_path / "environmentalist" / "environmentalist_report.json": placeholder_report(
            run_id=run_id,
            round_id=round_id,
            role="environmentalist",
        ),
    }

    directories = (
        round_path / "sociologist" / "raw",
        round_path / "sociologist" / "normalized",
        round_path / "sociologist" / "derived",
        round_path / "environmentalist" / "raw",
        round_path / "environmentalist" / "normalized",
        round_path / "environmentalist" / "derived",
        round_path / "historian" / "raw",
        round_path / "historian" / "normalized",
        round_path / "historian" / "derived",
        round_path / "moderator" / "derived",
        round_path / "shared" / "contexts",
    )
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

    for path, payload in files_to_write.items():
        write_json(path, payload, pretty=pretty)

    return {
        "round_id": round_id,
        "round_dir": str(round_path),
        "files_written": [str(path) for path in sorted(files_to_write)],
        "directories_ready": [str(path) for path in sorted(directories)],
    }


def scaffold_run_from_mission(
    *,
    run_dir: Path,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]] | None,
    pretty: bool,
) -> dict[str, Any]:
    validation = validate_payload("mission", mission)
    if not validation["validation"]["ok"]:
        raise ValueError(f"Mission payload failed validation: {validation['validation']['issues']}")

    run_path = run_dir.expanduser().resolve()
    run_path.mkdir(parents=True, exist_ok=True)
    run_id = mission["run_id"]
    round_id = "round-001"
    task_list = tasks if tasks is not None else default_round_tasks(mission=mission, round_id=round_id)

    mission_path = run_path / "mission.json"
    write_json(mission_path, mission, pretty=pretty)
    round_result = scaffold_round(
        run_dir=run_path,
        run_id=run_id,
        round_id=round_id,
        tasks=task_list,
        mission=mission,
        pretty=pretty,
    )
    return {
        "run_dir": str(run_path),
        "run_id": run_id,
        "round_id": round_id,
        "mission_path": str(mission_path),
        "round": round_result,
        "schema_path": str(SCHEMA_PATH),
    }


def scaffold_run(
    *,
    run_dir: Path,
    run_id: str,
    topic: str,
    objective: str,
    start_utc: str,
    end_utc: str,
    region_label: str,
    geometry: dict[str, Any],
    pretty: bool,
) -> dict[str, Any]:
    if parse_utc_datetime(start_utc) is None:
        raise ValueError("--start-utc must be RFC3339 UTC with trailing Z.")
    if parse_utc_datetime(end_utc) is None:
        raise ValueError("--end-utc must be RFC3339 UTC with trailing Z.")
    if parse_utc_datetime(end_utc) < parse_utc_datetime(start_utc):
        raise ValueError("--end-utc must be >= --start-utc.")

    mission = copy.deepcopy(EXAMPLES["mission"])
    mission["run_id"] = run_id
    mission["topic"] = topic
    mission["objective"] = objective
    mission["window"]["start_utc"] = start_utc
    mission["window"]["end_utc"] = end_utc
    mission["region"]["label"] = region_label
    mission["region"]["geometry"] = geometry
    return scaffold_run_from_mission(
        run_dir=run_dir,
        mission=mission,
        tasks=None,
        pretty=pretty,
    )


def validate_bundle(run_dir: Path) -> dict[str, Any]:
    bundle_path = run_dir.expanduser().resolve()
    results: list[dict[str, Any]] = []
    missing_required: list[str] = []
    missing_optional: list[str] = []
    round_summaries: list[dict[str, Any]] = []

    mission_path = bundle_path / "mission.json"
    if not mission_path.exists():
        missing_required.append(str(mission_path))
    else:
        mission_payload = read_json(mission_path)
        mission_result = validate_payload("mission", mission_payload)
        mission_result["path"] = str(mission_path)
        results.append(mission_result)

    round_ids: list[str] = []
    for child in sorted(bundle_path.iterdir(), key=lambda item: item.name):
        if not child.is_dir():
            continue
        round_id = round_id_from_dirname(child.name)
        if round_id:
            round_ids.append(round_id)
    round_ids.sort(key=round_sort_key)
    if not round_ids:
        missing_required.append(str(bundle_path / "round_001"))

    for round_id in round_ids:
        round_path = bundle_path / round_dir_name(round_id)
        round_required = {
            round_path / "moderator" / "tasks.json": "round-task",
            round_path / "shared" / "claims.json": "claim",
            round_path / "shared" / "observations.json": "observation",
            round_path / "shared" / "evidence_cards.json": "evidence-card",
            round_path / "sociologist" / "source_selection.json": "source-selection",
            round_path / "environmentalist" / "source_selection.json": "source-selection",
            round_path / "sociologist" / "sociologist_report.json": "expert-report",
            round_path / "environmentalist" / "environmentalist_report.json": "expert-report",
        }
        round_optional = {
            round_path / "historian" / "historian_report.json": "expert-report",
            round_path / "moderator" / "council_decision.json": "council-decision",
        }

        round_results: list[dict[str, Any]] = []
        round_missing_required: list[str] = []
        round_missing_optional: list[str] = []

        for path, kind in round_required.items():
            if not path.exists():
                round_missing_required.append(str(path))
                continue
            payload = read_json(path)
            result = validate_payload(kind, payload)
            result["path"] = str(path)
            round_results.append(result)
            results.append(result)

        for path, kind in round_optional.items():
            if not path.exists():
                round_missing_optional.append(str(path))
                continue
            payload = read_json(path)
            result = validate_payload(kind, payload)
            result["path"] = str(path)
            round_results.append(result)
            results.append(result)

        missing_required.extend(round_missing_required)
        missing_optional.extend(round_missing_optional)
        round_summaries.append(
            {
                "round_id": round_id,
                "round_dir": str(round_path),
                "missing_required_files": round_missing_required,
                "missing_optional_files": round_missing_optional,
                "results": round_results,
            }
        )

    ok = not missing_required and all(item["validation"]["ok"] for item in results)
    return {
        "run_dir": str(bundle_path),
        "ok": ok,
        "round_ids": round_ids,
        "missing_required_files": missing_required,
        "missing_optional_files": missing_optional,
        "results": results,
        "rounds": round_summaries,
    }


def command_list_kinds(_: argparse.Namespace) -> dict[str, Any]:
    return {
        "kinds": list(OBJECT_KINDS),
        "schema_path": str(SCHEMA_PATH),
        "ddl_path": str(DDL_PATH),
    }


def command_write_example(args: argparse.Namespace) -> dict[str, Any]:
    payload = copy.deepcopy(EXAMPLES[args.kind])
    output_path = Path(args.output).expanduser().resolve()
    write_json(output_path, payload, pretty=args.pretty)
    return {"kind": args.kind, "output": str(output_path)}


def command_validate(args: argparse.Namespace) -> dict[str, Any]:
    input_path = Path(args.input).expanduser().resolve()
    payload = read_json(input_path)
    result = validate_payload(args.kind, payload)
    result["input"] = str(input_path)
    return result


def command_init_db(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    ddl = load_ddl()
    with sqlite3.connect(db_path) as conn:
        conn.executescript(ddl)
        conn.commit()
    return {
        "db": str(db_path),
        "ddl_path": str(DDL_PATH),
        "initialized_at": utc_now_iso(),
    }


def command_scaffold_run(args: argparse.Namespace) -> dict[str, Any]:
    if bool(args.point) == bool(args.bbox):
        raise ValueError("Provide exactly one of --point or --bbox.")
    geometry = parse_point(args.point) if args.point else parse_bbox(args.bbox)
    return scaffold_run(
        run_dir=Path(args.run_dir),
        run_id=args.run_id,
        topic=args.topic,
        objective=args.objective,
        start_utc=args.start_utc,
        end_utc=args.end_utc,
        region_label=args.region_label,
        geometry=geometry,
        pretty=args.pretty,
    )


def command_scaffold_run_from_mission(args: argparse.Namespace) -> dict[str, Any]:
    mission_path = Path(args.mission_input).expanduser().resolve()
    mission_payload = read_json(mission_path)
    tasks_payload: list[dict[str, Any]] | None = None
    if args.tasks_input:
        tasks_path = Path(args.tasks_input).expanduser().resolve()
        loaded_tasks = read_json(tasks_path)
        if not isinstance(loaded_tasks, list):
            raise ValueError("--tasks-input must contain a JSON list of round-task objects.")
        tasks_payload = [item for item in loaded_tasks if isinstance(item, dict)]
        if len(tasks_payload) != len(loaded_tasks):
            raise ValueError("--tasks-input must contain only JSON objects.")
    result = scaffold_run_from_mission(
        run_dir=Path(args.run_dir),
        mission=mission_payload,
        tasks=tasks_payload,
        pretty=args.pretty,
    )
    result["mission_input"] = str(mission_path)
    if args.tasks_input:
        result["tasks_input"] = str(Path(args.tasks_input).expanduser().resolve())
    return result


def command_scaffold_round(args: argparse.Namespace) -> dict[str, Any]:
    tasks_path = Path(args.tasks_input).expanduser().resolve()
    task_payload = read_json(tasks_path)
    if not isinstance(task_payload, list):
        raise ValueError("--tasks-input must contain a JSON list of round-task objects.")
    if not all(isinstance(item, dict) for item in task_payload):
        raise ValueError("--tasks-input must contain only JSON objects.")

    mission_path = Path(args.mission_input).expanduser().resolve() if args.mission_input else Path(args.run_dir).expanduser().resolve() / "mission.json"
    mission_payload = read_json(mission_path)
    mission_validation = validate_payload("mission", mission_payload)
    if not mission_validation["validation"]["ok"]:
        raise ValueError(f"Mission payload failed validation: {mission_validation['validation']['issues']}")

    run_id = mission_payload["run_id"]
    result = scaffold_round(
        run_dir=Path(args.run_dir),
        run_id=run_id,
        round_id=args.round_id,
        tasks=task_payload,
        mission=mission_payload,
        pretty=args.pretty,
    )
    result["mission_input"] = str(mission_path)
    result["tasks_input"] = str(tasks_path)
    return result


def command_validate_bundle(args: argparse.Namespace) -> dict[str, Any]:
    return validate_bundle(Path(args.run_dir))


def add_pretty_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate and scaffold eco-council shared contracts.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_list = subparsers.add_parser("list-kinds", help="List canonical object kinds.")
    add_pretty_flag(parser_list)

    parser_write = subparsers.add_parser("write-example", help="Write one example payload to disk.")
    parser_write.add_argument("--kind", required=True, choices=OBJECT_KINDS)
    parser_write.add_argument("--output", required=True, help="Output JSON path.")
    add_pretty_flag(parser_write)

    parser_validate = subparsers.add_parser("validate", help="Validate one JSON file.")
    parser_validate.add_argument("--kind", required=True, choices=OBJECT_KINDS)
    parser_validate.add_argument("--input", required=True, help="Input JSON path.")
    add_pretty_flag(parser_validate)

    parser_init_db = subparsers.add_parser("init-db", help="Initialize the canonical SQLite database.")
    parser_init_db.add_argument("--db", required=True, help="SQLite database path.")
    add_pretty_flag(parser_init_db)

    parser_scaffold = subparsers.add_parser("scaffold-run", help="Scaffold one eco-council run directory.")
    parser_scaffold.add_argument("--run-dir", required=True, help="Run directory.")
    parser_scaffold.add_argument("--run-id", required=True, help="Stable run identifier.")
    parser_scaffold.add_argument("--topic", required=True, help="Mission topic.")
    parser_scaffold.add_argument("--objective", required=True, help="Mission objective.")
    parser_scaffold.add_argument("--start-utc", required=True, help="Mission start datetime in UTC.")
    parser_scaffold.add_argument("--end-utc", required=True, help="Mission end datetime in UTC.")
    parser_scaffold.add_argument("--region-label", required=True, help="Human-readable region label.")
    parser_scaffold.add_argument("--point", help="Point geometry as latitude,longitude.")
    parser_scaffold.add_argument("--bbox", help="BBox geometry as west,south,east,north.")
    add_pretty_flag(parser_scaffold)

    parser_scaffold_mission = subparsers.add_parser(
        "scaffold-run-from-mission",
        help="Scaffold one eco-council run directory from an existing mission JSON payload.",
    )
    parser_scaffold_mission.add_argument("--run-dir", required=True, help="Run directory.")
    parser_scaffold_mission.add_argument("--mission-input", required=True, help="Mission JSON path.")
    parser_scaffold_mission.add_argument(
        "--tasks-input",
        default="",
        help="Optional JSON path containing initial round-task list for round-001.",
    )
    add_pretty_flag(parser_scaffold_mission)

    parser_scaffold_round = subparsers.add_parser(
        "scaffold-round",
        help="Scaffold one additional round directory from a validated round-task list.",
    )
    parser_scaffold_round.add_argument("--run-dir", required=True, help="Run directory.")
    parser_scaffold_round.add_argument("--round-id", required=True, help="Round identifier, for example round-002.")
    parser_scaffold_round.add_argument("--tasks-input", required=True, help="JSON path containing round-task list.")
    parser_scaffold_round.add_argument(
        "--mission-input",
        default="",
        help="Optional mission JSON path. Defaults to <run-dir>/mission.json.",
    )
    add_pretty_flag(parser_scaffold_round)

    parser_bundle = subparsers.add_parser(
        "validate-bundle",
        help="Validate a scaffolded run bundle and any canonical files already produced.",
    )
    parser_bundle.add_argument("--run-dir", required=True, help="Run directory to inspect.")
    add_pretty_flag(parser_bundle)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    command_map = {
        "list-kinds": command_list_kinds,
        "write-example": command_write_example,
        "validate": command_validate,
        "init-db": command_init_db,
        "scaffold-run": command_scaffold_run,
        "scaffold-run-from-mission": command_scaffold_run_from_mission,
        "scaffold-round": command_scaffold_round,
        "validate-bundle": command_validate_bundle,
    }
    try:
        payload = command_map[args.command](args)
    except Exception as exc:
        error_payload = {
            "command": args.command,
            "ok": False,
            "error": str(exc),
        }
        print(pretty_json(error_payload, pretty=getattr(args, "pretty", False)))
        return 1

    result = {
        "command": args.command,
        "ok": True,
        "payload": payload,
    }
    print(pretty_json(result, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
