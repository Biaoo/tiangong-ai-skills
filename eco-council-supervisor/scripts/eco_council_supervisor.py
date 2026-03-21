#!/usr/bin/env python3
"""Run eco-council stages with approval gates and fixed agent handoffs."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
REPO_DIR = SKILL_DIR.parent

ORCHESTRATE_SCRIPT = REPO_DIR / "eco-council-orchestrate" / "scripts" / "eco_council_orchestrate.py"
REPORTING_SCRIPT = REPO_DIR / "eco-council-reporting" / "scripts" / "eco_council_reporting.py"
CONTRACT_SCRIPT = REPO_DIR / "eco-council-data-contract" / "scripts" / "eco_council_contract.py"

SCHEMA_VERSION = "1.0.0"
ROUND_ID_PATTERN = re.compile(r"^round-\d{3}$")
ROUND_DIR_PATTERN = re.compile(r"^round_(\d{3})$")
AGENT_ID_SAFE = re.compile(r"[^a-z0-9-]+")
ROLES = ("moderator", "sociologist", "environmentalist")
REPORT_ROLES = ("sociologist", "environmentalist")

STAGE_AWAITING_TASK_REVIEW = "awaiting-moderator-task-review"
STAGE_READY_PREPARE = "ready-to-prepare-round"
STAGE_READY_FETCH = "ready-to-execute-fetch-plan"
STAGE_READY_DATA_PLANE = "ready-to-run-data-plane"
STAGE_AWAITING_REPORTS = "awaiting-expert-reports"
STAGE_AWAITING_DECISION = "awaiting-moderator-decision"
STAGE_READY_PROMOTE = "ready-to-promote"
STAGE_READY_ADVANCE = "ready-to-advance-round"
STAGE_COMPLETED = "completed"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty_json(data: Any, *, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any, *, pretty: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(pretty_json(payload, pretty=pretty) + "\n", encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def require_round_id(value: str) -> str:
    if not ROUND_ID_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid round id: {value!r}")
    return value


def round_dir_name(round_id: str) -> str:
    require_round_id(round_id)
    return f"round_{round_id.split('-')[1]}"


def round_dir(run_dir: Path, round_id: str) -> Path:
    return run_dir / round_dir_name(round_id)


def discover_round_ids(run_dir: Path) -> list[str]:
    round_ids: list[str] = []
    if not run_dir.exists():
        return round_ids
    for child in run_dir.iterdir():
        if not child.is_dir():
            continue
        match = ROUND_DIR_PATTERN.fullmatch(child.name)
        if match is None:
            continue
        round_ids.append(f"round-{match.group(1)}")
    round_ids.sort()
    return round_ids


def latest_round_id(run_dir: Path) -> str:
    round_ids = discover_round_ids(run_dir)
    if not round_ids:
        raise ValueError(f"No round_* directories found in {run_dir}")
    return round_ids[-1]


def next_round_id(round_id: str) -> str:
    require_round_id(round_id)
    number = int(round_id.split("-")[1])
    return f"round-{number + 1:03d}"


def tasks_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "tasks.json"


def mission_path(run_dir: Path) -> Path:
    return run_dir / "mission.json"


def task_review_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_task_review_prompt.txt"


def fetch_plan_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "fetch_plan.json"


def report_draft_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / f"{role}_report_draft.json"


def report_prompt_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "openclaw_report_prompt.txt"


def report_packet_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "derived" / "report_packet.json"


def decision_draft_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "council_decision_draft.json"


def decision_prompt_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "openclaw_decision_prompt.txt"


def decision_packet_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "decision_packet.json"


def decision_target_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "council_decision.json"


def supervisor_dir(run_dir: Path) -> Path:
    return run_dir / "supervisor"


def supervisor_state_path(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "state.json"


def supervisor_sessions_dir(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "sessions"


def supervisor_outbox_dir(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "outbox"


def supervisor_responses_dir(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "responses"


def supervisor_current_step_path(run_dir: Path) -> Path:
    return supervisor_dir(run_dir) / "CURRENT_STEP.txt"


def response_base_path(run_dir: Path, round_id: str, role: str, kind: str) -> Path:
    safe_kind = kind.replace("-", "_")
    return supervisor_responses_dir(run_dir) / f"{round_id}_{role}_{safe_kind}"


def extract_json_suffix(text: str) -> Any:
    clean = text.strip()
    if not clean:
        raise ValueError("Expected JSON output but command returned nothing.")
    for index, char in enumerate(clean):
        if char not in "[{":
            continue
        candidate = clean[index:]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    raise ValueError(f"Command output did not contain parseable JSON:\n{clean}")


def run_json_command(argv: list[str], *, cwd: Path | None = None) -> Any:
    completed = subprocess.run(
        argv,
        cwd=str(cwd) if cwd is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "Command failed:\n"
            + " ".join(argv)
            + "\nSTDOUT:\n"
            + completed.stdout
            + "\nSTDERR:\n"
            + completed.stderr
        )
    return extract_json_suffix(completed.stdout)


def run_check_command(argv: list[str], *, cwd: Path | None = None) -> None:
    completed = subprocess.run(
        argv,
        cwd=str(cwd) if cwd is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "Command failed:\n"
            + " ".join(argv)
            + "\nSTDOUT:\n"
            + completed.stdout
            + "\nSTDERR:\n"
            + completed.stderr
        )


def load_state(run_dir: Path) -> dict[str, Any]:
    path = supervisor_state_path(run_dir)
    if not path.exists():
        raise ValueError(f"Supervisor state not found: {path}")
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"Supervisor state is not a JSON object: {path}")
    return payload


def save_state(run_dir: Path, state: dict[str, Any]) -> None:
    state["updated_at_utc"] = utc_now_iso()
    refresh_supervisor_files(run_dir, state)
    write_json(supervisor_state_path(run_dir), state, pretty=True)


def normalize_agent_prefix(value: str) -> str:
    text = AGENT_ID_SAFE.sub("-", value.strip().lower()).strip("-")
    return text or "eco-council"


def openclaw_workspace_root(run_dir: Path, state: dict[str, Any]) -> Path:
    configured = maybe_text(state.get("openclaw", {}).get("workspace_root"))
    if configured:
        return Path(configured).expanduser().resolve()
    return supervisor_dir(run_dir) / "openclaw-workspaces"


def session_prompt_path(run_dir: Path, role: str) -> Path:
    return supervisor_sessions_dir(run_dir) / f"{role}_session_prompt.txt"


def outbox_message_path(run_dir: Path, name: str) -> Path:
    return supervisor_outbox_dir(run_dir) / f"{name}.txt"


def role_display_name(role: str) -> str:
    return {
        "moderator": "Moderator",
        "sociologist": "Sociologist",
        "environmentalist": "Environmentalist",
    }[role]


def session_prompt_text(*, role: str, agent_id: str) -> str:
    header = [
        f"You are the fixed {role_display_name(role)} agent for this eco-council workflow.",
        f"OpenClaw agent id: {agent_id}",
        "",
        "Role rules:",
    ]
    if role == "moderator":
        rules = [
            "1. Stay in role for the full run.",
            "2. Only work on the JSON file/object explicitly requested by the supervisor.",
            "3. For task review turns, return only a JSON list of round-task objects.",
            "4. For decision turns, return only one JSON object shaped like council-decision.",
            "5. Never add markdown, prose, or code fences.",
            "6. If a referenced local skill is unavailable in this OpenClaw instance, follow the referenced file as the source of truth anyway.",
        ]
    else:
        rules = [
            "1. Stay in role for the full run.",
            "2. Only work on the report packet or prompt explicitly requested by the supervisor.",
            "3. Return only one JSON object shaped like expert-report.",
            "4. Never add markdown, prose, or code fences.",
            "5. Do not invent new raw data fetch results in the report stage.",
            "6. If a referenced local skill is unavailable in this OpenClaw instance, follow the referenced file as the source of truth anyway.",
        ]
    return "\n".join(header + rules)


def role_prompt_outbox_text(*, role: str, round_id: str, prompt_path: Path) -> str:
    lines = [
        f"This is your current eco-council turn for {round_id}.",
        "",
        "Open and follow this file exactly:",
        str(prompt_path),
        "",
        "If this OpenClaw instance cannot open local files directly, ask the human to paste the file contents and then continue.",
        "Return only JSON.",
    ]
    if role == "moderator":
        lines.insert(0, "Use your moderator session rules.")
    else:
        lines.insert(0, f"Use your {role} session rules.")
    return "\n".join(lines)


def build_current_step_text(run_dir: Path, state: dict[str, Any]) -> str:
    round_id = maybe_text(state.get("current_round_id"))
    stage = maybe_text(state.get("stage"))
    lines = [
        f"Current round: {round_id}",
        f"Current stage: {stage}",
        "",
    ]
    if stage == STAGE_AWAITING_TASK_REVIEW:
        lines.extend(
            [
                "Preferred: run the moderator turn automatically:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " run-agent-step --run-dir "
                + str(run_dir)
                + " --pretty",
                "",
                "Manual fallback:",
                "1. Open the moderator session prompt:",
                str(session_prompt_path(run_dir, "moderator")),
                "",
                "2. Send this turn prompt to the moderator agent:",
                str(outbox_message_path(run_dir, "moderator_task_review")),
                "",
                "3. Save the moderator JSON reply to any local file, then import it:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " import-task-review --run-dir "
                + str(run_dir)
                + " --input /path/to/moderator_tasks.json --pretty",
            ]
        )
    elif stage == STAGE_READY_PREPARE:
        lines.extend(
            [
                "Run the next approved shell stage:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " continue-run --run-dir "
                + str(run_dir)
                + " --pretty",
            ]
        )
    elif stage == STAGE_READY_FETCH:
        lines.extend(
            [
                "Run the local raw-data fetch plan:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " continue-run --run-dir "
                + str(run_dir)
                + " --pretty",
            ]
        )
    elif stage == STAGE_READY_DATA_PLANE:
        lines.extend(
            [
                "Run normalization and draft generation:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " continue-run --run-dir "
                + str(run_dir)
                + " --pretty",
            ]
        )
    elif stage == STAGE_AWAITING_REPORTS:
        lines.extend(
            [
                "Preferred: run the two expert turns automatically, one by one:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " run-agent-step --run-dir "
                + str(run_dir)
                + " --role sociologist --pretty",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " run-agent-step --run-dir "
                + str(run_dir)
                + " --role environmentalist --pretty",
                "",
                "Manual fallback:",
                "1. Open the sociologist session prompt:",
                str(session_prompt_path(run_dir, "sociologist")),
                "",
                "2. Send this turn prompt to the sociologist agent:",
                str(outbox_message_path(run_dir, "sociologist_report")),
                "",
                "3. Import the returned JSON:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " import-report --run-dir "
                + str(run_dir)
                + " --role sociologist --input /path/to/sociologist_report.json --pretty",
                "",
                "4. Repeat the same pattern for the environmentalist:",
                str(session_prompt_path(run_dir, "environmentalist")),
                str(outbox_message_path(run_dir, "environmentalist_report")),
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " import-report --run-dir "
                + str(run_dir)
                + " --role environmentalist --input /path/to/environmentalist_report.json --pretty",
            ]
        )
    elif stage == STAGE_AWAITING_DECISION:
        lines.extend(
            [
                "Preferred: run the moderator decision turn automatically:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " run-agent-step --run-dir "
                + str(run_dir)
                + " --pretty",
                "",
                "Manual fallback:",
                "1. Open the moderator session prompt:",
                str(session_prompt_path(run_dir, "moderator")),
                "",
                "2. Send this decision turn prompt to the moderator agent:",
                str(outbox_message_path(run_dir, "moderator_decision")),
                "",
                "3. Import the returned JSON:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " import-decision --run-dir "
                + str(run_dir)
                + " --input /path/to/council_decision.json --pretty",
            ]
        )
    elif stage == STAGE_READY_PROMOTE:
        lines.extend(
            [
                "Promote the approved drafts into canonical files:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " continue-run --run-dir "
                + str(run_dir)
                + " --pretty",
            ]
        )
    elif stage == STAGE_READY_ADVANCE:
        lines.extend(
            [
                "Open the next round after approval:",
                "python3 "
                + str(SCRIPT_DIR / "eco_council_supervisor.py")
                + " continue-run --run-dir "
                + str(run_dir)
                + " --pretty",
            ]
        )
    else:
        lines.append("Run completed. No further action is required.")
    return "\n".join(lines)


def refresh_supervisor_files(run_dir: Path, state: dict[str, Any]) -> None:
    run_dir = run_dir.expanduser().resolve()
    current_round_id = maybe_text(state.get("current_round_id"))
    if not current_round_id:
        return

    openclaw_section = state.setdefault("openclaw", {})
    agents = openclaw_section.setdefault("agents", {})
    prefix = normalize_agent_prefix(maybe_text(openclaw_section.get("agent_prefix")) or run_dir.name)

    for role in ROLES:
        role_agent = agents.setdefault(role, {})
        role_agent.setdefault("id", f"{prefix}-{role}")
        write_text(
            session_prompt_path(run_dir, role),
            session_prompt_text(role=role, agent_id=maybe_text(role_agent.get("id"))),
        )

    outbox_dir = supervisor_outbox_dir(run_dir)
    outbox_dir.mkdir(parents=True, exist_ok=True)
    for name in ("moderator_task_review", "sociologist_report", "environmentalist_report", "moderator_decision"):
        path = outbox_message_path(run_dir, name)
        if path.exists():
            path.unlink()

    stage = maybe_text(state.get("stage"))
    if stage == STAGE_AWAITING_TASK_REVIEW:
        write_text(
            outbox_message_path(run_dir, "moderator_task_review"),
            role_prompt_outbox_text(
                role="moderator",
                round_id=current_round_id,
                prompt_path=task_review_prompt_path(run_dir, current_round_id),
            ),
        )
    if stage == STAGE_AWAITING_REPORTS:
        for role in REPORT_ROLES:
            write_text(
                outbox_message_path(run_dir, f"{role}_report"),
                role_prompt_outbox_text(
                    role=role,
                    round_id=current_round_id,
                    prompt_path=report_prompt_path(run_dir, current_round_id, role),
                ),
            )
    if stage == STAGE_AWAITING_DECISION:
        write_text(
            outbox_message_path(run_dir, "moderator_decision"),
            role_prompt_outbox_text(
                role="moderator",
                round_id=current_round_id,
                prompt_path=decision_prompt_path(run_dir, current_round_id),
            ),
        )

    write_text(supervisor_current_step_path(run_dir), build_current_step_text(run_dir, state))


def build_state_payload(*, run_dir: Path, round_id: str, agent_prefix: str) -> dict[str, Any]:
    prefix = normalize_agent_prefix(agent_prefix or run_dir.name)
    return {
        "schema_version": SCHEMA_VERSION,
        "run_dir": str(run_dir),
        "current_round_id": round_id,
        "stage": STAGE_AWAITING_TASK_REVIEW,
        "fetch_execution": "supervisor-local-shell",
        "imports": {
            "task_review_received": False,
            "report_roles_received": [],
            "decision_received": False,
        },
        "openclaw": {
            "agent_prefix": prefix,
            "workspace_root": str(supervisor_dir(run_dir) / "openclaw-workspaces"),
            "agents": {
                role: {
                    "id": f"{prefix}-{role}",
                    "workspace": str((supervisor_dir(run_dir) / "openclaw-workspaces" / role).resolve()),
                }
                for role in ROLES
            },
        },
        "updated_at_utc": utc_now_iso(),
    }


def build_status_payload(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    run_dir = run_dir.expanduser().resolve()
    round_id = maybe_text(state.get("current_round_id"))
    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    stage = maybe_text(state.get("stage"))
    stage_outboxes = {
        STAGE_AWAITING_TASK_REVIEW: ("moderator_task_review",),
        STAGE_AWAITING_REPORTS: ("sociologist_report", "environmentalist_report"),
        STAGE_AWAITING_DECISION: ("moderator_decision",),
    }.get(stage, ())

    outbox_paths: dict[str, str] = {}
    for name in stage_outboxes:
        path = outbox_message_path(run_dir, name)
        if path.exists():
            outbox_paths[name] = str(path)

    session_paths = {role: str(session_prompt_path(run_dir, role)) for role in ROLES}
    return {
        "schema_version": SCHEMA_VERSION,
        "run_dir": str(run_dir),
        "current_round_id": round_id,
        "stage": stage,
        "fetch_execution": maybe_text(state.get("fetch_execution")),
        "imports": {
            "task_review_received": bool(imports.get("task_review_received")),
            "report_roles_received": sorted(
                {maybe_text(role) for role in imports.get("report_roles_received", []) if maybe_text(role)}
            ),
            "decision_received": bool(imports.get("decision_received")),
        },
        "task_review_prompt_path": str(task_review_prompt_path(run_dir, round_id)),
        "fetch_plan_path": str(fetch_plan_path(run_dir, round_id)),
        "session_prompt_paths": session_paths,
        "outbox_paths": outbox_paths,
        "current_step_path": str(supervisor_current_step_path(run_dir)),
        "openclaw": state.get("openclaw", {}),
    }


def ask_for_approval(summary: str, *, assume_yes: bool) -> bool:
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        raise ValueError("Approval is required. Rerun in a terminal or pass --yes.")
    reply = input(f"{summary}\nContinue? [y/N]: ").strip().lower()
    return reply in {"y", "yes"}


def validate_input_file(kind: str, input_path: Path) -> None:
    run_check_command(
        [
            "python3",
            str(CONTRACT_SCRIPT),
            "validate",
            "--kind",
            kind,
            "--input",
            str(input_path),
        ],
        cwd=REPO_DIR,
    )


def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def ensure_task_review_matches(payload: Any, *, round_id: str) -> None:
    if not isinstance(payload, list):
        raise ValueError("Task review payload must be a JSON list.")
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("Each round task must be a JSON object.")
        item_round_id = maybe_text(item.get("round_id"))
        if item_round_id and item_round_id != round_id:
            raise ValueError(f"Task round_id mismatch: expected {round_id}, got {item_round_id}")


def ensure_report_matches(payload: Any, *, round_id: str, role: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Report payload must be a JSON object.")
    payload_round_id = maybe_text(payload.get("round_id"))
    payload_role = maybe_text(payload.get("agent_role"))
    if payload_round_id and payload_round_id != round_id:
        raise ValueError(f"Report round_id mismatch: expected {round_id}, got {payload_round_id}")
    if payload_role and payload_role != role:
        raise ValueError(f"Report agent_role mismatch: expected {role}, got {payload_role}")


def ensure_decision_matches(payload: Any, *, round_id: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Decision payload must be a JSON object.")
    payload_round_id = maybe_text(payload.get("round_id"))
    if payload_round_id and payload_round_id != round_id:
        raise ValueError(f"Decision round_id mismatch: expected {round_id}, got {payload_round_id}")


def import_task_review_payload(*, run_dir: Path, state: dict[str, Any], payload: Any, source_path: Path) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_task_review_matches(payload, round_id=round_id)
    target = tasks_path(run_dir, round_id)
    write_json(target, payload, pretty=True)
    state["stage"] = STAGE_READY_PREPARE
    state["imports"] = {
        "task_review_received": True,
        "report_roles_received": [],
        "decision_received": False,
    }
    save_state(run_dir, state)
    return {
        "imported_kind": "round-task",
        "input_path": str(source_path),
        "target_path": str(target),
        "state": build_status_payload(run_dir, state),
    }


def import_report_payload(*, run_dir: Path, state: dict[str, Any], role: str, payload: Any, source_path: Path) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_report_matches(payload, round_id=round_id, role=role)
    target = report_draft_path(run_dir, round_id, role)
    write_json(target, payload, pretty=True)

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    received = {maybe_text(item) for item in imports.get("report_roles_received", []) if maybe_text(item)}
    received.add(role)
    imports["report_roles_received"] = sorted(received)
    state["imports"] = imports
    state["stage"] = STAGE_AWAITING_DECISION if received == set(REPORT_ROLES) else STAGE_AWAITING_REPORTS
    save_state(run_dir, state)
    return {
        "imported_kind": "expert-report",
        "role": role,
        "input_path": str(source_path),
        "target_path": str(target),
        "state": build_status_payload(run_dir, state),
    }


def import_decision_payload(*, run_dir: Path, state: dict[str, Any], payload: Any, source_path: Path) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    ensure_decision_matches(payload, round_id=round_id)
    target = decision_draft_path(run_dir, round_id)
    write_json(target, payload, pretty=True)

    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    imports["decision_received"] = True
    state["imports"] = imports
    state["stage"] = STAGE_READY_PROMOTE
    save_state(run_dir, state)
    return {
        "imported_kind": "council-decision",
        "input_path": str(source_path),
        "target_path": str(target),
        "state": build_status_payload(run_dir, state),
    }


def current_agent_turn(*, state: dict[str, Any], requested_role: str) -> tuple[str, str, str]:
    stage = maybe_text(state.get("stage"))
    imports = state.get("imports", {}) if isinstance(state.get("imports"), dict) else {}
    requested = maybe_text(requested_role)

    if stage == STAGE_AWAITING_TASK_REVIEW:
        if requested and requested != "moderator":
            raise ValueError("Current stage only accepts role=moderator.")
        return ("moderator", "task-review", "round-task")

    if stage == STAGE_AWAITING_DECISION:
        if requested and requested != "moderator":
            raise ValueError("Current stage only accepts role=moderator.")
        return ("moderator", "decision", "council-decision")

    if stage == STAGE_AWAITING_REPORTS:
        missing = [role for role in REPORT_ROLES if role not in {maybe_text(item) for item in imports.get("report_roles_received", [])}]
        if requested:
            if requested not in REPORT_ROLES:
                raise ValueError("Report stage requires role=sociologist or role=environmentalist.")
            if requested not in missing:
                raise ValueError(f"Role {requested} has already been imported for this round.")
            return (requested, "report", "expert-report")
        if len(missing) == 1:
            return (missing[0], "report", "expert-report")
        raise ValueError("Current stage needs --role sociologist or --role environmentalist.")

    raise ValueError(f"Current stage does not accept agent turns: {stage}")


def build_agent_message(*, run_dir: Path, state: dict[str, Any], role: str, turn_kind: str) -> str:
    round_id = maybe_text(state.get("current_round_id"))
    session_text = load_text(session_prompt_path(run_dir, role))

    if turn_kind == "task-review":
        prompt_text = load_text(task_review_prompt_path(run_dir, round_id))
        mission_text = load_text(mission_path(run_dir))
        tasks_text = load_text(tasks_path(run_dir, round_id))
        return "\n\n".join(
            [
                session_text,
                (
                    f"Current automated turn: moderator task review for {round_id}.\n"
                    "All referenced file contents are embedded below. Do not ask for filesystem access. "
                    "Return only the final JSON list."
                ),
                "=== TASK REVIEW PROMPT ===\n" + prompt_text,
                "=== MISSION.JSON ===\n" + mission_text,
                "=== CURRENT TASKS.JSON ===\n" + tasks_text,
            ]
        )

    if turn_kind == "report":
        prompt_text = load_text(report_prompt_path(run_dir, round_id, role))
        packet_text = load_text(report_packet_path(run_dir, round_id, role))
        return "\n\n".join(
            [
                session_text,
                (
                    f"Current automated turn: {role} report drafting for {round_id}.\n"
                    "The required packet content is embedded below. Do not ask for filesystem access. "
                    "Return only the final JSON object."
                ),
                "=== REPORT PROMPT ===\n" + prompt_text,
                "=== REPORT PACKET.JSON ===\n" + packet_text,
            ]
        )

    if turn_kind == "decision":
        prompt_text = load_text(decision_prompt_path(run_dir, round_id))
        packet_text = load_text(decision_packet_path(run_dir, round_id))
        return "\n\n".join(
            [
                session_text,
                (
                    f"Current automated turn: moderator decision drafting for {round_id}.\n"
                    "The required packet content is embedded below. Do not ask for filesystem access. "
                    "Return only the final JSON object."
                ),
                "=== DECISION PROMPT ===\n" + prompt_text,
                "=== DECISION PACKET.JSON ===\n" + packet_text,
            ]
        )

    raise ValueError(f"Unsupported agent turn kind: {turn_kind}")


def run_openclaw_agent_turn(
    *,
    run_dir: Path,
    state: dict[str, Any],
    role: str,
    turn_kind: str,
    schema_kind: str,
    message: str,
    timeout_seconds: int,
    thinking: str,
) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    agent_id = maybe_text(state.get("openclaw", {}).get("agents", {}).get(role, {}).get("id"))
    if not agent_id:
        raise ValueError(f"No configured OpenClaw agent id for role={role}")

    base_path = response_base_path(run_dir, round_id, role, turn_kind)
    stdout_path = base_path.with_suffix(".stdout.txt")
    stderr_path = base_path.with_suffix(".stderr.txt")
    json_path = base_path.with_suffix(".json")
    stdout_path.parent.mkdir(parents=True, exist_ok=True)

    argv = [
        "openclaw",
        "--no-color",
        "agent",
        "--agent",
        agent_id,
        "--local",
        "--message",
        message,
        "--timeout",
        str(timeout_seconds),
    ]
    if thinking:
        argv.extend(["--thinking", thinking])

    completed = subprocess.run(
        argv,
        cwd=str(REPO_DIR),
        capture_output=True,
        text=True,
        check=False,
    )
    stdout_path.write_text(completed.stdout, encoding="utf-8")
    stderr_path.write_text(completed.stderr, encoding="utf-8")
    if completed.returncode != 0:
        raise RuntimeError(
            f"OpenClaw agent turn failed for role={role}. "
            f"See {stdout_path} and {stderr_path}."
        )

    payload = extract_json_suffix(completed.stdout)
    write_json(json_path, payload, pretty=True)
    validate_input_file(schema_kind, json_path)
    return {
        "agent_id": agent_id,
        "role": role,
        "turn_kind": turn_kind,
        "schema_kind": schema_kind,
        "response_json_path": str(json_path),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "payload": payload,
    }


def command_init_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    mission_input = Path(args.mission_input).expanduser().resolve()
    run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "bootstrap-run",
            "--run-dir",
            str(run_dir),
            "--mission-input",
            str(mission_input),
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    round_id = latest_round_id(run_dir)
    state = build_state_payload(run_dir=run_dir, round_id=round_id, agent_prefix=args.agent_prefix)
    save_state(run_dir, state)
    return build_status_payload(run_dir, state)


def command_status(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    state = load_state(run_dir)
    refresh_supervisor_files(run_dir, state)
    write_json(supervisor_state_path(run_dir), state, pretty=True)
    return build_status_payload(run_dir, state)


def continue_prepare_round(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "prepare-round",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    state["stage"] = STAGE_READY_FETCH
    save_state(run_dir, state)
    return {"action": "prepare-round", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_execute_fetch(run_dir: Path, state: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "execute-fetch-plan",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--timeout-seconds",
            str(timeout_seconds),
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    failures = [
        item
        for item in payload.get("statuses", [])
        if isinstance(item, dict) and maybe_text(item.get("status")) == "failed"
    ]
    if failures:
        raise RuntimeError(f"Fetch plan reported failed steps. Inspect stderr paths: {failures}")
    state["stage"] = STAGE_READY_DATA_PLANE
    save_state(run_dir, state)
    return {"action": "execute-fetch-plan", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_run_data_plane(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "run-data-plane",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    state["stage"] = STAGE_AWAITING_REPORTS
    state["imports"] = {
        "task_review_received": True,
        "report_roles_received": [],
        "decision_received": False,
    }
    save_state(run_dir, state)
    return {"action": "run-data-plane", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_promote(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT),
            "promote-all",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    decision_payload = read_json(decision_target_path(run_dir, round_id))
    if not isinstance(decision_payload, dict):
        raise ValueError("Canonical moderator decision is not a JSON object after promote-all.")
    if bool(decision_payload.get("next_round_required")):
        state["stage"] = STAGE_READY_ADVANCE
    else:
        state["stage"] = STAGE_COMPLETED
    save_state(run_dir, state)
    return {"action": "promote-all", "payload": payload, "state": build_status_payload(run_dir, state)}


def continue_advance_round(run_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    round_id = maybe_text(state.get("current_round_id"))
    payload = run_json_command(
        [
            "python3",
            str(ORCHESTRATE_SCRIPT),
            "advance-round",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    new_round_id = latest_round_id(run_dir)
    state["current_round_id"] = new_round_id
    state["stage"] = STAGE_AWAITING_TASK_REVIEW
    state["imports"] = {
        "task_review_received": False,
        "report_roles_received": [],
        "decision_received": False,
    }
    save_state(run_dir, state)
    return {"action": "advance-round", "payload": payload, "state": build_status_payload(run_dir, state)}


def command_continue_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    state = load_state(run_dir)
    stage = maybe_text(state.get("stage"))
    action_map = {
        STAGE_READY_PREPARE: ("prepare-round", continue_prepare_round),
        STAGE_READY_FETCH: ("execute-fetch-plan", lambda d, s: continue_execute_fetch(d, s, args.timeout_seconds)),
        STAGE_READY_DATA_PLANE: ("run-data-plane", continue_run_data_plane),
        STAGE_READY_PROMOTE: ("promote-all", continue_promote),
        STAGE_READY_ADVANCE: ("advance-round", continue_advance_round),
    }
    action = action_map.get(stage)
    if action is None:
        raise ValueError(f"Current stage does not accept continue-run: {stage}")
    action_name, handler = action
    approved = ask_for_approval(
        f"About to run stage {action_name} for {maybe_text(state.get('current_round_id'))}.",
        assume_yes=args.yes,
    )
    if not approved:
        return {
            "approved": False,
            "stage": stage,
            "state": build_status_payload(run_dir, state),
        }
    result = handler(run_dir, state)
    result["approved"] = True
    return result


def command_import_task_review(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    state = load_state(run_dir)
    if maybe_text(state.get("stage")) != STAGE_AWAITING_TASK_REVIEW:
        raise ValueError("import-task-review is only allowed while waiting for moderator task review.")
    round_id = maybe_text(state.get("current_round_id"))
    validate_input_file("round-task", input_path)
    payload = read_json(input_path)
    return import_task_review_payload(run_dir=run_dir, state=state, payload=payload, source_path=input_path)


def command_import_report(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    role = args.role
    state = load_state(run_dir)
    if maybe_text(state.get("stage")) not in {STAGE_AWAITING_REPORTS, STAGE_AWAITING_DECISION}:
        raise ValueError("import-report is only allowed while waiting for expert reports.")
    round_id = maybe_text(state.get("current_round_id"))
    validate_input_file("expert-report", input_path)
    payload = read_json(input_path)
    return import_report_payload(run_dir=run_dir, state=state, role=role, payload=payload, source_path=input_path)


def command_import_decision(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    input_path = Path(args.input).expanduser().resolve()
    state = load_state(run_dir)
    if maybe_text(state.get("stage")) != STAGE_AWAITING_DECISION:
        raise ValueError("import-decision is only allowed while waiting for the moderator decision.")
    round_id = maybe_text(state.get("current_round_id"))
    validate_input_file("council-decision", input_path)
    payload = read_json(input_path)
    return import_decision_payload(run_dir=run_dir, state=state, payload=payload, source_path=input_path)


def command_run_agent_step(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    state = load_state(run_dir)
    role, turn_kind, schema_kind = current_agent_turn(state=state, requested_role=args.role)
    approved = ask_for_approval(
        f"About to run OpenClaw agent turn {turn_kind} for role={role} in {maybe_text(state.get('current_round_id'))}.",
        assume_yes=args.yes,
    )
    if not approved:
        return {
            "approved": False,
            "state": build_status_payload(run_dir, state),
        }

    message = build_agent_message(run_dir=run_dir, state=state, role=role, turn_kind=turn_kind)
    result = run_openclaw_agent_turn(
        run_dir=run_dir,
        state=state,
        role=role,
        turn_kind=turn_kind,
        schema_kind=schema_kind,
        message=message,
        timeout_seconds=args.timeout_seconds,
        thinking=args.thinking,
    )
    response_path = Path(result["response_json_path"]).resolve()
    payload = result["payload"]
    if schema_kind == "round-task":
        imported = import_task_review_payload(run_dir=run_dir, state=state, payload=payload, source_path=response_path)
    elif schema_kind == "expert-report":
        imported = import_report_payload(run_dir=run_dir, state=state, role=role, payload=payload, source_path=response_path)
    elif schema_kind == "council-decision":
        imported = import_decision_payload(run_dir=run_dir, state=state, payload=payload, source_path=response_path)
    else:
        raise ValueError(f"Unsupported schema kind: {schema_kind}")

    return {
        "approved": True,
        "agent_turn": {
            "agent_id": result["agent_id"],
            "role": role,
            "turn_kind": turn_kind,
            "response_json_path": result["response_json_path"],
            "stdout_path": result["stdout_path"],
            "stderr_path": result["stderr_path"],
        },
        "import_result": imported,
    }


def existing_openclaw_agents() -> dict[str, dict[str, Any]]:
    payload = run_json_command(["openclaw", "agents", "list", "--json"], cwd=REPO_DIR)
    if not isinstance(payload, list):
        raise ValueError("Unexpected openclaw agents list payload.")
    output: dict[str, dict[str, Any]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        agent_id = maybe_text(item.get("id"))
        if agent_id:
            output[agent_id] = item
    return output


def identity_text(*, role: str, agent_id: str) -> str:
    values = {
        "moderator": {
            "name": "Eco Council Moderator",
            "creature": "procedural council chair",
            "vibe": "skeptical, structured, concise",
            "emoji": "gavel",
        },
        "sociologist": {
            "name": "Eco Council Sociologist",
            "creature": "public-opinion analyst",
            "vibe": "evidence-led, careful, restrained",
            "emoji": "speech",
        },
        "environmentalist": {
            "name": "Eco Council Environmentalist",
            "creature": "physical-signal analyst",
            "vibe": "technical, methodical, cautious",
            "emoji": "globe",
        },
    }[role]
    return "\n".join(
        [
            "# IDENTITY.md - Who Am I?",
            "",
            f"- **Name:** {values['name']}",
            f"- **Creature:** {values['creature']}",
            f"- **Vibe:** {values['vibe']}",
            f"- **Emoji:** {values['emoji']}",
            "- **Avatar:**",
            "",
            f"Agent id: {agent_id}",
        ]
    )


def ensure_openclaw_agent(run_dir: Path, *, role: str, state: dict[str, Any]) -> dict[str, Any]:
    openclaw_section = state.setdefault("openclaw", {})
    agents = openclaw_section.setdefault("agents", {})
    role_info = agents.setdefault(role, {})
    agent_id = maybe_text(role_info.get("id"))
    if not agent_id:
        raise ValueError(f"Missing configured agent id for role {role}")
    workspace = Path(maybe_text(role_info.get("workspace"))).expanduser().resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    write_text(workspace / "IDENTITY.md", identity_text(role=role, agent_id=agent_id))

    current_agents = existing_openclaw_agents()
    if agent_id not in current_agents:
        run_json_command(
            [
                "openclaw",
                "agents",
                "add",
                agent_id,
                "--workspace",
                str(workspace),
                "--non-interactive",
                "--json",
            ],
            cwd=REPO_DIR,
        )
    run_json_command(
        [
            "openclaw",
            "agents",
            "set-identity",
            "--agent",
            agent_id,
            "--workspace",
            str(workspace),
            "--from-identity",
            "--json",
        ],
        cwd=REPO_DIR,
    )
    role_info["workspace"] = str(workspace)
    return {"role": role, "agent_id": agent_id, "workspace": str(workspace)}


def command_provision_openclaw_agents(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    state = load_state(run_dir)
    workspace_root = Path(args.workspace_root).expanduser().resolve() if args.workspace_root else openclaw_workspace_root(run_dir, state)
    state.setdefault("openclaw", {})["workspace_root"] = str(workspace_root)
    for role in ROLES:
        state.setdefault("openclaw", {}).setdefault("agents", {}).setdefault(role, {})["workspace"] = str(
            (workspace_root / role).resolve()
        )

    approved = ask_for_approval(
        "About to create or reuse three OpenClaw isolated agents for moderator/sociologist/environmentalist.",
        assume_yes=args.yes,
    )
    if not approved:
        return {
            "approved": False,
            "state": build_status_payload(run_dir, state),
        }

    created = [ensure_openclaw_agent(run_dir, role=role, state=state) for role in ROLES]
    save_state(run_dir, state)
    return {
        "approved": True,
        "created_agents": created,
        "state": build_status_payload(run_dir, state),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run an eco-council workflow with approval gates.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_run = sub.add_parser("init-run", help="Bootstrap a run and create supervisor state.")
    init_run.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    init_run.add_argument("--mission-input", required=True, help="Mission JSON file.")
    init_run.add_argument("--agent-prefix", default="", help="Optional OpenClaw agent id prefix.")
    init_run.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    provision = sub.add_parser("provision-openclaw-agents", help="Create or reuse three isolated OpenClaw agents.")
    provision.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    provision.add_argument("--workspace-root", default="", help="Optional workspace root for the three agents.")
    provision.add_argument("--yes", action="store_true", help="Skip interactive approval.")
    provision.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    status = sub.add_parser("status", help="Show current supervisor state.")
    status.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    status.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    continue_run = sub.add_parser("continue-run", help="Run the next approved local shell stage.")
    continue_run.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    continue_run.add_argument("--timeout-seconds", type=int, default=600, help="Timeout for execute-fetch-plan.")
    continue_run.add_argument("--yes", action="store_true", help="Skip interactive approval.")
    continue_run.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    run_agent = sub.add_parser("run-agent-step", help="Send the current turn to OpenClaw, receive JSON, and import it.")
    run_agent.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    run_agent.add_argument("--role", default="", choices=("", "moderator", "sociologist", "environmentalist"), help="Optional role override for expert-report stages.")
    run_agent.add_argument("--timeout-seconds", type=int, default=600, help="OpenClaw agent timeout.")
    run_agent.add_argument("--thinking", default="low", choices=("off", "minimal", "low", "medium", "high"), help="OpenClaw thinking level.")
    run_agent.add_argument("--yes", action="store_true", help="Skip interactive approval.")
    run_agent.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_task = sub.add_parser("import-task-review", help="Import moderator task-review JSON into tasks.json.")
    import_task.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_task.add_argument("--input", required=True, help="JSON file returned by the moderator.")
    import_task.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_report = sub.add_parser("import-report", help="Import one expert-report JSON into the draft path.")
    import_report.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_report.add_argument("--role", required=True, choices=REPORT_ROLES, help="Expert role.")
    import_report.add_argument("--input", required=True, help="JSON file returned by the expert agent.")
    import_report.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_decision = sub.add_parser("import-decision", help="Import moderator decision JSON into the draft path.")
    import_decision.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_decision.add_argument("--input", required=True, help="JSON file returned by the moderator.")
    import_decision.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "init-run": command_init_run,
        "provision-openclaw-agents": command_provision_openclaw_agents,
        "status": command_status,
        "continue-run": command_continue_run,
        "run-agent-step": command_run_agent_step,
        "import-task-review": command_import_task_review,
        "import-report": command_import_report,
        "import-decision": command_import_decision,
    }
    try:
        payload = handlers[args.command](args)
    except Exception as exc:  # noqa: BLE001
        print(pretty_json({"ok": False, "error": str(exc)}, pretty=True))
        return 1
    print(pretty_json(payload, pretty=bool(getattr(args, "pretty", False))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
