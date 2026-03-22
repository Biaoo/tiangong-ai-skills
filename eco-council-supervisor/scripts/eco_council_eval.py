#!/usr/bin/env python3
"""Deterministic replay and evaluation runner for eco-council fixtures."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
REPO_DIR = SKILL_DIR.parent
NORMALIZE_SCRIPT = REPO_DIR / "eco-council-normalize" / "scripts" / "eco_council_normalize.py"
REPORTING_SCRIPT = REPO_DIR / "eco-council-reporting" / "scripts" / "eco_council_reporting.py"
CONTRACT_SCRIPT = REPO_DIR / "eco-council-data-contract" / "scripts" / "eco_council_contract.py"
DEFAULT_SUITE_DIR = SKILL_DIR / "assets" / "eval-cases"
DEFAULT_OUTPUT_ROOT = Path("/tmp/eco-council-eval-runs")


def pretty_json(data: Any, *, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any, *, pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(pretty_json(payload, pretty=pretty) + "\n", encoding="utf-8")


def round_directory_name(round_id: str) -> str:
    return round_id.replace("-", "_")


def round_dir(run_dir: Path, round_id: str) -> Path:
    return run_dir / round_directory_name(round_id)


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


def run_json_command(argv: list[str]) -> Any:
    completed = subprocess.run(
        argv,
        cwd=str(REPO_DIR),
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


def load_case(case_path: Path) -> dict[str, Any]:
    payload = read_json(case_path)
    if not isinstance(payload, dict):
        raise ValueError(f"Case fixture must be a JSON object: {case_path}")
    return payload


def materialize_case(case: dict[str, Any], run_dir: Path, *, pretty: bool) -> str:
    mission = case.get("mission")
    if not isinstance(mission, dict):
        raise ValueError("Case fixture missing mission.")
    round_id = maybe_text(case.get("round_id"))
    if not round_id:
        raise ValueError("Case fixture missing round_id.")

    write_json(run_dir / "mission.json", mission, pretty=pretty)
    base_round = round_dir(run_dir, round_id)
    write_json(base_round / "moderator" / "tasks.json", case.get("tasks", []), pretty=pretty)
    write_json(base_round / "shared" / "claims.json", case.get("claims", []), pretty=pretty)
    write_json(base_round / "shared" / "observations.json", case.get("observations", []), pretty=pretty)
    write_json(base_round / "shared" / "evidence_cards.json", case.get("evidence_cards", []), pretty=pretty)
    write_json(
        base_round / "moderator" / "derived" / "fetch_execution.json",
        {"statuses": case.get("fetch_statuses", [])},
        pretty=pretty,
    )
    return round_id


def evaluate_expectations(case: dict[str, Any], run_dir: Path, round_id: str) -> list[str]:
    issues: list[str] = []
    expect = case.get("expect")
    if not isinstance(expect, dict):
        return issues

    decision_expect = expect.get("decision")
    if isinstance(decision_expect, dict):
        decision = read_json(round_dir(run_dir, round_id) / "moderator" / "council_decision.json")
        for key, expected in decision_expect.items():
            actual = decision.get(key)
            if isinstance(expected, list):
                actual_list = actual if isinstance(actual, list) else []
                if sorted(maybe_text(item) for item in actual_list) != sorted(maybe_text(item) for item in expected):
                    issues.append(f"decision.{key}: expected {expected!r}, got {actual!r}")
            else:
                if actual != expected:
                    issues.append(f"decision.{key}: expected {expected!r}, got {actual!r}")

    context_expect = expect.get("context")
    if isinstance(context_expect, dict):
        context = read_json(round_dir(run_dir, round_id) / "moderator" / "derived" / "context_moderator.json")
        if "context_layer" in context_expect and context.get("context_layer") != context_expect["context_layer"]:
            issues.append(
                f"context.context_layer: expected {context_expect['context_layer']!r}, got {context.get('context_layer')!r}"
            )
        max_observations = context_expect.get("max_observations")
        if isinstance(max_observations, int) and len(context.get("observations", [])) > max_observations:
            issues.append(
                f"context.observations length expected <= {max_observations}, got {len(context.get('observations', []))}"
            )
        max_claims = context_expect.get("max_claims")
        if isinstance(max_claims, int) and len(context.get("claims", [])) > max_claims:
            issues.append(f"context.claims length expected <= {max_claims}, got {len(context.get('claims', []))}")
    return issues


def run_case(case_path: Path, *, output_root: Path, pretty: bool, overwrite: bool) -> dict[str, Any]:
    case = load_case(case_path)
    case_id = maybe_text(case.get("case_id")) or case_path.stem
    run_dir_path = output_root / case_id
    if run_dir_path.exists():
        if not overwrite:
            raise ValueError(f"Case output already exists: {run_dir_path}")
        shutil.rmtree(run_dir_path)
    run_dir_path.mkdir(parents=True, exist_ok=True)

    round_id = materialize_case(case, run_dir_path, pretty=pretty)
    run_json_command(["python3", str(NORMALIZE_SCRIPT), "init-run", "--run-dir", str(run_dir_path), "--round-id", round_id, "--pretty"])
    run_json_command(["python3", str(NORMALIZE_SCRIPT), "build-round-context", "--run-dir", str(run_dir_path), "--round-id", round_id, "--pretty"])
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT),
            "build-all",
            "--run-dir",
            str(run_dir_path),
            "--round-id",
            round_id,
            "--prefer-draft-reports",
            "--pretty",
        ]
    )
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT),
            "promote-all",
            "--run-dir",
            str(run_dir_path),
            "--round-id",
            round_id,
            "--allow-overwrite",
            "--pretty",
        ]
    )
    bundle = run_json_command(["python3", str(CONTRACT_SCRIPT), "validate-bundle", "--run-dir", str(run_dir_path)])
    issues = evaluate_expectations(case, run_dir_path, round_id)
    return {
        "case_id": case_id,
        "description": maybe_text(case.get("description")),
        "round_id": round_id,
        "run_dir": str(run_dir_path),
        "bundle_ok": bool(bundle.get("ok")) if isinstance(bundle, dict) else False,
        "issues": issues,
        "passed": (bool(bundle.get("ok")) if isinstance(bundle, dict) else False) and not issues,
    }


def collect_case_paths(suite_dir: Path, case_id: str) -> list[Path]:
    cases = sorted(path for path in suite_dir.glob("*.json") if path.is_file())
    if case_id:
        cases = [path for path in cases if path.stem == case_id]
    if not cases:
        raise ValueError(f"No case fixtures found in {suite_dir} for case_id={case_id!r}.")
    return cases


def command_run_suite(args: argparse.Namespace) -> dict[str, Any]:
    suite_dir = Path(args.suite_dir).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    results = [
        run_case(path, output_root=output_root, pretty=args.pretty, overwrite=args.overwrite)
        for path in collect_case_paths(suite_dir, args.case_id)
    ]
    passed = sum(1 for item in results if item.get("passed"))
    return {
        "suite_dir": str(suite_dir),
        "output_root": str(output_root),
        "case_count": len(results),
        "passed_count": passed,
        "failed_count": len(results) - passed,
        "results": results,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay deterministic eco-council eval fixtures.")
    sub = parser.add_subparsers(dest="command", required=True)

    run_suite = sub.add_parser("run-suite", help="Run all eval fixtures in a suite directory.")
    run_suite.add_argument("--suite-dir", default=str(DEFAULT_SUITE_DIR), help="Eval fixture directory.")
    run_suite.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Output root for replayed runs.")
    run_suite.add_argument("--case-id", default="", help="Optional single case id to run.")
    run_suite.add_argument("--overwrite", action="store_true", help="Overwrite existing replay output directories.")
    run_suite.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {"run-suite": command_run_suite}
    try:
        payload = handlers[args.command](args)
    except Exception as exc:  # noqa: BLE001
        print(pretty_json({"command": args.command, "ok": False, "error": str(exc)}, pretty=getattr(args, "pretty", False)))
        return 1
    print(pretty_json({"command": args.command, "ok": True, "payload": payload}, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
