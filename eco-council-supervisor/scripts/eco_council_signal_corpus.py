#!/usr/bin/env python3
"""Cross-run SQLite signal corpus for eco-council normalized artifacts."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
DDL_PATH = SKILL_DIR / "assets" / "sqlite" / "eco_council_signal_corpus.sql"
SUPERVISOR_PATH = SKILL_DIR / "scripts" / "eco_council_supervisor.py"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty_json(data: Any, *, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def maybe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def bool_int(value: Any) -> int:
    return 1 if bool(value) else 0


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return read_json(path)


def load_supervisor_module() -> Any:
    module_name = "eco_council_supervisor_signal_corpus"
    spec = importlib.util.spec_from_file_location(module_name, SUPERVISOR_PATH)
    if spec is None or spec.loader is None:
        raise ValueError(f"Unable to load supervisor module from {SUPERVISOR_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


SUP = load_supervisor_module()


def read_ddl() -> str:
    return DDL_PATH.read_text(encoding="utf-8")


def connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(path: Path) -> None:
    with connect_db(path) as conn:
        conn.executescript(read_ddl())
        conn.commit()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def insert_many(conn: sqlite3.Connection, sql: str, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    conn.executemany(sql, rows)


def state_for_run(run_dir: Path) -> dict[str, Any]:
    state_path = SUP.supervisor_state_path(run_dir)
    if state_path.exists():
        return SUP.load_state(run_dir)
    round_ids = SUP.discover_round_ids(run_dir)
    return {
        "current_round_id": round_ids[-1] if round_ids else "",
        "stage": "",
    }


def default_public_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "public_signals.sqlite"


def default_environment_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "environment_signals.sqlite"


def run_manifest_path(run_dir: Path) -> Path:
    return run_dir / "run_manifest.json"


def resolve_manifest_db_path(run_dir: Path, value: Any, fallback: Path) -> Path:
    text = maybe_text(value)
    if not text:
        return fallback.expanduser().resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def resolve_analytics_db_paths(run_dir: Path) -> tuple[Path, Path]:
    manifest = load_json_if_exists(run_manifest_path(run_dir))
    dbs = manifest.get("databases") if isinstance(manifest, dict) else {}
    public_value = dbs.get("public_signals") if isinstance(dbs, dict) else ""
    environment_value = dbs.get("environment_signals") if isinstance(dbs, dict) else ""
    return (
        resolve_manifest_db_path(run_dir, public_value, default_public_db_path(run_dir)),
        resolve_manifest_db_path(run_dir, environment_value, default_environment_db_path(run_dir)),
    )


def collect_run_snapshot(run_dir: Path) -> dict[str, Any]:
    mission = SUP.read_json(SUP.mission_path(run_dir))
    if not isinstance(mission, dict):
        raise ValueError(f"Invalid mission.json in {run_dir}")
    state = state_for_run(run_dir)
    round_ids = SUP.discover_round_ids(run_dir)
    round_summaries = [SUP.collect_round_summary(run_dir, state, round_id) for round_id in round_ids]
    current_round_id = maybe_text(state.get("current_round_id")) or (round_ids[-1] if round_ids else "")
    current_summary = next((item for item in round_summaries if item.get("round_id") == current_round_id), None)
    if current_summary is None and round_summaries:
        current_summary = round_summaries[-1]
    return {
        "mission": mission,
        "state": state,
        "round_ids": round_ids,
        "round_summaries": round_summaries,
        "current_summary": current_summary if isinstance(current_summary, dict) else {},
    }


def load_rows_for_run(db_path: Path, table_name: str, *, run_id: str, order_by: str) -> list[sqlite3.Row]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if not table_exists(conn, table_name):
            return []
        sql = f"SELECT * FROM {table_name} WHERE run_id = ? ORDER BY {order_by}"
        return conn.execute(sql, (run_id,)).fetchall()


def delete_run(conn: sqlite3.Connection, run_id: str) -> None:
    conn.execute("DELETE FROM corpus_runs WHERE run_id = ?", (run_id,))


def insert_run_metadata(
    conn: sqlite3.Connection,
    *,
    run_dir: Path,
    snapshot: dict[str, Any],
    public_db_path: Path,
    environment_db_path: Path,
    public_db_exists: bool,
    environment_db_exists: bool,
    public_signal_count: int,
    environment_signal_count: int,
    claim_candidate_count: int,
    observation_summary_count: int,
    imported_at_utc: str,
) -> str:
    mission = snapshot["mission"]
    state = snapshot["state"]
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    window = mission.get("window") if isinstance(mission.get("window"), dict) else {}
    run_id = maybe_text(mission.get("run_id"))
    if not run_id:
        raise ValueError("mission.run_id is required")
    conn.execute(
        """
        INSERT INTO corpus_runs (
            run_id, run_dir, topic, objective, region_label, region_geometry_json,
            window_start_utc, window_end_utc, current_round_id, current_stage, round_count,
            public_db_path, public_db_exists, environment_db_path, environment_db_exists,
            public_signal_count, environment_signal_count, claim_candidate_count, observation_summary_count,
            imported_at_utc, mission_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            str(run_dir),
            maybe_text(mission.get("topic")),
            maybe_text(mission.get("objective")),
            maybe_text(region.get("label")),
            json_text(region.get("geometry", {})),
            maybe_text(window.get("start_utc")),
            maybe_text(window.get("end_utc")),
            maybe_text(state.get("current_round_id")),
            maybe_text(state.get("stage")),
            len(snapshot.get("round_ids", [])),
            str(public_db_path),
            bool_int(public_db_exists),
            str(environment_db_path),
            bool_int(environment_db_exists),
            public_signal_count,
            environment_signal_count,
            claim_candidate_count,
            observation_summary_count,
            imported_at_utc,
            json_text(mission),
        ),
    )
    return run_id


def insert_round_summaries(conn: sqlite3.Connection, *, run_id: str, snapshot: dict[str, Any], imported_at_utc: str) -> None:
    rows: list[tuple[Any, ...]] = []
    for round_summary in snapshot.get("round_summaries", []):
        if not isinstance(round_summary, dict):
            continue
        round_id = maybe_text(round_summary.get("round_id"))
        fetch = round_summary.get("fetch") if isinstance(round_summary.get("fetch"), dict) else {}
        shared = round_summary.get("shared") if isinstance(round_summary.get("shared"), dict) else {}
        normalized = round_summary.get("normalized") if isinstance(round_summary.get("normalized"), dict) else {}
        decision = round_summary.get("decision") if isinstance(round_summary.get("decision"), dict) else {}
        rows.append(
            (
                run_id,
                round_id,
                maybe_int(round_summary.get("round_number")),
                bool_int(round_summary.get("is_current_round")),
                maybe_text(round_summary.get("status_label")),
                maybe_int(round_summary.get("task_count")),
                maybe_int(fetch.get("step_count")),
                maybe_int(fetch.get("completed_count")),
                maybe_int(fetch.get("failed_count")),
                maybe_int(shared.get("claim_count")),
                maybe_int(shared.get("observation_count")),
                maybe_int(shared.get("evidence_count")),
                maybe_int(normalized.get("public_signal_count")),
                maybe_int(normalized.get("environment_signal_count")),
                maybe_text(decision.get("moderator_status")),
                maybe_text(decision.get("evidence_sufficiency")),
                maybe_text(decision.get("decision_summary")),
                bool_int(decision.get("next_round_required")),
                json_text(decision.get("missing_evidence_types", [])),
                imported_at_utc,
            )
        )
    insert_many(
        conn,
        """
        INSERT INTO corpus_rounds (
            run_id, round_id, round_number, is_current_round, status_label, task_count,
            fetch_step_count, fetch_completed_count, fetch_failed_count, claim_count, observation_count,
            evidence_count, public_signal_count, environment_signal_count, moderator_status,
            evidence_sufficiency, decision_summary, next_round_required, missing_evidence_types_json,
            imported_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def insert_artifact_inventory(conn: sqlite3.Connection, *, run_dir: Path, run_id: str, round_ids: list[str], imported_at_utc: str) -> int:
    rows: list[tuple[Any, ...]] = []
    for round_id in round_ids:
        fetch_payload = SUP.load_json_if_exists(SUP.fetch_execution_path(run_dir, round_id))
        if not isinstance(fetch_payload, dict):
            continue
        statuses = fetch_payload.get("statuses")
        if not isinstance(statuses, list):
            continue
        execution_mode = maybe_text(fetch_payload.get("execution_mode"))
        plan_path = maybe_text(fetch_payload.get("plan_path"))
        plan_sha256 = maybe_text(fetch_payload.get("plan_sha256"))
        for status in statuses:
            if not isinstance(status, dict):
                continue
            step_id = maybe_text(status.get("step_id"))
            if not step_id:
                continue
            rows.append(
                (
                    run_id,
                    round_id,
                    step_id,
                    maybe_text(status.get("role")),
                    maybe_text(status.get("source_skill")),
                    maybe_text(status.get("status")),
                    maybe_text(status.get("reason")),
                    maybe_text(status.get("artifact_path")) or None,
                    maybe_text(status.get("stdout_path")) or None,
                    maybe_text(status.get("stderr_path")) or None,
                    execution_mode or None,
                    plan_path or None,
                    plan_sha256 or None,
                    imported_at_utc,
                )
            )
    insert_many(
        conn,
        """
        INSERT INTO artifact_inventory (
            run_id, round_id, step_id, role, source_skill, status, reason,
            artifact_path, stdout_path, stderr_path, execution_mode, plan_path, plan_sha256,
            imported_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def insert_public_rows(
    conn: sqlite3.Connection,
    *,
    signal_rows: list[sqlite3.Row],
    claim_rows: list[sqlite3.Row],
    imported_at_utc: str,
) -> None:
    insert_many(
        conn,
        """
        INSERT INTO public_signal_instances (
            run_id, round_id, signal_id, source_skill, signal_kind, external_id, title, text,
            url, author_name, channel_name, language, query_text, published_at_utc,
            captured_at_utc, engagement_json, metadata_json, artifact_path, record_locator,
            sha256, raw_json, imported_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["run_id"],
                row["round_id"],
                row["signal_id"],
                row["source_skill"],
                row["signal_kind"],
                row["external_id"],
                row["title"],
                row["text"],
                row["url"],
                row["author_name"],
                row["channel_name"],
                row["language"],
                row["query_text"],
                row["published_at_utc"],
                row["captured_at_utc"],
                row["engagement_json"],
                row["metadata_json"],
                row["artifact_path"],
                row["record_locator"],
                row["sha256"],
                row["raw_json"],
                imported_at_utc,
            )
            for row in signal_rows
        ],
    )
    insert_many(
        conn,
        """
        INSERT INTO public_claim_candidates (
            run_id, round_id, claim_id, claim_type, priority, summary, statement,
            source_signal_ids_json, claim_json, imported_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["run_id"],
                row["round_id"],
                row["claim_id"],
                row["claim_type"],
                row["priority"],
                row["summary"],
                row["statement"],
                row["source_signal_ids_json"],
                row["claim_json"],
                imported_at_utc,
            )
            for row in claim_rows
        ],
    )


def insert_environment_rows(
    conn: sqlite3.Connection,
    *,
    signal_rows: list[sqlite3.Row],
    observation_rows: list[sqlite3.Row],
    imported_at_utc: str,
) -> None:
    insert_many(
        conn,
        """
        INSERT INTO environment_signal_instances (
            run_id, round_id, signal_id, source_skill, signal_kind, metric, value, unit,
            observed_at_utc, window_start_utc, window_end_utc, latitude, longitude,
            bbox_json, quality_flags_json, metadata_json, artifact_path, record_locator,
            sha256, raw_json, imported_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["run_id"],
                row["round_id"],
                row["signal_id"],
                row["source_skill"],
                row["signal_kind"],
                row["metric"],
                row["value"],
                row["unit"],
                row["observed_at_utc"],
                row["window_start_utc"],
                row["window_end_utc"],
                row["latitude"],
                row["longitude"],
                row["bbox_json"],
                row["quality_flags_json"],
                row["metadata_json"],
                row["artifact_path"],
                row["record_locator"],
                row["sha256"],
                row["raw_json"],
                imported_at_utc,
            )
            for row in signal_rows
        ],
    )
    insert_many(
        conn,
        """
        INSERT INTO environment_observation_summaries (
            run_id, round_id, observation_id, metric, source_skill, observation_json, imported_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["run_id"],
                row["round_id"],
                row["observation_id"],
                row["metric"],
                row["source_skill"],
                row["observation_json"],
                imported_at_utc,
            )
            for row in observation_rows
        ],
    )


def import_run(db_path: Path, run_dir: Path, *, overwrite: bool) -> dict[str, Any]:
    if not run_dir.exists():
        raise ValueError(f"Run directory does not exist: {run_dir}")
    snapshot = collect_run_snapshot(run_dir)
    mission = snapshot["mission"]
    run_id = maybe_text(mission.get("run_id"))
    if not run_id:
        raise ValueError("mission.run_id is required")

    public_db_path, environment_db_path = resolve_analytics_db_paths(run_dir)
    public_db_exists = public_db_path.exists()
    environment_db_exists = environment_db_path.exists()
    if not public_db_exists and not environment_db_exists:
        raise ValueError(
            "No analytics databases found for this run. Expected at least one of: "
            + f"{public_db_path} or {environment_db_path}"
        )

    public_signal_rows = load_rows_for_run(public_db_path, "public_signals", run_id=run_id, order_by="round_id, signal_id")
    claim_rows = load_rows_for_run(public_db_path, "claim_candidates", run_id=run_id, order_by="round_id, claim_id")
    environment_signal_rows = load_rows_for_run(
        environment_db_path,
        "environment_signals",
        run_id=run_id,
        order_by="round_id, signal_id",
    )
    observation_rows = load_rows_for_run(
        environment_db_path,
        "observation_summaries",
        run_id=run_id,
        order_by="round_id, observation_id",
    )

    imported_at_utc = utc_now_iso()
    with connect_db(db_path) as conn:
        existing = conn.execute("SELECT 1 FROM corpus_runs WHERE run_id = ?", (run_id,)).fetchone()
        if existing is not None and not overwrite:
            raise ValueError(f"Run already exists in signal corpus: {run_id}. Use --overwrite to replace it.")
        if existing is not None:
            delete_run(conn, run_id)

        insert_run_metadata(
            conn,
            run_dir=run_dir,
            snapshot=snapshot,
            public_db_path=public_db_path,
            environment_db_path=environment_db_path,
            public_db_exists=public_db_exists,
            environment_db_exists=environment_db_exists,
            public_signal_count=len(public_signal_rows),
            environment_signal_count=len(environment_signal_rows),
            claim_candidate_count=len(claim_rows),
            observation_summary_count=len(observation_rows),
            imported_at_utc=imported_at_utc,
        )
        insert_round_summaries(conn, run_id=run_id, snapshot=snapshot, imported_at_utc=imported_at_utc)
        artifact_count = insert_artifact_inventory(
            conn,
            run_dir=run_dir,
            run_id=run_id,
            round_ids=[maybe_text(item) for item in snapshot.get("round_ids", []) if maybe_text(item)],
            imported_at_utc=imported_at_utc,
        )
        insert_public_rows(conn, signal_rows=public_signal_rows, claim_rows=claim_rows, imported_at_utc=imported_at_utc)
        insert_environment_rows(
            conn,
            signal_rows=environment_signal_rows,
            observation_rows=observation_rows,
            imported_at_utc=imported_at_utc,
        )
        conn.commit()

    return {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "round_count": len(snapshot.get("round_ids", [])),
        "current_round_id": maybe_text(snapshot.get("state", {}).get("current_round_id")),
        "current_stage": maybe_text(snapshot.get("state", {}).get("stage")),
        "public_db_path": str(public_db_path),
        "public_db_exists": public_db_exists,
        "public_signal_count": len(public_signal_rows),
        "claim_candidate_count": len(claim_rows),
        "environment_db_path": str(environment_db_path),
        "environment_db_exists": environment_db_exists,
        "environment_signal_count": len(environment_signal_rows),
        "observation_summary_count": len(observation_rows),
        "artifact_count": artifact_count,
    }


def import_runs_root(db_path: Path, runs_root: Path, *, overwrite: bool) -> dict[str, Any]:
    if not runs_root.exists():
        raise ValueError(f"Runs root does not exist: {runs_root}")
    results: list[dict[str, Any]] = []
    for child in sorted(runs_root.iterdir()):
        if not child.is_dir():
            continue
        if not (child / "mission.json").exists():
            continue
        try:
            results.append(import_run(db_path, child, overwrite=overwrite))
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    "run_dir": str(child),
                    "ok": False,
                    "error": str(exc),
                }
            )
    imported_count = sum(1 for item in results if item.get("run_id"))
    failed_count = sum(1 for item in results if not item.get("run_id"))
    return {
        "runs_root": str(runs_root),
        "imported_count": imported_count,
        "failed_count": failed_count,
        "results": results,
    }


def load_run_bundle(conn: sqlite3.Connection, run_id: str) -> dict[str, Any]:
    run_row = conn.execute("SELECT * FROM corpus_runs WHERE run_id = ?", (run_id,)).fetchone()
    if run_row is None:
        raise ValueError(f"Run not found: {run_id}")
    rounds = conn.execute(
        """
        SELECT round_id, round_number, is_current_round, status_label, task_count,
               fetch_step_count, fetch_completed_count, fetch_failed_count,
               claim_count, observation_count, evidence_count,
               public_signal_count, environment_signal_count,
               moderator_status, evidence_sufficiency, decision_summary,
               next_round_required, missing_evidence_types_json
        FROM corpus_rounds
        WHERE run_id = ?
        ORDER BY round_number ASC
        """,
        (run_id,),
    ).fetchall()
    public_sources = conn.execute(
        """
        SELECT source_skill, signal_kind, COUNT(*) AS signal_count,
               MIN(published_at_utc) AS first_published_at_utc,
               MAX(published_at_utc) AS last_published_at_utc
        FROM public_signal_instances
        WHERE run_id = ?
        GROUP BY source_skill, signal_kind
        ORDER BY signal_count DESC, source_skill ASC, signal_kind ASC
        """,
        (run_id,),
    ).fetchall()
    claim_types = conn.execute(
        """
        SELECT claim_type, COUNT(*) AS claim_candidate_count
        FROM public_claim_candidates
        WHERE run_id = ?
        GROUP BY claim_type
        ORDER BY claim_candidate_count DESC, claim_type ASC
        """,
        (run_id,),
    ).fetchall()
    environment_metrics = conn.execute(
        """
        SELECT source_skill, metric, COUNT(*) AS signal_count,
               MIN(value) AS min_value, MAX(value) AS max_value
        FROM environment_signal_instances
        WHERE run_id = ?
        GROUP BY source_skill, metric
        ORDER BY signal_count DESC, source_skill ASC, metric ASC
        """,
        (run_id,),
    ).fetchall()
    artifact_breakdown = conn.execute(
        """
        SELECT round_id, role, source_skill, status, COUNT(*) AS step_count
        FROM artifact_inventory
        WHERE run_id = ?
        GROUP BY round_id, role, source_skill, status
        ORDER BY round_id ASC, role ASC, source_skill ASC, status ASC
        """,
        (run_id,),
    ).fetchall()
    return {
        "run": dict(run_row),
        "rounds": [dict(row) for row in rounds],
        "public_sources": [dict(row) for row in public_sources],
        "public_claim_types": [dict(row) for row in claim_types],
        "environment_metrics": [dict(row) for row in environment_metrics],
        "artifact_breakdown": [dict(row) for row in artifact_breakdown],
    }


def command_init_db(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    init_db(db_path)
    return {"db": str(db_path), "ddl_path": str(DDL_PATH)}


def command_import_run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    run_dir = Path(args.run_dir).expanduser().resolve()
    init_db(db_path)
    return import_run(db_path, run_dir, overwrite=args.overwrite)


def command_import_runs_root(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    runs_root = Path(args.runs_root).expanduser().resolve()
    init_db(db_path)
    return import_runs_root(db_path, runs_root, overwrite=args.overwrite)


def command_list_runs(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    with connect_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT run_id, topic, objective, region_label, window_start_utc, window_end_utc,
                   round_count, current_round_id, current_stage,
                   public_signal_count, environment_signal_count,
                   claim_candidate_count, observation_summary_count,
                   public_db_exists, environment_db_exists, imported_at_utc
            FROM corpus_runs
            ORDER BY imported_at_utc DESC, run_id DESC
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()
    return {
        "db": str(db_path),
        "count": len(rows),
        "runs": [dict(row) for row in rows],
    }


def command_show_run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    run_id = maybe_text(args.run_id)
    with connect_db(db_path) as conn:
        result = load_run_bundle(conn, run_id)
    result["db"] = str(db_path)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage a cross-run SQLite signal corpus for eco-council normalized artifacts.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_db_cmd = sub.add_parser("init-db", help="Initialize the local eco-council signal-corpus SQLite database.")
    init_db_cmd.add_argument("--db", required=True, help="SQLite database path.")
    init_db_cmd.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_run_cmd = sub.add_parser("import-run", help="Import one run directory into the local signal corpus.")
    import_run_cmd.add_argument("--db", required=True, help="SQLite database path.")
    import_run_cmd.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_run_cmd.add_argument("--overwrite", action="store_true", help="Replace an existing run with the same run_id.")
    import_run_cmd.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_root_cmd = sub.add_parser("import-runs-root", help="Import every run directory under one runs root into the signal corpus.")
    import_root_cmd.add_argument("--db", required=True, help="SQLite database path.")
    import_root_cmd.add_argument("--runs-root", required=True, help="Runs root directory.")
    import_root_cmd.add_argument("--overwrite", action="store_true", help="Replace existing runs when run_id matches.")
    import_root_cmd.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    list_runs_cmd = sub.add_parser("list-runs", help="List imported runs in the local signal corpus.")
    list_runs_cmd.add_argument("--db", required=True, help="SQLite database path.")
    list_runs_cmd.add_argument("--limit", type=int, default=50, help="Maximum runs to return.")
    list_runs_cmd.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    show_run_cmd = sub.add_parser("show-run", help="Show one imported run with round and source breakdowns.")
    show_run_cmd.add_argument("--db", required=True, help="SQLite database path.")
    show_run_cmd.add_argument("--run-id", required=True, help="Run id, usually mission.run_id.")
    show_run_cmd.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "init-db": command_init_db,
        "import-run": command_import_run,
        "import-runs-root": command_import_runs_root,
        "list-runs": command_list_runs,
        "show-run": command_show_run,
    }
    try:
        payload = handlers[args.command](args)
    except Exception as exc:  # noqa: BLE001
        print(pretty_json({"command": args.command, "ok": False, "error": str(exc)}, pretty=getattr(args, "pretty", False)))
        return 1
    print(pretty_json({"command": args.command, "ok": True, "payload": payload}, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
