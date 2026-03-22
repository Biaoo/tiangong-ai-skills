---
name: eco-council-supervisor
description: Run an eco-council workflow through one stage-gated local supervisor and maintain a local SQLite case library of historical eco-council records. Use when you want to bootstrap a run from mission JSON, provision fixed OpenClaw moderator/sociologist/environmentalist agents, require audited expert source-selection before any fetch stage, import agent JSON replies safely, advance rounds with minimal manual freedom, render a human-readable meeting record from the run directory, or archive completed runs into a queryable local history database.
---

# Eco Council Supervisor

Use this skill when the eco-council flow should be driven by one deterministic local controller instead of ad hoc shell usage.

## Core Workflow

1. Initialize one run with `init-run`.
   - Optionally attach one local case-library SQLite database so the moderator receives compact similar-case context automatically.
2. Optionally create three isolated OpenClaw agents with `provision-openclaw-agents`.
3. Let the moderator review or revise `tasks.json`.
4. Let the sociologist and environmentalist each return one audited `source-selection` object.
5. Follow `RUN_DIR/supervisor/CURRENT_STEP.txt`.
6. At each shell stage, use `continue-run` and approve the step.
7. At each agent stage, import returned JSON with:
  - `import-task-review`
  - `import-source-selection`
  - `import-report`
  - `import-decision`

## Command Surface

- `python3 scripts/eco_council_supervisor.py init-run --run-dir ... --mission-input ... [--history-db ... --history-top-k 3] --pretty`
  - Calls `$eco-council-orchestrate bootstrap-run`.
  - Creates supervisor state plus role/session prompt files.
  - When `--history-db` is set, moderator task-review and decision turns also receive a compact similar-case summary from the local history library.
- `python3 scripts/eco_council_supervisor.py provision-openclaw-agents --run-dir ... --pretty`
  - Creates or reuses fixed OpenClaw agent ids for moderator, sociologist, and environmentalist.
- `python3 scripts/eco_council_supervisor.py status --run-dir ... [--history-db ... --history-top-k 3] [--disable-history-context] --pretty`
  - Shows current round, stage, outbox prompts, `CURRENT_STEP.txt`, and the current historical-context attachment state.
- `python3 scripts/eco_council_supervisor.py summarize-run --run-dir ... --lang zh --pretty`
  - Renders one human-readable Markdown meeting record under `RUN_DIR/reports/`.
  - Supports `--lang zh|en` for report language only; workflow payloads remain in English.
- `python3 scripts/eco_council_case_library.py init-db --db ... --pretty`
  - Initializes a local SQLite history database for eco-council records.
- `python3 scripts/eco_council_case_library.py import-run --db ... --run-dir ... --overwrite --pretty`
  - Imports one completed or in-progress run directory into the case library.
- `python3 scripts/eco_council_case_library.py import-runs-root --db ... --runs-root ... --overwrite --pretty`
  - Bulk-imports every run under one `runs/` root.
- `python3 scripts/eco_council_case_library.py list-cases --db ... --pretty`
  - Lists imported historical cases with final status and latest counts.
- `python3 scripts/eco_council_case_library.py search-cases --db ... --query ... [--region-label ...] --pretty`
  - Finds similar historical cases for human lookup or moderator context injection.
- `python3 scripts/eco_council_case_library.py show-case --db ... --case-id ... --pretty`
  - Shows one case plus per-round summaries; optional flags can include report, claim, and evidence summaries.
- `python3 scripts/eco_council_case_library.py export-case-markdown --db ... --case-id ... [--lang zh|en] --pretty`
  - Exports one human-readable Markdown case record for audit or replay review.
- `python3 scripts/eco_council_supervisor.py continue-run --run-dir ... --pretty`
  - Runs exactly one approved shell stage.
- `python3 scripts/eco_council_supervisor.py run-agent-step --run-dir ... --pretty`
  - Sends the current moderator/expert turn to OpenClaw, captures JSON, validates it, and imports it automatically.
  - Supports moderator task review, expert source selection, expert report drafting, and moderator decision drafting.
- `python3 scripts/eco_council_supervisor.py import-task-review ...`
- `python3 scripts/eco_council_supervisor.py import-source-selection ...`
- `python3 scripts/eco_council_supervisor.py import-report ...`
- `python3 scripts/eco_council_supervisor.py import-decision ...`

## Guardrails

- Keep shell execution inside the supervisor.
- Keep agents limited to JSON-only outputs.
- No source runs unless an expert selected it in `source_selection.json` or a task explicitly forces it through `task.inputs.required_sources`.
- Treat `RUN_DIR/supervisor/CURRENT_STEP.txt` as the human checklist.
- If OpenClaw cannot load local repo skills directly, still use the generated prompt files as the source of truth.
- Treat the case-library SQLite database as historical record storage, not as a replacement for canonical per-run JSON artifacts.
- Treat moderator historical context as planning-only retrieval help; it must not override current-run evidence.

## References

- `references/workflow.md`
