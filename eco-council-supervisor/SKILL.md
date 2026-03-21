---
name: eco-council-supervisor
description: Run an eco-council workflow through one stage-gated local supervisor. Use when you want to bootstrap a run from mission JSON, provision fixed OpenClaw moderator/sociologist/environmentalist agents, require human approval before each shell stage, import agent JSON replies safely, and advance rounds with minimal manual freedom.
---

# Eco Council Supervisor

Use this skill when the eco-council flow should be driven by one deterministic local controller instead of ad hoc shell usage.

## Core Workflow

1. Initialize one run with `init-run`.
2. Optionally create three isolated OpenClaw agents with `provision-openclaw-agents`.
3. Follow `RUN_DIR/supervisor/CURRENT_STEP.txt`.
4. At each shell stage, use `continue-run` and approve the step.
5. At each agent stage, import returned JSON with:
   - `import-task-review`
   - `import-report`
   - `import-decision`

## Command Surface

- `python3 scripts/eco_council_supervisor.py init-run --run-dir ... --mission-input ... --pretty`
  - Calls `$eco-council-orchestrate bootstrap-run`.
  - Creates supervisor state plus role/session prompt files.
- `python3 scripts/eco_council_supervisor.py provision-openclaw-agents --run-dir ... --pretty`
  - Creates or reuses fixed OpenClaw agent ids for moderator, sociologist, and environmentalist.
- `python3 scripts/eco_council_supervisor.py status --run-dir ... --pretty`
  - Shows current round, stage, outbox prompts, and `CURRENT_STEP.txt`.
- `python3 scripts/eco_council_supervisor.py continue-run --run-dir ... --pretty`
  - Runs exactly one approved shell stage.
- `python3 scripts/eco_council_supervisor.py run-agent-step --run-dir ... --pretty`
  - Sends the current moderator/expert turn to OpenClaw, captures JSON, validates it, and imports it automatically.
- `python3 scripts/eco_council_supervisor.py import-task-review ...`
- `python3 scripts/eco_council_supervisor.py import-report ...`
- `python3 scripts/eco_council_supervisor.py import-decision ...`

## Guardrails

- Keep shell execution inside the supervisor.
- Keep agents limited to JSON-only outputs.
- Treat `RUN_DIR/supervisor/CURRENT_STEP.txt` as the human checklist.
- If OpenClaw cannot load local repo skills directly, still use the generated prompt files as the source of truth.

## References

- `references/workflow.md`
