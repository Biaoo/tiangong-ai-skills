# Supervisor Workflow

The supervisor keeps a strict split:

1. Local shell stages
   - `prepare-round`
   - `execute-fetch-plan`
   - `run-data-plane`
   - `promote-all`
   - `advance-round`
2. Agent stages
   - moderator task review
   - sociologist report draft
   - environmentalist report draft
   - moderator decision draft

## Files the user should care about

- `RUN_DIR/supervisor/CURRENT_STEP.txt`
- `RUN_DIR/supervisor/state.json`
- `RUN_DIR/supervisor/sessions/*.txt`
- `RUN_DIR/supervisor/outbox/*.txt`
- `RUN_DIR/supervisor/responses/*`

## Stage Map

- `awaiting-moderator-task-review`
  - Prefer `run-agent-step`.
  - Manual fallback: send the moderator session prompt and task-review outbox prompt, then use `import-task-review`.
- `ready-to-prepare-round`
  - Run `continue-run`.
- `ready-to-execute-fetch-plan`
  - Run `continue-run`.
- `ready-to-run-data-plane`
  - Run `continue-run`.
- `awaiting-expert-reports`
  - Prefer `run-agent-step --role sociologist` and `run-agent-step --role environmentalist`.
  - Manual fallback: send the two expert outbox prompts, then use `import-report`.
- `awaiting-moderator-decision`
  - Prefer `run-agent-step`.
  - Manual fallback: send the moderator decision outbox prompt, then use `import-decision`.
- `ready-to-promote`
  - Run `continue-run`.
- `ready-to-advance-round`
  - Run `continue-run`.
- `completed`
  - Stop.

## OpenClaw Note

`provision-openclaw-agents` creates isolated OpenClaw agents, but it does not force a chat channel. That keeps Feishu optional. You can talk to the agents through whatever OpenClaw surface you prefer.
