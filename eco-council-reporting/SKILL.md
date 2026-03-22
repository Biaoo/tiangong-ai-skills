---
name: eco-council-reporting
description: Build role-specific expert-report packets, deterministic report drafts, moderator decision packets, and next-round task drafts from normalized eco-council round artifacts. Use when eco-council-normalize has already produced claims, observations, evidence cards, and round context JSON, and OpenClaw needs moderator-ready reporting inputs or valid contract-shaped draft outputs before final deliberation.
---

# Eco Council Reporting

## Core Goal

- Keep normalization and deliberation separate.
- Turn canonical round artifacts into:
  - `round_001/sociologist/derived/report_packet.json`
  - `round_001/sociologist/derived/sociologist_report_draft.json`
  - `round_001/environmentalist/derived/report_packet.json`
  - `round_001/environmentalist/derived/environmentalist_report_draft.json`
  - `round_001/moderator/derived/decision_packet.json`
  - `round_001/moderator/derived/council_decision_draft.json`
- Produce valid draft `expert-report` and `council-decision` objects that OpenClaw can revise or promote.

## Required Upstream State

- Start from one scaffolded eco-council run directory created by `$eco-council-data-contract`.
- Run `$eco-council-normalize` first so `claims.json`, `observations.json`, `evidence_cards.json`, and preferably `context_*.json` already exist.
- Keep the final canonical files such as `sociologist_report.json`, `environmentalist_report.json`, and `council_decision.json` unchanged until moderator or expert review is complete.

## Workflow

1. Build expert report packets and deterministic draft reports.

```bash
python3 scripts/eco_council_reporting.py build-report-packets \
  --run-dir ./runs/20260321-chiangmai-smoke \
  --round-id round-001 \
  --pretty
```

2. Build the moderator decision packet and next-round task drafts.
- Sociologist and environmentalist next-round tasks now include `source-selection` in `expected_output_kinds` so audited source choice remains explicit before any future fetch stage.

```bash
python3 scripts/eco_council_reporting.py build-decision-packet \
  --run-dir ./runs/20260321-chiangmai-smoke \
  --round-id round-001 \
  --next-round-id round-002 \
  --prefer-draft-reports \
  --pretty
```

3. Or run both steps in one call.

```bash
python3 scripts/eco_council_reporting.py build-all \
  --run-dir ./runs/20260321-chiangmai-smoke \
  --round-id round-001 \
  --prefer-draft-reports \
  --pretty
```

4. Render OpenClaw text prompts from the generated packets.

```bash
python3 scripts/eco_council_reporting.py render-openclaw-prompts \
  --run-dir ./runs/20260321-chiangmai-smoke \
  --round-id round-001
```

5. After expert or moderator review, promote the approved drafts into canonical contract paths.

```bash
python3 scripts/eco_council_reporting.py promote-all \
  --run-dir ./runs/20260321-chiangmai-smoke \
  --round-id round-001 \
  --pretty
```

## What This Skill Writes

- Derived packets only. This skill does not overwrite canonical `*_report.json` or `council_decision.json`.
- Each packet embeds:
  - relevant round context
  - contract-facing writing rules
  - validation hints
  - one valid draft object for direct editing or promotion
- Promotion commands validate the selected draft and then write canonical outputs only when safe to do so.

## Scope Decisions

- Do not fetch remote data here.
- Do not mutate raw, normalized, or shared canonical inputs.
- Do not silently invent IDs, coordinates, or time windows.
- Use deterministic heuristics for:
  - report finding seeds
  - gap-to-question conversion
  - missing evidence typing
  - next-round task seeding

## References

- `references/packet-format.md`
- `references/decision-heuristics.md`
- `references/openclaw-chaining-templates.md`

## Script

- `scripts/eco_council_reporting.py`

## OpenClaw Compatibility

- Call this skill after `$eco-council-normalize`.
- Feed `report_packet.json` or `decision_packet.json` to OpenClaw agents instead of raw DB rows.
- Use `render-openclaw-prompts` when you want ready-to-paste role prompts that point at the packet files.
- Validate any promoted draft with `$eco-council-data-contract`.
