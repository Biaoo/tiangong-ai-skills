# Contract Notes

## Design Intent

Use one shared contract across all eco-council roles:

- moderator
- sociologist
- environmentalist
- historian

Keep role boundaries strict:

- moderator issues `mission`, `round-task`, and `council-decision`
- sociologist and environmentalist each produce one audited `source-selection` before fetch execution
- sociologist produces `claim` and `expert-report`
- environmentalist produces `observation`, `evidence-card`, and `expert-report`
- historian later produces `expert-report` plus case-support attachments outside this first contract version

## Naming and Versioning

- Keep `schema_version` on every top-level object.
- Keep `run_id` stable for the whole council session.
- Keep `round_id` stable within one run, for example `round-001`.
- Keep object IDs deterministic when possible:
  - `claim_id`: `claim-001`
  - `observation_id`: `obs-001`
  - `evidence_id`: `evidence-001`
  - `report_id`: `report-sociologist-round-001`

## Time Rules

- Use UTC only.
- Use RFC 3339 timestamps with trailing `Z`.
- Keep all windows inclusive at storage time unless the producer explicitly documents a different boundary model.
- Store the original source timestamp separately in raw artifacts, not in canonical objects unless needed for provenance.

## Geometry Rules

Use only these geometry forms in canonical objects:

- `Point`
- `BBox`

Do not let expert agents invent coordinates in free text. If upstream tasks only provide place names, resolve them in a deterministic geocoding pre-step before physical validation starts.

## Provenance Rules

Every `claim`, `observation`, and `evidence-card` should remain traceable back to raw artifacts.

Canonical provenance fields:

- `source_skill`
- `artifact_path`
- `record_locator`
- `external_id`
- optional `sha256`

Do not overwrite or mutate raw files after capture.

## Moderator Round Rules

One moderator round should produce:

1. one or more `round-task` objects
2. one `source-selection` per expert role that may fetch data
3. zero or more `claim` objects
4. zero or more `observation` objects
5. zero or more `evidence-card` objects
6. one `expert-report` per participating role
7. one `council-decision`

If evidence is insufficient, the moderator should create new `round-task` objects for the next round instead of editing earlier tasks in place.

Recommended directory rule:

- keep one folder per round using `round_001`, `round_002`, `round_003`, ...
- keep the contract `round_id` field as `round-001`, `round-002`, `round-003`, ...

## Historical Extension

This first contract version reserves historian participation through `agent_role=historian` inside `expert-report`.

When case retrieval is added later, prefer adding a separate `case-support` object rather than overloading `evidence-card`. Keep current-event evidence and historical analog evidence separate.
