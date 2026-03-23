PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS corpus_runs (
    run_id TEXT PRIMARY KEY,
    run_dir TEXT NOT NULL,
    topic TEXT NOT NULL,
    objective TEXT NOT NULL,
    region_label TEXT NOT NULL,
    region_geometry_json TEXT NOT NULL,
    window_start_utc TEXT NOT NULL,
    window_end_utc TEXT NOT NULL,
    current_round_id TEXT NOT NULL,
    current_stage TEXT NOT NULL,
    round_count INTEGER NOT NULL,
    public_db_path TEXT NOT NULL,
    public_db_exists INTEGER NOT NULL,
    environment_db_path TEXT NOT NULL,
    environment_db_exists INTEGER NOT NULL,
    public_signal_count INTEGER NOT NULL,
    environment_signal_count INTEGER NOT NULL,
    claim_candidate_count INTEGER NOT NULL,
    observation_summary_count INTEGER NOT NULL,
    imported_at_utc TEXT NOT NULL,
    mission_json TEXT NOT NULL CHECK (json_valid(mission_json))
);

CREATE INDEX IF NOT EXISTS idx_corpus_runs_imported_at
    ON corpus_runs(imported_at_utc);

CREATE INDEX IF NOT EXISTS idx_corpus_runs_topic
    ON corpus_runs(topic);

CREATE INDEX IF NOT EXISTS idx_corpus_runs_region_label
    ON corpus_runs(region_label);

CREATE TABLE IF NOT EXISTS corpus_rounds (
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    round_number INTEGER NOT NULL,
    is_current_round INTEGER NOT NULL,
    status_label TEXT NOT NULL,
    task_count INTEGER NOT NULL,
    fetch_step_count INTEGER NOT NULL,
    fetch_completed_count INTEGER NOT NULL,
    fetch_failed_count INTEGER NOT NULL,
    claim_count INTEGER NOT NULL,
    observation_count INTEGER NOT NULL,
    evidence_count INTEGER NOT NULL,
    public_signal_count INTEGER NOT NULL,
    environment_signal_count INTEGER NOT NULL,
    moderator_status TEXT,
    evidence_sufficiency TEXT,
    decision_summary TEXT,
    next_round_required INTEGER NOT NULL,
    missing_evidence_types_json TEXT NOT NULL CHECK (json_valid(missing_evidence_types_json)),
    imported_at_utc TEXT NOT NULL,
    PRIMARY KEY (run_id, round_id),
    FOREIGN KEY (run_id) REFERENCES corpus_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_corpus_rounds_run_round
    ON corpus_rounds(run_id, round_number);

CREATE TABLE IF NOT EXISTS artifact_inventory (
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    role TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    artifact_path TEXT,
    stdout_path TEXT,
    stderr_path TEXT,
    execution_mode TEXT,
    plan_path TEXT,
    plan_sha256 TEXT,
    imported_at_utc TEXT NOT NULL,
    PRIMARY KEY (run_id, round_id, step_id),
    FOREIGN KEY (run_id) REFERENCES corpus_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_artifact_inventory_source
    ON artifact_inventory(source_skill, status);

CREATE INDEX IF NOT EXISTS idx_artifact_inventory_run_round
    ON artifact_inventory(run_id, round_id);

CREATE TABLE IF NOT EXISTS public_signal_instances (
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    external_id TEXT,
    title TEXT,
    text TEXT,
    url TEXT,
    author_name TEXT,
    channel_name TEXT,
    language TEXT,
    query_text TEXT,
    published_at_utc TEXT,
    captured_at_utc TEXT NOT NULL,
    engagement_json TEXT NOT NULL CHECK (json_valid(engagement_json)),
    metadata_json TEXT NOT NULL CHECK (json_valid(metadata_json)),
    artifact_path TEXT NOT NULL,
    record_locator TEXT NOT NULL,
    sha256 TEXT,
    raw_json TEXT NOT NULL CHECK (json_valid(raw_json)),
    imported_at_utc TEXT NOT NULL,
    PRIMARY KEY (run_id, round_id, signal_id),
    FOREIGN KEY (run_id) REFERENCES corpus_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_public_signal_instances_source
    ON public_signal_instances(source_skill, signal_kind);

CREATE INDEX IF NOT EXISTS idx_public_signal_instances_sha256
    ON public_signal_instances(sha256);

CREATE INDEX IF NOT EXISTS idx_public_signal_instances_run_round
    ON public_signal_instances(run_id, round_id);

CREATE TABLE IF NOT EXISTS public_claim_candidates (
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    claim_id TEXT NOT NULL,
    claim_type TEXT NOT NULL,
    priority INTEGER NOT NULL,
    summary TEXT NOT NULL,
    statement TEXT NOT NULL,
    source_signal_ids_json TEXT NOT NULL CHECK (json_valid(source_signal_ids_json)),
    claim_json TEXT NOT NULL CHECK (json_valid(claim_json)),
    imported_at_utc TEXT NOT NULL,
    PRIMARY KEY (run_id, round_id, claim_id),
    FOREIGN KEY (run_id) REFERENCES corpus_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_public_claim_candidates_type
    ON public_claim_candidates(claim_type);

CREATE INDEX IF NOT EXISTS idx_public_claim_candidates_run_round
    ON public_claim_candidates(run_id, round_id);

CREATE TABLE IF NOT EXISTS environment_signal_instances (
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    metric TEXT NOT NULL,
    value REAL,
    unit TEXT NOT NULL,
    observed_at_utc TEXT,
    window_start_utc TEXT,
    window_end_utc TEXT,
    latitude REAL,
    longitude REAL,
    bbox_json TEXT CHECK (bbox_json IS NULL OR json_valid(bbox_json)),
    quality_flags_json TEXT NOT NULL CHECK (json_valid(quality_flags_json)),
    metadata_json TEXT NOT NULL CHECK (json_valid(metadata_json)),
    artifact_path TEXT NOT NULL,
    record_locator TEXT NOT NULL,
    sha256 TEXT,
    raw_json TEXT NOT NULL CHECK (json_valid(raw_json)),
    imported_at_utc TEXT NOT NULL,
    PRIMARY KEY (run_id, round_id, signal_id),
    FOREIGN KEY (run_id) REFERENCES corpus_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_environment_signal_instances_source
    ON environment_signal_instances(source_skill, metric);

CREATE INDEX IF NOT EXISTS idx_environment_signal_instances_sha256
    ON environment_signal_instances(sha256);

CREATE INDEX IF NOT EXISTS idx_environment_signal_instances_run_round
    ON environment_signal_instances(run_id, round_id);

CREATE TABLE IF NOT EXISTS environment_observation_summaries (
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    observation_json TEXT NOT NULL CHECK (json_valid(observation_json)),
    imported_at_utc TEXT NOT NULL,
    PRIMARY KEY (run_id, round_id, observation_id),
    FOREIGN KEY (run_id) REFERENCES corpus_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_environment_observation_metric
    ON environment_observation_summaries(metric);

CREATE INDEX IF NOT EXISTS idx_environment_observation_run_round
    ON environment_observation_summaries(run_id, round_id);
