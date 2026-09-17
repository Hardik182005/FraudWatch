-- FraudWatch schema v1. Applied by fwsvc.db.migrate() under an advisory lock.
-- All *_at columns are epoch milliseconds (UTC) so Java, Python, SQL and the browsers agree.

CREATE TABLE demo_sessions (
    id              text PRIMARY KEY,
    label           text,
    kind            text    NOT NULL DEFAULT 'visitor',      -- visitor | operator | experiment
    created_at      bigint  NOT NULL,
    expires_at      bigint  NOT NULL,
    event_cap       integer NOT NULL,
    events_used     integer NOT NULL DEFAULT 0,
    rate_cap_eps    real    NOT NULL,
    active_run_id   text,
    replaced_by     text,
    stream_updates  boolean NOT NULL DEFAULT true,           -- false for benchmark sessions (no SSE fan-out writes)
    client_hash     text                                      -- salted hash of client IP, for abuse caps only
);

CREATE TABLE runs (
    id              text PRIMARY KEY,
    session_id      text    NOT NULL REFERENCES demo_sessions(id),
    scenario        text    NOT NULL,
    seed            bigint  NOT NULL,
    rate_eps        real    NOT NULL,
    engines         text[]  NOT NULL,
    status          text    NOT NULL,
    max_events      integer NOT NULL,
    events_sent     integer NOT NULL DEFAULT 0,
    created_at      bigint  NOT NULL,
    started_at      bigint,
    ended_at        bigint,
    replay_of       text,
    control_log     jsonb   NOT NULL DEFAULT '[]',
    created_by      text    NOT NULL,
    experiment_id   text,
    error           text,
    updated_at      bigint  NOT NULL
);
CREATE INDEX runs_session_idx ON runs (session_id, created_at DESC);

CREATE TABLE scored_events (
    engine               text    NOT NULL,
    event_id             text    NOT NULL,
    session_id           text    NOT NULL,
    run_id               text    NOT NULL,
    seq                  integer NOT NULL,
    account_id           text    NOT NULL,
    event_time           bigint  NOT NULL,
    amount_inr           double precision NOT NULL,
    merchant_name        text,
    merchant_category    text,
    city                 text,
    pattern              text,
    injected             boolean NOT NULL DEFAULT false,
    action               text    NOT NULL,
    score                double precision,
    late                 boolean NOT NULL,
    watermark_ms         bigint,
    rules_fired          text[]  NOT NULL DEFAULT '{}',
    scoring_status       text    NOT NULL,
    engine_job_id        text,
    engine_run_label     text,
    event                jsonb   NOT NULL,
    detail               jsonb   NOT NULL,   -- raw_features, model_inputs, model, rules, policy, scoring{status,attempts,error}, kafka, spark
    produced_at          bigint  NOT NULL,
    kafka_ts             bigint,
    engine_received_at   bigint,
    scoring_started_at   bigint,
    scoring_completed_at bigint,
    emitted_at           bigint,
    persisted_at         bigint  NOT NULL,
    e2e_ms               integer,
    duplicates           integer NOT NULL DEFAULT 0,
    last_duplicate_at    bigint,
    PRIMARY KEY (engine, event_id)
);
CREATE INDEX scored_session_idx ON scored_events (session_id, engine, persisted_at DESC);
CREATE INDEX scored_run_idx     ON scored_events (run_id, engine);
CREATE INDEX scored_action_idx  ON scored_events (session_id, engine, action) WHERE action IN ('alert', 'review');

CREATE TABLE alerts (
    id              bigserial PRIMARY KEY,
    event_id        text    NOT NULL,
    engine          text    NOT NULL,
    session_id      text    NOT NULL,
    run_id          text    NOT NULL,
    account_id      text    NOT NULL,
    amount_inr      double precision NOT NULL,
    merchant_name   text,
    action          text    NOT NULL,
    score           double precision,
    rules_fired     text[]  NOT NULL DEFAULT '{}',
    created_at      bigint  NOT NULL,
    status          text    NOT NULL DEFAULT 'open',
    decision_note   text,
    updated_at      bigint  NOT NULL,
    UNIQUE (engine, event_id)
);
CREATE INDEX alerts_session_idx ON alerts (session_id, engine, created_at DESC);

-- Hash-chained, append-only audit log. hash = sha256(prev_hash || canonical JSON of the entry).
CREATE TABLE audit_log (
    id           bigserial PRIMARY KEY,
    at           bigint NOT NULL,
    actor        text   NOT NULL,
    action       text   NOT NULL,
    entity_type  text   NOT NULL,
    entity_id    text   NOT NULL,
    session_id   text,
    from_status  text,
    to_status    text,
    note         text,
    prev_hash    text   NOT NULL,
    hash         text   NOT NULL UNIQUE
);
CREATE INDEX audit_entity_idx ON audit_log (entity_type, entity_id, id);

CREATE TABLE window_aggregates (
    engine          text   NOT NULL,
    session_id      text   NOT NULL,
    window_start    bigint NOT NULL,
    window_end      bigint NOT NULL,
    txn_count       integer NOT NULL,
    amount_sum_inr  double precision NOT NULL,
    alert_count     integer NOT NULL,
    review_count    integer NOT NULL,
    unscored_count  integer NOT NULL,
    emitted_at      bigint,
    persisted_at    bigint NOT NULL,
    duplicates      integer NOT NULL DEFAULT 0,
    PRIMARY KEY (engine, session_id, window_start)
);

CREATE TABLE dlq_events (
    id           bigserial PRIMARY KEY,
    engine       text,
    error        text   NOT NULL,
    raw          text,
    kafka        jsonb,
    received_at  bigint,
    persisted_at bigint NOT NULL
);

CREATE TABLE spark_progress (
    id          bigserial PRIMARY KEY,
    query_name  text,
    run_label   text,
    batch_id    bigint,
    progress    jsonb  NOT NULL,
    at          bigint NOT NULL
);
CREATE INDEX spark_progress_label_idx ON spark_progress (run_label, id DESC);

-- Fan-out feed for SSE. Cursor is monotonic; rows are trimmed after a retention period.
CREATE TABLE stream_log (
    cursor      bigserial PRIMARY KEY,
    session_id  text   NOT NULL,
    type        text   NOT NULL,
    payload     jsonb  NOT NULL,
    at          bigint NOT NULL
);
CREATE INDEX stream_log_session_idx ON stream_log (session_id, cursor);

-- Liveness reported by internal services (generator, result_consumer, ops_agent).
CREATE TABLE component_heartbeats (
    id          text PRIMARY KEY,
    status      text   NOT NULL,
    detail      text,
    metrics     jsonb  NOT NULL DEFAULT '{}',
    checked_at  bigint NOT NULL
);

CREATE TABLE experiments (
    id           text PRIMARY KEY,
    kind         text   NOT NULL,
    status       text   NOT NULL,
    params       jsonb  NOT NULL,
    created_at   bigint NOT NULL,
    started_at   bigint,
    finished_at  bigint,
    created_by   text   NOT NULL,
    run_ids      text[] NOT NULL DEFAULT '{}',
    results      jsonb,
    error        text,
    environment  jsonb  NOT NULL DEFAULT '{}',
    progress     jsonb  NOT NULL DEFAULT '{}'
);
CREATE INDEX experiments_created_idx ON experiments (created_at DESC);
