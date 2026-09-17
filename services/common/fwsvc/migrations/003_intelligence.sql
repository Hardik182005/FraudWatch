-- System Intelligence: structured logs, client receipts; scenario presets, delivery options and run counters.

ALTER TABLE runs ADD COLUMN preset text;
ALTER TABLE runs ADD COLUMN options jsonb NOT NULL DEFAULT '{}';
ALTER TABLE runs ADD COLUMN events_acked integer;            -- broker delivery confirmations (all attempts); NULL = not recorded
ALTER TABLE runs ADD COLUMN delivery_errors integer;         -- NULL = not recorded (runs created before this migration)
ALTER TABLE runs ADD COLUMN duplicates_sent integer NOT NULL DEFAULT 0;
CREATE INDEX runs_experiment_idx ON runs (experiment_id) WHERE experiment_id IS NOT NULL;
CREATE INDEX runs_created_idx ON runs (created_at DESC);

CREATE TABLE system_logs (
    id             bigserial PRIMARY KEY,
    timestamp_utc  bigint  NOT NULL,     -- when the logged thing happened (as known by the service)
    observed_at    bigint  NOT NULL,     -- when the row was written
    severity       text    NOT NULL,     -- debug | info | warning | error | critical
    service        text    NOT NULL,
    environment    text,
    session_id     text,
    run_id         text,
    event_id       text,
    trace_id       text,
    engine         text,
    job_id         text,
    stage          text,
    status         text,
    duration_ms    double precision,
    message        text    NOT NULL,
    error_code     text,
    retry_attempt  integer,
    payload        jsonb
);
CREATE INDEX system_logs_ts_idx      ON system_logs (timestamp_utc DESC, id DESC);
CREATE INDEX system_logs_session_idx ON system_logs (session_id, id DESC) WHERE session_id IS NOT NULL;
CREATE INDEX system_logs_run_idx     ON system_logs (run_id, id DESC) WHERE run_id IS NOT NULL;
CREATE INDEX system_logs_event_idx   ON system_logs (event_id) WHERE event_id IS NOT NULL;
CREATE INDEX system_logs_observed_idx ON system_logs (observed_at);

CREATE TABLE client_receipts (
    event_id        text    NOT NULL,
    session_id      text    NOT NULL,
    received_at_ms  bigint  NOT NULL,   -- browser clock
    recorded_at     bigint  NOT NULL,   -- server clock
    PRIMARY KEY (event_id, session_id)
);
CREATE INDEX client_receipts_recorded_idx ON client_receipts (recorded_at);
