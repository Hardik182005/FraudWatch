-- runs.rate_eps tracks the *current* rate (it changes with controls); replays need the initial rate.
ALTER TABLE runs ADD COLUMN initial_rate_eps real;
UPDATE runs SET initial_rate_eps = rate_eps WHERE initial_rate_eps IS NULL;
ALTER TABLE runs ALTER COLUMN initial_rate_eps SET NOT NULL;
CREATE INDEX runs_status_idx ON runs (status) WHERE status IN ('starting', 'running', 'paused');
