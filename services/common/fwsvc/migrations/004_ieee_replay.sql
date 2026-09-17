-- Dataset override: events are replayed IEEE-CIS records (schema 1.1).
-- Amounts are the source TransactionAmt in source units (no currency conversion), and synthetic merchant/city
-- fields are no longer produced. Rows persisted before this migration (source_ref IS NULL) came from the retired
-- synthetic generator and carry amounts in the retired INR convention; the API marks them as legacy.

ALTER TABLE scored_events RENAME COLUMN amount_inr TO amount;
ALTER TABLE scored_events DROP COLUMN merchant_name;
ALTER TABLE scored_events DROP COLUMN merchant_category;
ALTER TABLE scored_events DROP COLUMN city;
ALTER TABLE scored_events ADD COLUMN source_ref text;          -- "ieee-cis:<TransactionID>"
ALTER TABLE scored_events ADD COLUMN source_dt_s bigint;       -- source TransactionDT (seconds from the dataset reference point)
ALTER TABLE scored_events ADD COLUMN modified boolean NOT NULL DEFAULT false;  -- feature values altered by a demo scenario
CREATE INDEX scored_events_source_ref_idx ON scored_events (source_ref) WHERE source_ref IS NOT NULL;

ALTER TABLE alerts RENAME COLUMN amount_inr TO amount;
ALTER TABLE alerts DROP COLUMN merchant_name;
ALTER TABLE alerts ADD COLUMN source_ref text;
ALTER TABLE alerts ADD COLUMN modified boolean NOT NULL DEFAULT false;

ALTER TABLE window_aggregates RENAME COLUMN amount_sum_inr TO amount_sum;

-- Evaluation-only labels for replayed records (loaded by the ops-agent from the replay pack's labels.json).
-- Never joined into the live feed, Kafka events or scoring requests.
CREATE TABLE replay_labels (
    source_ref  text PRIMARY KEY,
    is_fraud    boolean NOT NULL,
    pack_id     text NOT NULL
);
