-- Only blocked/gapped keys are needed for metric snapshots; avoid scanning healthy streams.
-- This additive index leaves the table format compatible with reader version 1.
CREATE INDEX consumer_state_attention_idx ON {{schema}}.consumer_state (handler_id)
    INCLUDE (is_blocked, gap_since)
    WHERE is_blocked OR gap_since IS NOT NULL;
