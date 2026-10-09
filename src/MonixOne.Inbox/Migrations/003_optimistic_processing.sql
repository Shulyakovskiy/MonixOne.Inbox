-- Preserve the installed history; remove only obsolete coordination state.
ALTER TABLE {{schema}}.consumer_state DROP CONSTRAINT consumer_state_sequence_ck;
ALTER TABLE {{schema}}.consumer_state ALTER COLUMN last_sequence DROP NOT NULL;
ALTER TABLE {{schema}}.consumer_state ALTER COLUMN last_sequence DROP DEFAULT;
UPDATE {{schema}}.consumer_state SET last_sequence = NULL WHERE last_sequence = 0;
ALTER TABLE {{schema}}.consumer_state ADD COLUMN version bigint NOT NULL DEFAULT 0;
ALTER TABLE {{schema}}.consumer_state ADD CONSTRAINT consumer_state_sequence_ck
    CHECK (last_sequence IS NULL OR last_sequence > 0);
ALTER TABLE {{schema}}.consumer_state ADD CONSTRAINT consumer_state_version_ck CHECK (version >= 0);
UPDATE {{schema}}.inbox SET status = 'pending' WHERE status = 'waiting_sequence';
ALTER TABLE {{schema}}.consumer_state DROP COLUMN is_blocked, DROP COLUMN blocked_reason,
    DROP COLUMN gap_since, DROP COLUMN expected_sequence;
DROP INDEX {{schema}}.inbox_due_idx;
ALTER TABLE {{schema}}.inbox DROP CONSTRAINT inbox_status_ck;
ALTER TABLE {{schema}}.inbox ADD CONSTRAINT inbox_status_ck
    CHECK (status IN ('pending', 'retry', 'processed', 'skipped', 'dead'));
-- Keep terminal evidence in attempts/DLQ. Schedule only unclosed legacy terminal decisions;
-- their finalization uses the ordinary active head and never invokes the business callback.
UPDATE {{schema}}.inbox i SET status = 'pending', code = 'terminal_policy_transition',
    completed_at = NULL, next_attempt_at = NULL
FROM {{schema}}.consumer_state s
WHERE i.status = 'dead' AND i.handler_id = s.handler_id AND i.producer = s.producer
    AND i.sequence_scope = s.sequence_scope AND i.object_key = s.object_key
    AND (s.last_sequence IS NULL OR i.sequence > s.last_sequence);
CREATE INDEX inbox_active_head_idx ON {{schema}}.inbox
    (handler_id, producer, sequence_scope, object_key, sequence) INCLUDE (received_at, next_attempt_at, status)
    WHERE status IN ('pending', 'retry');
CREATE INDEX inbox_completed_idx ON {{schema}}.inbox (completed_at, id)
    WHERE status IN ('processed', 'skipped', 'dead');
CREATE INDEX inbox_dlq_retention_idx ON {{schema}}.inbox_dlq (created_at, id);
CREATE INDEX inbox_dlq_inbox_idx ON {{schema}}.inbox_dlq (inbox_id) WHERE inbox_id IS NOT NULL;
CREATE INDEX inbox_dlq_related_idx ON {{schema}}.inbox_dlq (related_inbox_id) WHERE related_inbox_id IS NOT NULL;
CREATE INDEX inbox_journal_attempt_idx ON {{schema}}.inbox_journal (attempt_id) WHERE attempt_id IS NOT NULL;
ALTER TABLE {{schema}}.inbox_journal ADD COLUMN decision_id uuid;
ALTER TABLE {{schema}}.inbox_journal ADD CONSTRAINT inbox_journal_decision_uk UNIQUE (decision_id);
ALTER TABLE {{schema}}.schema_migrations DROP COLUMN IF EXISTS minimum_reader_version;
