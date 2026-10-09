-- Append new numbered migrations instead of changing an installed script.
-- Sequence belongs to the producer; last_sequence is the last CLOSED number.
CREATE TABLE {{schema}}.consumer_state (
    handler_id varchar(128) NOT NULL,
    producer text NOT NULL,
    sequence_scope text NOT NULL,
    object_key text NOT NULL,
    last_sequence bigint NOT NULL DEFAULT 0,
    is_blocked boolean NOT NULL DEFAULT false,
    blocked_reason text,
    gap_since timestamptz,
    expected_sequence bigint,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT consumer_state_pk PRIMARY KEY (handler_id, producer, sequence_scope, object_key),
    -- Byte limits keep compound btree keys below PostgreSQL's index-entry limit, including UTF-8.
    CONSTRAINT consumer_state_key_ck CHECK (
        length(handler_id) BETWEEN 1 AND 128 AND octet_length(handler_id) <= 128 AND
        octet_length(producer) BETWEEN 1 AND 256 AND
        octet_length(sequence_scope) BETWEEN 1 AND 256 AND
        octet_length(object_key) BETWEEN 1 AND 1024),
    CONSTRAINT consumer_state_sequence_ck CHECK (last_sequence >= 0 AND (expected_sequence IS NULL OR expected_sequence > 0))
);

CREATE INDEX consumer_state_ready_idx ON {{schema}}.consumer_state (handler_id, updated_at) WHERE NOT is_blocked;
CREATE INDEX consumer_state_gap_idx ON {{schema}}.consumer_state (handler_id, gap_since) WHERE gap_since IS NOT NULL;

CREATE TABLE {{schema}}.inbox (
    id uuid NOT NULL,
    handler_id varchar(128) NOT NULL,
    subscription_id varchar(128) NOT NULL,
    event_id text NOT NULL,
    producer text NOT NULL,
    sequence_scope text NOT NULL,
    object_key text NOT NULL,
    sequence bigint NOT NULL,
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    envelope jsonb NOT NULL,
    raw_payload bytea NOT NULL,
    source jsonb NOT NULL,
    status varchar(32) NOT NULL DEFAULT 'pending',
    attempt_count integer NOT NULL DEFAULT 0,
    run_id uuid NOT NULL,
    current_attempt_id uuid,
    next_attempt_at timestamptz,
    completed_at timestamptz,
    code text,
    reason text,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT inbox_pk PRIMARY KEY (id),
    CONSTRAINT inbox_event_uk UNIQUE (handler_id, event_id),
    CONSTRAINT inbox_sequence_uk UNIQUE (handler_id, producer, sequence_scope, object_key, sequence),
    CONSTRAINT inbox_state_fk FOREIGN KEY (handler_id, producer, sequence_scope, object_key)
        REFERENCES {{schema}}.consumer_state (handler_id, producer, sequence_scope, object_key) ON DELETE RESTRICT,
    CONSTRAINT inbox_sequence_ck CHECK (sequence > 0),
    CONSTRAINT inbox_attempt_count_ck CHECK (attempt_count >= 0),
    CONSTRAINT inbox_identifiers_ck CHECK (
        octet_length(event_id) BETWEEN 1 AND 512 AND length(event_type) > 0 AND
        length(subscription_id) BETWEEN 1 AND 128 AND octet_length(subscription_id) <= 128),
    CONSTRAINT inbox_status_ck CHECK (status IN ('pending', 'waiting_sequence', 'retry', 'processed', 'skipped', 'dead')),
    -- A custom extractor may use an array/scalar envelope; transport metadata is always an object.
    CONSTRAINT inbox_json_ck CHECK (jsonb_typeof(source) = 'object'),
    CONSTRAINT inbox_retry_ck CHECK (status <> 'retry' OR next_attempt_at IS NOT NULL)
);

CREATE INDEX inbox_due_idx ON {{schema}}.inbox (handler_id, next_attempt_at, received_at)
    WHERE status IN ('pending', 'waiting_sequence', 'retry');
CREATE INDEX inbox_source_idx ON {{schema}}.inbox (handler_id, subscription_id, received_at);

CREATE TABLE {{schema}}.inbox_attempts (
    attempt_id uuid NOT NULL,
    inbox_id uuid NOT NULL,
    run_id uuid NOT NULL,
    attempt_number integer NOT NULL,
    worker_instance_id text NOT NULL,
    handler_type text NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    duration_ms bigint,
    outcome varchar(32) NOT NULL,
    code text,
    reason text,
    details jsonb,
    exception text,
    next_attempt_at timestamptz,
    correlation_id text,
    trace_id text,
    diagnostics jsonb NOT NULL DEFAULT '[]'::jsonb,
    settings jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT inbox_attempts_pk PRIMARY KEY (attempt_id),
    CONSTRAINT inbox_attempts_number_uk UNIQUE (inbox_id, run_id, attempt_number),
    CONSTRAINT inbox_attempts_inbox_fk FOREIGN KEY (inbox_id) REFERENCES {{schema}}.inbox (id) ON DELETE RESTRICT,
    CONSTRAINT inbox_attempts_number_ck CHECK (attempt_number > 0),
    CONSTRAINT inbox_attempts_time_ck CHECK (
        (finished_at IS NULL OR finished_at >= started_at) AND (duration_ms IS NULL OR duration_ms >= 0)),
    CONSTRAINT inbox_attempts_outcome_ck CHECK (outcome IN ('started', 'applied', 'ignored', 'retry', 'rejected', 'timeout', 'interrupted', 'unknown')),
    CONSTRAINT inbox_attempts_json_ck CHECK (jsonb_typeof(diagnostics) = 'array' AND jsonb_typeof(settings) = 'object')
);

CREATE TABLE {{schema}}.inbox_dlq (
    id uuid NOT NULL,
    inbox_id uuid,
    related_inbox_id uuid,
    handler_id varchar(128) NOT NULL,
    subscription_id varchar(128) NOT NULL,
    deduplication_key text NOT NULL,
    category varchar(32) NOT NULL,
    event_id text,
    producer text,
    sequence_scope text,
    object_key text,
    sequence bigint,
    event_type text,
    envelope jsonb,
    raw_payload bytea NOT NULL,
    source jsonb NOT NULL,
    code text NOT NULL,
    reason text NOT NULL,
    exception text,
    last_sequence bigint,
    dlq_policy varchar(32),
    cursor_advanced boolean NOT NULL DEFAULT false,
    run_id uuid,
    worker_instance_id text NOT NULL,
    handler_type text NOT NULL,
    package_version text NOT NULL,
    settings jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT inbox_dlq_pk PRIMARY KEY (id),
    CONSTRAINT inbox_dlq_delivery_uk UNIQUE (handler_id, deduplication_key),
    -- RESTRICT preserves the original event and its evidence while a DLQ record exists.
    CONSTRAINT inbox_dlq_inbox_fk FOREIGN KEY (inbox_id) REFERENCES {{schema}}.inbox (id) ON DELETE RESTRICT,
    CONSTRAINT inbox_dlq_related_fk FOREIGN KEY (related_inbox_id) REFERENCES {{schema}}.inbox (id) ON DELETE RESTRICT,
    CONSTRAINT inbox_dlq_category_ck CHECK (category IN ('invalid_message', 'conflict', 'terminal')),
    CONSTRAINT inbox_dlq_sequence_ck CHECK ((sequence IS NULL OR sequence > 0) AND (last_sequence IS NULL OR last_sequence >= 0)),
    CONSTRAINT inbox_dlq_key_ck CHECK (
        octet_length(deduplication_key) BETWEEN 1 AND 512 AND
        octet_length(handler_id) BETWEEN 1 AND 128 AND octet_length(subscription_id) BETWEEN 1 AND 128 AND
        (event_id IS NULL OR octet_length(event_id) BETWEEN 1 AND 512) AND
        (producer IS NULL OR octet_length(producer) BETWEEN 1 AND 256) AND
        (sequence_scope IS NULL OR octet_length(sequence_scope) BETWEEN 1 AND 256) AND
        (object_key IS NULL OR octet_length(object_key) BETWEEN 1 AND 1024)),
    CONSTRAINT inbox_dlq_valid_event_ck CHECK (category = 'invalid_message' OR
        (event_id IS NOT NULL AND producer IS NOT NULL AND sequence_scope IS NOT NULL AND
         object_key IS NOT NULL AND sequence IS NOT NULL AND event_type IS NOT NULL)),
    CONSTRAINT inbox_dlq_terminal_ck CHECK (category <> 'terminal' OR (inbox_id IS NOT NULL AND run_id IS NOT NULL AND dlq_policy IS NOT NULL)),
    CONSTRAINT inbox_dlq_policy_ck CHECK (dlq_policy IS NULL OR dlq_policy IN ('SkipAndAdvance', 'BlockStream')),
    CONSTRAINT inbox_dlq_cursor_ck CHECK (NOT cursor_advanced OR
        (category = 'terminal' AND dlq_policy IS NOT NULL AND dlq_policy = 'SkipAndAdvance')),
    CONSTRAINT inbox_dlq_json_ck CHECK (jsonb_typeof(source) = 'object' AND jsonb_typeof(settings) = 'object')
);

CREATE INDEX inbox_dlq_handler_idx ON {{schema}}.inbox_dlq (handler_id, created_at, id);
CREATE INDEX inbox_dlq_object_idx ON {{schema}}.inbox_dlq (handler_id, producer, sequence_scope, object_key, created_at)
    WHERE object_key IS NOT NULL;

CREATE TABLE {{schema}}.inbox_journal (
    id bigint GENERATED ALWAYS AS IDENTITY,
    inbox_id uuid,
    dlq_id uuid,
    attempt_id uuid,
    handler_id varchar(128) NOT NULL,
    subscription_id varchar(128),
    worker_instance_id text NOT NULL,
    decision text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT inbox_journal_pk PRIMARY KEY (id),
    CONSTRAINT inbox_journal_inbox_fk FOREIGN KEY (inbox_id) REFERENCES {{schema}}.inbox (id) ON DELETE RESTRICT,
    CONSTRAINT inbox_journal_dlq_fk FOREIGN KEY (dlq_id) REFERENCES {{schema}}.inbox_dlq (id) ON DELETE RESTRICT,
    CONSTRAINT inbox_journal_attempt_fk FOREIGN KEY (attempt_id) REFERENCES {{schema}}.inbox_attempts (attempt_id) ON DELETE RESTRICT,
    CONSTRAINT inbox_journal_decision_ck CHECK (length(decision) > 0),
    CONSTRAINT inbox_journal_json_ck CHECK (jsonb_typeof(details) = 'object')
);

CREATE INDEX inbox_journal_inbox_idx ON {{schema}}.inbox_journal (inbox_id, id) WHERE inbox_id IS NOT NULL;
CREATE INDEX inbox_journal_dlq_idx ON {{schema}}.inbox_journal (dlq_id, id) WHERE dlq_id IS NOT NULL;
CREATE INDEX inbox_journal_handler_idx ON {{schema}}.inbox_journal (handler_id, created_at, id);
