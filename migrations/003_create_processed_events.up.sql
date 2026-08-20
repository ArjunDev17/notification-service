CREATE TABLE IF NOT EXISTS processed_events
(
    id UUID PRIMARY KEY,

    event_id UUID NOT NULL,

    event_type VARCHAR(100) NOT NULL,

    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_processed_events_event_id
        UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS idx_processed_events_event_type
ON processed_events(event_type);

CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at
ON processed_events(processed_at);