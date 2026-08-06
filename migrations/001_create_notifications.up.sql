CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS notifications
(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    type VARCHAR(100) NOT NULL,

    title VARCHAR(255) NOT NULL,

    message TEXT NOT NULL,

    metadata JSONB,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_notifications_type
ON notifications(type);

CREATE INDEX idx_notifications_created_at
ON notifications(created_at DESC);