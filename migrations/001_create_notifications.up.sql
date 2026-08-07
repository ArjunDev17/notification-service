-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =====================================================
-- Notifications
-- One business notification
-- Example:
--   Course Created
--   User Registered
-- =====================================================

CREATE TABLE IF NOT EXISTS notifications
(
    id UUID PRIMARY KEY
        DEFAULT gen_random_uuid(),

    type VARCHAR(100) NOT NULL
        CHECK (
            type IN
            (
                'COURSE_CREATED',
                'COURSE_UPDATED',
                'COURSE_DELETED',
                'USER_REGISTERED',
                'PASSWORD_RESET',
                'PAYMENT_SUCCESS'
            )
        ),

    title VARCHAR(255) NOT NULL,

    message TEXT NOT NULL,

    metadata JSONB NOT NULL
        DEFAULT '{}'::jsonb,

    created_at TIMESTAMP NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- Indexes
-- =====================================================

CREATE INDEX idx_notifications_type
ON notifications(type);

CREATE INDEX idx_notifications_created_at
ON notifications(created_at DESC);