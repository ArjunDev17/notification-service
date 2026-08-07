-- =====================================================
-- Notification Deliveries
-- One notification can have multiple deliveries
--
-- Notification
--      |
--      +------ EMAIL
--      |
--      +------ SMS
--      |
--      +------ PUSH
-- =====================================================

CREATE TABLE IF NOT EXISTS notification_deliveries
(
    id UUID PRIMARY KEY
        DEFAULT gen_random_uuid(),

    notification_id UUID NOT NULL,

    channel VARCHAR(50) NOT NULL
        CHECK (
            channel IN
            (
                'EMAIL',
                'SMS',
                'PUSH',
                'SLACK',
                'WEBHOOK'
            )
        ),

    recipient VARCHAR(255) NOT NULL,

    status VARCHAR(50) NOT NULL
        CHECK (
            status IN
            (
                'PENDING',
                'SENDING',
                'DELIVERED',
                'FAILED',
                'RETRYING'
            )
        ),

    retry_count INTEGER NOT NULL
        DEFAULT 0,

    error_message TEXT,

    sent_at TIMESTAMP,

    delivered_at TIMESTAMP,

    failed_at TIMESTAMP,

    created_at TIMESTAMP NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_notification_delivery
        FOREIGN KEY (notification_id)
        REFERENCES notifications(id)
        ON DELETE CASCADE
);

-- =====================================================
-- Indexes
-- =====================================================

CREATE INDEX idx_delivery_notification
ON notification_deliveries(notification_id);

CREATE INDEX idx_delivery_status
ON notification_deliveries(status);

CREATE INDEX idx_delivery_channel
ON notification_deliveries(channel);

CREATE INDEX idx_delivery_retry
ON notification_deliveries(retry_count);

CREATE INDEX idx_delivery_recipient
ON notification_deliveries(recipient);