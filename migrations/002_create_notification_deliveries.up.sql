CREATE TABLE IF NOT EXISTS notification_deliveries
(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    notification_id UUID NOT NULL,

    channel VARCHAR(50) NOT NULL,

    recipient VARCHAR(255) NOT NULL,

    status VARCHAR(50) NOT NULL,

    retry_count INTEGER NOT NULL DEFAULT 0,

    error_message TEXT,

    sent_at TIMESTAMP,

    delivered_at TIMESTAMP,

    failed_at TIMESTAMP,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_notification_delivery
        FOREIGN KEY(notification_id)
        REFERENCES notifications(id)
        ON DELETE CASCADE
);

CREATE INDEX idx_delivery_notification
ON notification_deliveries(notification_id);

CREATE INDEX idx_delivery_status
ON notification_deliveries(status);

CREATE INDEX idx_delivery_channel
ON notification_deliveries(channel);

CREATE INDEX idx_delivery_retry
ON notification_deliveries(retry_count);