-- ksqlDB monitoring queries

-- Message throughput by topic
CREATE TABLE topic_throughput AS
SELECT 
    'customer-events' as topic,
    WINDOWSTART as window_start,
    COUNT(*) as message_count
FROM customer_events_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY 1
EMIT CHANGES;

-- Error rate monitoring
CREATE STREAM error_events AS
SELECT 
    'PROCESSING_ERROR' as error_type,
    timestamp,
    'Failed to process customer event' as error_message
FROM customer_events_stream
WHERE user_id IS NULL OR user_id = ''
EMIT CHANGES;

-- Lag monitoring (conceptual - actual implementation varies)
CREATE TABLE consumer_lag AS
SELECT 
    'recommendation-engine' as consumer_group,
    WINDOWSTART as window_start,
    COUNT(*) as processed_messages,
    MAX(timestamp) as latest_processed_timestamp
FROM customer_events_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY 1
EMIT CHANGES;