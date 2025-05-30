-- Real-time revenue by category (5-minute windows)
CREATE TABLE revenue_by_category AS
SELECT 
    category,
    WINDOWSTART as window_start,
    WINDOWEND as window_end,
    COUNT(*) as order_count,
    SUM(price * quantity) as total_revenue,
    AVG(price * quantity) as avg_order_value
FROM order_events_stream o
INNER JOIN UNNEST(o.items) as item
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY category
EMIT CHANGES;

-- Popular products (hourly)
CREATE TABLE popular_products AS
SELECT 
    product_id,
    WINDOWSTART as window_start,
    COUNT(*) as view_count,
    COUNT_DISTINCT(user_id) as unique_viewers
FROM customer_events_stream
WHERE event_type = 'product_view'
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY product_id
HAVING COUNT(*) > 10
EMIT CHANGES;

-- User session analysis
CREATE TABLE user_sessions AS
SELECT 
    user_id,
    session_id,
    WINDOWSTART as session_start,
    WINDOWEND as session_end,
    COUNT(*) as event_count,
    COUNT_DISTINCT(event_type) as unique_events,
    COLLECT_LIST(event_type) as event_sequence
FROM customer_events_stream
WINDOW SESSION (30 MINUTES)
GROUP BY user_id, session_id
EMIT CHANGES;

-- Low inventory alerts
CREATE STREAM low_inventory_alerts AS
SELECT 
    product_id,
    sku,
    current_stock,
    category,
    warehouse_id,
    timestamp
FROM inventory_events_stream
WHERE current_stock < 10 AND event_type = 'stock_update'
EMIT CHANGES;

-- Fraud detection - high value orders
CREATE STREAM potential_fraud AS
SELECT 
    order_id,
    user_id,
    total_amount,
    payment_method,
    timestamp,
    'HIGH_VALUE_ORDER' as fraud_reason
FROM order_events_stream
WHERE total_amount > 1000
EMIT CHANGES;

-- Customer journey tracking
CREATE TABLE customer_funnel AS
SELECT 
    user_id,
    WINDOWSTART as window_start,
    COUNT_DISTINCT(CASE WHEN event_type = 'page_view' THEN session_id END) as page_views,
    COUNT_DISTINCT(CASE WHEN event_type = 'product_view' THEN product_id END) as products_viewed,
    COUNT_DISTINCT(CASE WHEN event_type = 'add_to_cart' THEN product_id END) as items_added_to_cart,
    COUNT_DISTINCT(CASE WHEN event_type = 'search' THEN search_query END) as searches
FROM customer_events_stream
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY user_id
EMIT CHANGES;