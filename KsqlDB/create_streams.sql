-- Create customer events stream
CREATE STREAM customer_events_stream (
    user_id VARCHAR,
    session_id VARCHAR,
    event_type VARCHAR,
    timestamp BIGINT,
    page_url VARCHAR,
    product_id VARCHAR,
    search_query VARCHAR,
    user_agent VARCHAR,
    ip_address VARCHAR,
    referrer VARCHAR
) WITH (
    KAFKA_TOPIC='customer-events',
    VALUE_FORMAT='AVRO',
    TIMESTAMP='timestamp'
);

-- Create order events stream
CREATE STREAM order_events_stream (
    order_id VARCHAR,
    user_id VARCHAR,
    event_type VARCHAR,
    timestamp BIGINT,
    total_amount DOUBLE,
    currency VARCHAR,
    items ARRAY<STRUCT<
        product_id VARCHAR,
        quantity INT,
        price DOUBLE,
        category VARCHAR
    >>,
    shipping_address STRUCT<
        street VARCHAR,
        city VARCHAR,
        state VARCHAR,
        zip_code VARCHAR,
        country VARCHAR
    >,
    payment_method VARCHAR
) WITH (
    KAFKA_TOPIC='order-events',
    VALUE_FORMAT='AVRO',
    TIMESTAMP='timestamp'
);

-- Create inventory events stream
CREATE STREAM inventory_events_stream (
    product_id VARCHAR,
    sku VARCHAR,
    event_type VARCHAR,
    timestamp BIGINT,
    current_stock INT,
    previous_stock INT,
    current_price DOUBLE,
    previous_price DOUBLE,
    category VARCHAR,
    warehouse_id VARCHAR
) WITH (
    KAFKA_TOPIC='inventory-events',
    VALUE_FORMAT='AVRO',
    TIMESTAMP='timestamp'
);