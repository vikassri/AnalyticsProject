# production_config.py
import os

# Production Confluent Cloud Configuration
PROD_CONFLUENT_CONFIG = {
    'bootstrap.servers': os.getenv('CONFLUENT_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('CONFLUENT_API_KEY'),
    'sasl.password': os.getenv('CONFLUENT_API_SECRET'),
    'client.id': f"{os.getenv('SERVICE_NAME', 'ecommerce-service')}-{os.getenv('INSTANCE_ID', '1')}",
    
    # Producer configurations
    'acks': 'all',
    'retries': 10,
    'max.in.flight.requests.per.connection': 5,
    'enable.idempotence': True,
    'compression.type': 'snappy',
    'batch.size': 32768,
    'linger.ms': 10,
    
    # Consumer configurations
    'enable.auto.commit': False,
    'max.poll.interval.ms': 300000,
    'session.timeout.ms': 45000,
    'heartbeat.interval.ms': 3000,
}

PROD_SCHEMA_REGISTRY_CONFIG = {
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_KEY')}:{os.getenv('SCHEMA_REGISTRY_SECRET')}"
}

# Monitoring Configuration
MONITORING_CONFIG = {
    'metrics_port': int(os.getenv('METRICS_PORT', '8080')),
    'log_level': os.getenv('LOG_LEVEL', 'INFO'),
    'enable_jmx': os.getenv('ENABLE_JMX', 'true').lower() == 'true'
}