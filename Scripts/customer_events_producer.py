# customer_events_producer.py
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from schema_client import SchemaManager
from config import CONFLUENT_CONFIG
import json
import time
import random
from faker import Faker

fake = Faker()

# Customer Event Schema
CUSTOMER_EVENT_SCHEMA = """
{
  "type": "record",
  "name": "CustomerEvent",
  "namespace": "ecommerce.events",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "session_id", "type": "string"},
    {"name": "event_type", "type": {"type": "enum", "name": "EventType", "symbols": ["page_view", "product_view", "add_to_cart", "search", "click"]}},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "page_url", "type": ["null", "string"], "default": null},
    {"name": "product_id", "type": ["null", "string"], "default": null},
    {"name": "search_query", "type": ["null", "string"], "default": null},
    {"name": "user_agent", "type": "string"},
    {"name": "ip_address", "type": "string"},
    {"name": "referrer", "type": ["null", "string"], "default": null}
  ]
}
"""

class CustomerEventsProducer:
    def __init__(self):
        self.producer = Producer(CONFLUENT_CONFIG)
        self.schema_manager = SchemaManager()
        self.avro_serializer = self.schema_manager.get_avro_serializer(CUSTOMER_EVENT_SCHEMA)
        
    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def generate_customer_event(self):
        event_types = ["page_view", "product_view", "add_to_cart", "search", "click"]
        event_type = random.choice(event_types)
        
        event = {
            "user_id": f"user_{random.randint(1, 1000)}",
            "session_id": fake.uuid4(),
            "event_type": event_type,
            "timestamp": int(time.time() * 1000),
            "page_url": fake.url() if event_type == "page_view" else None,
            "product_id": f"product_{random.randint(1, 500)}" if event_type in ["product_view", "add_to_cart"] else None,
            "search_query": fake.word() if event_type == "search" else None,
            "user_agent": fake.user_agent(),
            "ip_address": fake.ipv4(),
            "referrer": fake.url() if random.choice([True, False]) else None
        }
        return event
    
    def produce_events(self, num_events=100):
        for _ in range(num_events):
            event = self.generate_customer_event()
            
            self.producer.produce(
                topic='customer-events',
                key=event['user_id'],
                value=self.avro_serializer(event, SerializationContext('customer-events', MessageField.VALUE)),
                callback=self.delivery_report
            )
            
            time.sleep(0.1)  # 10 events per second
        
        self.producer.flush()

if __name__ == "__main__":
    producer = CustomerEventsProducer()
    producer.produce_events(10)