# order_events_producer.py
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from AnalyticsProject.Scripts.Confluent.schema_client import SchemaManager
from AnalyticsProject.Scripts.Confluent.config import CONFLUENT_CONFIG
import time
import random
from faker import Faker

fake = Faker()

ORDER_EVENT_SCHEMA = """
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "ecommerce.events",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "event_type", "type": {"type": "enum", "name": "OrderEventType", "symbols": ["created", "updated", "cancelled", "shipped", "delivered", "returned"]}},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "total_amount", "type": "double"},
    {"name": "currency", "type": "string", "default": "USD"},
    {"name": "items", "type": {
      "type": "array",
      "items": {
        "type": "record",
        "name": "OrderItem",
        "fields": [
          {"name": "product_id", "type": "string"},
          {"name": "quantity", "type": "int"},
          {"name": "price", "type": "double"},
          {"name": "category", "type": "string"}
        ]
      }
    }},
    {"name": "shipping_address", "type": {
      "type": "record",
      "name": "Address",
      "fields": [
        {"name": "street", "type": "string"},
        {"name": "city", "type": "string"},
        {"name": "state", "type": "string"},
        {"name": "zip_code", "type": "string"},
        {"name": "country", "type": "string"}
      ]
    }},
    {"name": "payment_method", "type": "string"}
  ]
}
"""

class OrderEventsProducer:
    def __init__(self):
        self.producer = Producer(CONFLUENT_CONFIG)
        self.schema_manager = SchemaManager()
        self.avro_serializer = self.schema_manager.get_avro_serializer(ORDER_EVENT_SCHEMA)
        
    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def generate_order_event(self):
        categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
        payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay"]
        
        items = []
        for _ in range(random.randint(1, 5)):
            items.append({
                "product_id": f"product_{random.randint(1, 500)}",
                "quantity": random.randint(1, 3),
                "price": round(random.uniform(10.0, 200.0), 2),
                "category": random.choice(categories)
            })
        
        total_amount = sum(item["price"] * item["quantity"] for item in items)
        
        event = {
            "order_id": f"order_{fake.uuid4()}",
            "user_id": f"user_{random.randint(1, 1000)}",
            "event_type": "created",
            "timestamp": int(time.time() * 1000),
            "total_amount": round(total_amount, 2),
            "currency": "USD",
            "items": items,
            "shipping_address": {
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state(),
                "zip_code": fake.zipcode(),
                "country": "USA"
            },
            "payment_method": random.choice(payment_methods)
        }
        return event
    
    def produce_events(self, num_events=50):
        for _ in range(num_events):
            event = self.generate_order_event()
            
            self.producer.produce(
                topic='order-events',
                key=event['order_id'],
                value=self.avro_serializer(event, SerializationContext('order-events', MessageField.VALUE)),
                callback=self.delivery_report
            )
            
            time.sleep(2)  # 0.5 orders per second
        
        self.producer.flush()

if __name__ == "__main__":
    producer = OrderEventsProducer()
    producer.produce_events(100)