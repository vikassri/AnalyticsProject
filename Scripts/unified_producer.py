# python unified_producer.py --env confluent --topic order-events --count 1000

import argparse
import time, json
import random
import uuid
import logging
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from Confluent.schema_client import SchemaManager
from config import CONFLUENT_CONFIG, CLOUDERA_KAFKA_BOOTSTRAP


logging.basicConfig(level=logging.INFO)
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

def generate_order_event():
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

    return {
        "order_id": f"order_{uuid.uuid4()}",
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

def get_producer_config(environment):
    if environment == "confluent":
        return CONFLUENT_CONFIG
    elif environment == "cloudera":
        return CLOUDERA_KAFKA_BOOTSTRAP
    else:
        raise ValueError("Invalid environment. Use 'cloudera' or 'confluent'.")

def delivery_report(err, msg):
    if err:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_messages(environment, topic, count):
    logging.info(f"Producing {count} messages to '{topic}' on {environment} cluster.")
    config = get_producer_config(environment)
    producer = Producer(config)

    if environment == "confluent":
        schema_manager = SchemaManager()
        serializer = schema_manager.get_avro_serializer(ORDER_EVENT_SCHEMA)

    try:
        for i in range(count):
            event = generate_order_event()
            value = (serializer(event, SerializationContext(topic, MessageField.VALUE))
                     if environment == "confluent" else
                     json.dumps(event))

            producer.produce(
                topic=topic,
                key=f"{event['order_id']}_{i}",
                value=value,
                callback=delivery_report
            )
            print(f"Produced message {i + 1}/{count}: {event['order_id']}")
            time.sleep(1)  # adjustable rate

    except KeyboardInterrupt:
        logging.warning("⚠️ Keyboard interrupt detected. Stopping message production...")

    finally:
        logging.info("Flushing producer...")
        producer.flush()
        logging.info("✅ Producer shutdown complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka order event producer for Cloudera and Confluent.")
    parser.add_argument("--env", required=True, choices=["cloudera", "confluent"], help="Kafka environment")
    parser.add_argument("--topic", default="order-events", help="Kafka topic")
    parser.add_argument("--count", type=int, default=10, help="Number of messages to produce")
    args = parser.parse_args()

    produce_messages(args.env, args.topic, args.count)