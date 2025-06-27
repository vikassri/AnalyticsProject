#python unified_producer_multithread.py --env confluent --topic order-events --count 1000 --threads 8
import argparse
import time
import json
import random
import uuid
import logging
import threading
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from Confluent.schema_client import SchemaManager
from config import CONFLUENT_CONFIG, CLOUDERA_KAFKA_BOOTSTRAP

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
    handlers=[
        logging.FileHandler("producer.log"),
        logging.StreamHandler()
    ]
)

fake = Faker()

ORDER_EVENT_SCHEMA = """
{ "type": "record", "name": "OrderEvent", "namespace": "ecommerce.events", "fields": [
  {"name": "order_id", "type": "string"},
  {"name": "user_id", "type": "string"},
  {"name": "event_type", "type": {"type": "enum", "name": "OrderEventType", "symbols": ["created", "updated", "cancelled", "shipped", "delivered", "returned"]}},
  {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
  {"name": "total_amount", "type": "double"},
  {"name": "currency", "type": "string", "default": "USD"},
  {"name": "items", "type": {"type": "array", "items": {"type": "record", "name": "OrderItem", "fields": [
    {"name": "product_id", "type": "string"},
    {"name": "quantity", "type": "int"},
    {"name": "price", "type": "double"},
    {"name": "category", "type": "string"}
  ]}}},
  {"name": "shipping_address", "type": {"type": "record", "name": "Address", "fields": [
    {"name": "street", "type": "string"},
    {"name": "city", "type": "string"},
    {"name": "state", "type": "string"},
    {"name": "zip_code", "type": "string"},
    {"name": "country", "type": "string"}]}},
  {"name": "payment_method", "type": "string"}
]}"""

def generate_order_event():
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
    payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay"]
    items = [{
        "product_id": f"product_{random.randint(1, 500)}",
        "quantity": random.randint(1, 3),
        "price": round(random.uniform(10.0, 200.0), 2),
        "category": random.choice(categories)
    } for _ in range(random.randint(1, 5))]
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
        logging.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def producer_thread(producer, serializer, environment, topic, batch, results):
    start_time = time.time()
    for _ in range(batch):
        event = generate_order_event()
        value = serializer(event, SerializationContext(topic, MessageField.VALUE)) if environment == "confluent" else json.dumps(event)
        producer.produce(topic=topic, key=event['order_id'], value=value, callback=delivery_report)
    producer.flush()
    end_time = time.time()
    duration = end_time - start_time
    results.append((batch, duration))
    logging.info(f"Thread sent {batch} messages in {duration:.2f} seconds ({batch/duration:.2f} msg/sec)")

def produce_messages(environment, topic, count, threads=4):
    logging.info(f"Producing {count} messages to '{topic}' on {environment} cluster with {threads} threads.")
    config = get_producer_config(environment)
    producer = Producer(config)
    serializer = None
    if environment == "confluent":
        schema_manager = SchemaManager()
        serializer = schema_manager.get_avro_serializer(ORDER_EVENT_SCHEMA)

    batch_size = count // threads
    thread_list = []
    results = []

    try:
        for i in range(threads):
            t = threading.Thread(
                target=producer_thread,
                args=(producer, serializer, environment, topic, batch_size, results),
                name=f"ProducerThread-{i+1}"
            )
            thread_list.append(t)
            t.start()

        for t in thread_list:
            t.join()

    except KeyboardInterrupt:
        logging.warning("🛑 KeyboardInterrupt detected. Flushing and stopping producers...")
        producer.flush()

    total_msgs = sum(r[0] for r in results)
    total_time = sum(r[1] for r in results)
    avg_throughput = total_msgs / total_time if total_time > 0 else 0
    logging.info(f"\n✅ Produced {total_msgs} messages in {total_time:.2f} seconds")
    logging.info(f"📈 Average throughput: {avg_throughput:.2f} messages/sec")

def main():
    parser = argparse.ArgumentParser(description="Kafka order event producer for Cloudera and Confluent.")
    parser.add_argument("--env", required=True, choices=["cloudera", "confluent"], help="Kafka environment")
    parser.add_argument("--topic", default="order-events", help="Kafka topic")
    parser.add_argument("--count", type=int, default=10, help="Number of messages to produce")
    parser.add_argument("--threads", type=int, default=4, help="Number of producer threads")
    args = parser.parse_args()

    try:
        produce_messages(args.env, args.topic, args.count, args.threads)
    except KeyboardInterrupt:
        logging.warning("🛑 Production interrupted from terminal.")

if __name__ == "__main__":
    main()