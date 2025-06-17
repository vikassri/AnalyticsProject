#python unified_consumer.py --env confluent --topic order-events --max-messages 1000 --threads 8
import argparse
import json
import logging
import datetime
import time
from queue import Queue
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from Confluent.schema_client import SchemaManager
from config import CONFLUENT_CONFIG, CLOUDERA_KAFKA_BOOTSTRAP

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("consumer.log"),
        logging.StreamHandler()
    ]
)

def datetime_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def get_consumer_config(env, group_id):
    if env == "confluent":
        config = dict(CONFLUENT_CONFIG)
    elif env == "cloudera":
        config = dict(CLOUDERA_KAFKA_BOOTSTRAP)
    else:
        raise ValueError("Invalid environment. Use 'cloudera' or 'confluent'.")

    config["group.id"] = group_id
    config["auto.offset.reset"] = "earliest"
    return config

def process_message(msg, env, avro_deserializer, topic):
    key = msg.key().decode("utf-8") if msg.key() else None
    try:
        if env == "confluent":
            value = avro_deserializer(
                msg.value(),
                SerializationContext(topic, MessageField.VALUE)
            )
        else:
            value = json.loads(msg.value().decode("utf-8"))
    except Exception as e:
        logging.error(f"❌ Error deserializing message: {e}")
        return

    logging.debug(f"Key: {key}\n{json.dumps(value, indent=2, default=datetime_serializer)}")

def consumer_loop(env, topic, group_id, max_messages, worker_threads=4):
    config = get_consumer_config(env, group_id)
    consumer = Consumer(config)
    avro_deserializer = None
    if env == "confluent":
        schema_manager = SchemaManager()
        avro_deserializer = schema_manager.get_avro_deserializer()

    consumer.subscribe([topic])
    logging.info(f"✅ Subscribed to topic: {topic}")

    queue = Queue(maxsize=1000)
    consumed = 0
    start_time = time.time()

    def worker():
        while True:
            msg = queue.get()
            if msg is None:
                break
            process_message(msg, env, avro_deserializer, topic)
            queue.task_done()

    threads = [Thread(target=worker, daemon=True) for _ in range(worker_threads)]
    for t in threads:
        t.start()

    try:
        while True:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"❌ Consumer error: {msg.error()}")
                continue

            queue.put(msg)
            consumed += 1
            if max_messages and consumed >= max_messages:
                break

    except KeyboardInterrupt:
        logging.info("\n🛑 Consumer interrupted.")

    finally:
        logging.info("👋 Shutting down consumer and workers...")
        consumer.close()
        for _ in threads:
            queue.put(None)
        for t in threads:
            t.join()
        duration = time.time() - start_time
        throughput = consumed / duration if duration > 0 else 0
        logging.info(f"\n✅ Consumed {consumed} messages in {duration:.2f} seconds")
        logging.info(f"📈 Average throughput: {throughput:.2f} messages/sec")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Consumer for Cloudera and Confluent")
    parser.add_argument("--env", required=True, choices=["cloudera", "confluent"], help="Kafka environment")
    parser.add_argument("--topic", default="order-events", help="Kafka topic name")
    parser.add_argument("--group-id", default="order-events-consumer-group", help="Consumer group ID")
    parser.add_argument("--max-messages", type=int, help="Maximum number of messages to consume")
    parser.add_argument("--threads", type=int, default=4, help="Number of worker threads for processing")
    args = parser.parse_args()

    logging.info("🧵 Starting multi-threaded Kafka consumer")
    consumer_loop(args.env, args.topic, args.group_id, args.max_messages, args.threads)