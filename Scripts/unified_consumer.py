# python unified_consumer.py --env confluent --topic order-events --group-id mygroup-1
import argparse
import json
import logging
import datetime
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from Confluent.schema_client import SchemaManager  # your internal schema manager
from config import CONFLUENT_CONFIG, CLOUDERA_KAFKA_BOOTSTRAP

logging.basicConfig(level=logging.INFO)

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
    config["enable.auto.commit"] = True
    return config

def consume_messages(env, topic, group_id):
    logging.info(f"📥 Starting consumer on '{env}' for topic '{topic}'...")
    config = get_consumer_config(env, group_id)
    consumer = Consumer(config)
    avro_deserializer = None

    if env == "confluent":
        schema_manager = SchemaManager()
        avro_deserializer = schema_manager.get_avro_deserializer()

    consumer.subscribe([topic])
    logging.info(f"✅ Subscribed to topic: {topic}")

    try:
        while True:
            msg = consumer.poll(10)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"❌ Consumer error: {msg.error()}")
                continue

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
                continue

            print(f"\nKey: {key}")
            print(json.dumps(value, indent=2, default=datetime_serializer))

    except KeyboardInterrupt:
        print("\n🛑 Consumer interrupted.")

    finally:
        print("👋 Closing consumer...")
        consumer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Consumer for Cloudera and Confluent")
    parser.add_argument("--env", required=True, choices=["cloudera", "confluent"], help="Kafka environment")
    parser.add_argument("--topic", default="order-events", help="Kafka topic name")
    parser.add_argument("--group-id", default="order-events-consumer-group", help="Consumer group ID")
    args = parser.parse_args()

    consume_messages(args.env, args.topic, args.group_id)