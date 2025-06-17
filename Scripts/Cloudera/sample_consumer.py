#python unified_kafka_consumer.py --env confluent --topic order-events --group-id mygroup

from confluent_kafka import Consumer
import json
import datetime
import logging
from config import CLOUDERA_KAFKA_BOOTSTRAP

logging.basicConfig(level=logging.INFO)

class KafkaClient:
    def __init__(self, topic):
        self.topic = topic
        self.consumer = Consumer(CLOUDERA_KAFKA_BOOTSTRAP)

    def datetime_serializer(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    def consume(self):
        self.consumer.subscribe([self.topic])
        logging.info(f"✅ Subscribed to topic: {self.topic}")

        try:
            while True:
                msg = self.consumer.poll(10)
                if msg is None:
                    continue
                if msg.error():
                    logging.error(f"❌ Consumer error: {msg.error()}")
                    continue

                try:
                    value_bytes = msg.value()
                    value_str = value_bytes.decode('utf-8') if value_bytes else '{}'
                    value = json.loads(value_str)
                except (UnicodeDecodeError, json.JSONDecodeError) as e:
                    logging.error(f"❌ Failed to decode/parse message value: {e}")
                    continue

                key = msg.key().decode('utf-8') if msg.key() else None
                print(f"\nKey: {key}")
                print(f"Value:\n{json.dumps(value, indent=2, default=self.datetime_serializer)}")

        except KeyboardInterrupt:
            print("\n🛑 Consumer interrupted by user.")

        finally:
            print("👋 Closing consumer...")
            self.consumer.close()

# Run the client
if __name__ == "__main__":
    kafka = KafkaClient('messages')
    kafka.consume()