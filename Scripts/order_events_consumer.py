from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from schema_client import SchemaManager
from config import CONFLUENT_CONFIG
import json
import sys
import datetime

class OrderEventsConsumer:
    def __init__(self, group_id="order-events-consumer-group"):
        # Add group.id to config
        self.config = dict(CONFLUENT_CONFIG)
        self.config["group.id"] = group_id
        self.config["auto.offset.reset"] = "earliest"

        self.consumer = Consumer(self.config)
        self.schema_manager = SchemaManager()
        self.avro_deserializer = self.schema_manager.get_avro_deserializer()

    def datetime_serializer(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()  # Or use: int(obj.timestamp() * 1000) for epoch millis
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    def consume_events(self, topic='order-events', timeout=1.0):
        self.consumer.subscribe([topic])
        print(f"Subscribed to topic: {topic}")

        try:
            while True:
                msg = self.consumer.poll(timeout)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                value = self.avro_deserializer(
                    msg.value(),
                    SerializationContext(topic, MessageField.VALUE)
                )

                key = msg.key().decode('utf-8') if msg.key() else None
                print(f"\nReceived message with key: {key}")
                print(json.dumps(value, indent=2, default=self.datetime_serializer))

        except KeyboardInterrupt:
            print("Consumer interrupted. Closing...")

        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = OrderEventsConsumer()
    consumer.consume_events()