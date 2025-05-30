# test_consumer.py
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from schema_client import SchemaManager
from config import CONFLUENT_CONFIG
import json

class TestConsumer:
    def __init__(self, topics):
        consumer_config = CONFLUENT_CONFIG.copy()
        consumer_config.update({
            'group.id': 'test-consumer',
            'auto.offset.reset': 'earliest'
        })
        
        self.consumer = Consumer(consumer_config)
        self.topics = topics
        
    def consume_messages(self, timeout=30):
        self.consumer.subscribe(self.topics)
        
        start_time = time.time()
        message_count = 0
        
        try:
            while time.time() - start_time < timeout:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                print(f"Topic: {msg.topic()}")
                print(f"Partition: {msg.partition()}")
                print(f"Offset: {msg.offset()}")
                print(f"Key: {msg.key()}")
                print(f"Value: {msg.value()}")
                print("-" * 50)
                
                message_count += 1
                
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
            print(f"Consumed {message_count} messages")

if __name__ == "__main__":
    # Test different topics
    topics_to_test = [
        'customer-events',
        'order-events',
        'recommendations',
        'fraud-alerts'
    ]
    
    consumer = TestConsumer(topics_to_test)
    consumer.consume_messages(60)  # Run for 1 minute