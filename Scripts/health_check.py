# health_check.py
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from config import CONFLUENT_CONFIG
import time, os, sys

class SuppressStderr:
    def __enter__(self):
        self.stderr_fd = sys.stderr.fileno()
        self.old_stderr = os.dup(self.stderr_fd)
        self.devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(self.devnull, self.stderr_fd)

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.dup2(self.old_stderr, self.stderr_fd)
        os.close(self.devnull)
        os.close(self.old_stderr)


class HealthChecker:
    def __init__(self):
        self.admin_client = AdminClient(CONFLUENT_CONFIG)
        self.producer = Producer(CONFLUENT_CONFIG)
        
    def check_cluster_health(self):
        try:
            # Get cluster metadata
            metadata = self.admin_client.list_topics(timeout=10)
            print(f"✓ Cluster is accessible")
            print(f"✓ Found {len(metadata.topics)} topics")
            
            # Check if our topics exist
            required_topics = [
                'customer-events', 'order-events', 'inventory-events',
                'recommendations', 'fraud-alerts'
            ]
            
            missing_topics = []
            for topic in required_topics:
                if topic not in metadata.topics:
                    missing_topics.append(topic)
            
            if missing_topics:
                print(f"✗ Missing topics: {missing_topics}")
                return False
            else:
                print(f"✓ All required topics exist")
                
            return True
            
        except Exception as e:
            print(f"✗ Cluster health check failed: {e}")
            return False
    
    def test_produce_consume(self):
        try:
            # Test message
            test_message = {
                'test_id': 'health_check',
                'timestamp': int(time.time() * 1000),
                'status': 'testing'
            }
            
            with SuppressStderr():
                # Produce test message
                self.producer.produce(
                    topic='customer-events',
                    key='health_check',
                    value=str(test_message).encode('utf-8')
                )
                self.producer.flush()
                print("✓ Test message produced successfully")
            
            # Consume test message
            consumer_config = CONFLUENT_CONFIG.copy()
            consumer_config.update({
                'group.id': 'health-check-consumer',
                'auto.offset.reset': 'earliest'
            })
            consumer = Consumer(consumer_config)
            consumer.subscribe(['customer-events'])
            
            # Try to consume for 10 seconds
            start_time = time.time()
            message_found = False
            
            while time.time() - start_time < 10:
                with SuppressStderr():
                    msg = consumer.poll(timeout=1.0)
                    if msg and not msg.error():
                        if msg.key() and msg.key().decode('utf-8') == 'health_check':
                            message_found = True
                            break
            
            consumer.close()
            
            if message_found:
                print("✓ Test message consumed successfully")
                return True
            else:
                print("✗ Test message not consumed")
                return False
                
        except Exception as e:
            print(f"✗ Produce/consume test failed: {e}")
            return False
    
    def run_health_checks(self):
        print("Running Confluent Cloud Health Checks...")
        print("=" * 50)
        
        cluster_healthy = self.check_cluster_health()
        produce_consume_healthy = self.test_produce_consume()
        
        print("=" * 50)
        if cluster_healthy and produce_consume_healthy:
            print("✓ All health checks passed!")
            return True
        else:
            print("✗ Some health checks failed!")
            return False

if __name__ == "__main__":
    checker = HealthChecker()
    checker.run_health_checks()