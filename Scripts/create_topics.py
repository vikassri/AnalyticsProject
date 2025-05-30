from confluent_kafka.admin import AdminClient, NewTopic
from config import CONFLUENT_CONFIG

def create_topics():
    admin_client = AdminClient(CONFLUENT_CONFIG)
    
    topics = [
        NewTopic("customer-events", num_partitions=6, replication_factor=3),
        NewTopic("order-events", num_partitions=6, replication_factor=3),
        NewTopic("inventory-events", num_partitions=3, replication_factor=3),
        NewTopic("user-profiles", num_partitions=6, replication_factor=3),
        NewTopic("recommendations", num_partitions=6, replication_factor=3),
        NewTopic("fraud-alerts", num_partitions=3, replication_factor=3),
        NewTopic("revenue-aggregates", num_partitions=3, replication_factor=3),
        NewTopic("inventory-alerts", num_partitions=3, replication_factor=3)
    ]
    
    futures = admin_client.create_topics(topics)
    
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic {topic} created successfully")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

if __name__ == "__main__":
    create_topics()