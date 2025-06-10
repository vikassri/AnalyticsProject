from confluent_kafka.admin import AdminClient, NewTopic
from config import CONFLUENT_CONFIG
import os


admin_client = AdminClient(CONFLUENT_CONFIG)

def create_topics():
    
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

def extract_topic_details():
    metadata = admin_client.list_topics(timeout=10)
    
    f = open(os.path.dirname(os.path.abspath(__file__)) + "/../output/topics_details.txt", "w")
    for topic_name, topic_metadata in metadata.topics.items():
        if topic_metadata.error is not None:
            print(f"⚠️ Error retrieving metadata for {topic_name}: {topic_metadata.error}")
            continue

        partition_count = len(topic_metadata.partitions)
        # Collect replication factors from each partition (they should be the same, but we check)
        replication_factors = set(len(p.replicas) for p in topic_metadata.partitions.values())

        replication_factor_str = (
            str(replication_factors.pop()) if len(replication_factors) == 1
            else f"Mixed ({', '.join(map(str, replication_factors))})"
        )
        if not topic_name.startswith("_") and not topic_name.startswith("pksql"):
            f.write(f"{topic_name}|{partition_count}|{replication_factor_str}\n")

    f.close()
        
    
if __name__ == "__main__":
    extract_topic_details()