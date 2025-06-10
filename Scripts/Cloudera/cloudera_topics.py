from confluent_kafka.admin import AdminClient, NewTopic
from config import CLOUDERA_KAFKA_BOOTSTRAP
import os

admin_client = AdminClient(
    conf=CLOUDERA_KAFKA_BOOTSTRAP
)

def create_topics():
    with open( os.path.dirname(os.path.abspath(__file__)) + "/../output/topics_details.txt", "r") as f:
        lines = f.readlines()
    topics = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("|")
        if len(parts) != 3:
            print(f"⚠️ Invalid topic definition: {line}")
            continue

        topic_name, partition_count, replication_factor = parts
        try:
            partition_count = int(partition_count)
            #replication_factor = int(replication_factor)
            replication_factor = 1
            #print(f"Creating topic: {topic_name} with Partitions: {partition_count} and Replication Factor: {replication_factor}")
            topics.append(NewTopic(topic=topic_name, num_partitions=partition_count, replication_factor=replication_factor))
        except ValueError as e:
            print(f"⚠️ Error parsing topic definition '{line}': {e}")
            continue
        
    try:
        future = admin_client.create_topics(new_topics=topics, validate_only=True)
        for topic_name, future in future.items():
            try:
                print(f"✅ Topic {topic_name} created successfully")
            except Exception as e:
                print(f"❌ Failed to create topic {topic_name}: {e}")
    except Exception as e:
        print(f"❌ Failed to create topics: {e}")

def extract_topic_details():
    metadata = admin_client.list_topics(timeout=10)
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
        if not topic_name.startswith("_") :
            print(f"{topic_name}|{partition_count}|{replication_factor_str}")

if __name__ == "__main__":
    #create_topics()
    extract_topic_details()