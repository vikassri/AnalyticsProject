from confluent_kafka import Producer
from faker import Faker
import uuid,json
import logging
from config import CLOUDERA_KAFKA_BOOTSTRAP

logging.basicConfig(level=logging.INFO)

class client:
    def __init__(self, topic):
        self.topic = topic
        self.producer = Producer(CLOUDERA_KAFKA_BOOTSTRAP)

    def delivery_report(self,err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        
    def _get_msg(self):
        fake = Faker(['en_US'])
        return {
            'event_id': str(uuid.uuid4()),
            'name': fake.name(),
            'company': fake.company(),
            'ssn': fake.ssn(),
            'job': fake.job()
        }
    
    def produce(self,count=1):
        logging.info(f"✅ Producing messages to topic: {self.topic}")
        if not self.topic:
            logging.error("❌ Topic name is not set.")
            return
        logging.info(f"Producing {count} messages to topic: {self.topic}")
        for msg in range(count):
            data = self._get_msg()
            self.producer.produce(self.topic, key=str(uuid.uuid4()), value=json.dumps(data), callback=self.delivery_report)
        self.producer.poll(10)
        self.producer.flush()


kafka = client('messages')
kafka.produce()
