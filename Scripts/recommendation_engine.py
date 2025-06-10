# recommendation_engine.py
from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from AnalyticsProject.Scripts.Confluent.schema_client import SchemaManager
from AnalyticsProject.Scripts.Confluent.config import CONFLUENT_CONFIG
import json
import time
from collections import defaultdict, Counter

class RecommendationEngine:
    def __init__(self):
        consumer_config = CONFLUENT_CONFIG.copy()
        consumer_config.update({
            'group.id': 'recommendation-engine',
            'auto.offset.reset': 'latest'
        })
        
        self.consumer = Consumer(consumer_config)
        self.producer = Producer(CONFLUENT_CONFIG)
        self.schema_manager = SchemaManager()
        
        # In-memory storage for recommendations
        self.user_interactions = defaultdict(list)
        self.product_cooccurrence = defaultdict(Counter)
        
    def process_customer_events(self):
        self.consumer.subscribe(['customer-events'])
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                # Process the event
                event = json.loads(msg.value().decode('utf-8'))
                self.update_recommendations(event)
                
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
    
    def update_recommendations(self, event):
        user_id = event['user_id']
        
        if event['event_type'] in ['product_view', 'add_to_cart']:
            product_id = event['product_id']
            
            # Track user interactions
            self.user_interactions[user_id].append(product_id)
            
            # Update product co-occurrence matrix
            user_products = self.user_interactions[user_id]
            if len(user_products) > 1:
                for other_product in user_products[:-1]:
                    if other_product != product_id:
                        self.product_cooccurrence[product_id][other_product] += 1
                        self.product_cooccurrence[other_product][product_id] += 1
            
            # Generate recommendations
            recommendations = self.generate_recommendations(user_id, product_id)
            
            if recommendations:
                self.send_recommendations(user_id, recommendations)
    
    def generate_recommendations(self, user_id, current_product):
        # Simple collaborative filtering
        similar_products = self.product_cooccurrence[current_product]
        
        # Get top 5 recommendations
        recommendations = []
        for product, score in similar_products.most_common(5):
            recommendations.append({
                'product_id': product,
                'score': score,
                'reason': 'collaborative_filtering'
            })
        
        return recommendations
    
    def send_recommendations(self, user_id, recommendations):
        recommendation_event = {
            'user_id': user_id,
            'timestamp': int(time.time() * 1000),
            'recommendations': recommendations,
            'algorithm': 'collaborative_filtering'
        }
        
        self.producer.produce(
            topic='recommendations',
            key=user_id,
            value=json.dumps(recommendation_event).encode('utf-8')
        )
        self.producer.flush()

if __name__ == "__main__":
    engine = RecommendationEngine()
    engine.process_customer_events()