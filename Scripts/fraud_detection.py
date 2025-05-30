# fraud_detection.py
from confluent_kafka import Consumer, Producer
from config import CONFLUENT_CONFIG
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta

class FraudDetectionEngine:
    def __init__(self):
        consumer_config = CONFLUENT_CONFIG.copy()
        consumer_config.update({
            'group.id': 'fraud-detection',
            'auto.offset.reset': 'latest'
        })
        
        self.consumer = Consumer(consumer_config)
        self.producer = Producer(CONFLUENT_CONFIG)
        
        # In-memory storage for fraud detection
        self.user_order_history = defaultdict(list)
        self.ip_order_history = defaultdict(list)
        
    def process_order_events(self):
        self.consumer.subscribe(['order-events'])
        
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
                if event['event_type'] == 'created':
                    fraud_score = self.calculate_fraud_score(event)
                    
                    if fraud_score > 0.7:  # High fraud probability
                        self.send_fraud_alert(event, fraud_score)
                
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
    
    def calculate_fraud_score(self, order):
        score = 0.0
        reasons = []
        
        user_id = order['user_id']
        order_amount = order['total_amount']
        order_time = datetime.fromtimestamp(order['timestamp'] / 1000)
        
        # Check 1: High order amount
        if order_amount > 1000:
            score += 0.3
            reasons.append("high_order_amount")
        
        # Check 2: Multiple orders in short time
        user_orders = self.user_order_history[user_id]
        recent_orders = [
            o for o in user_orders 
            if (order_time - datetime.fromtimestamp(o['timestamp'] / 1000)).seconds < 300
        ]
        
        if len(recent_orders) > 2:
            score += 0.4
            reasons.append("multiple_recent_orders")
        
        # Check 3: Unusual order time (late night/early morning)
        if order_time.hour < 6 or order_time.hour > 23:
            score += 0.2
            reasons.append("unusual_order_time")
        
        # Check 4: First-time high-value customer
        if len(user_orders) == 0 and order_amount > 500:
            score += 0.3
            reasons.append("first_time_high_value")
        
        # Update history
        self.user_order_history[user_id].append(order)
        
        return min(score, 1.0), reasons
    
    def send_fraud_alert(self, order, fraud_info):
        fraud_score, reasons = fraud_info
        
        alert = {
            'order_id': order['order_id'],
            'user_id': order['user_id'],
            'fraud_score': fraud_score,
            'reasons': reasons,
            'order_amount': order['total_amount'],
            'timestamp': int(time.time() * 1000),
            'alert_type': 'FRAUD_DETECTION'
        }
        
        self.producer.produce(
            topic='fraud-alerts',
            key=order['order_id'],
            value=json.dumps(alert).encode('utf-8')
        )
        self.producer.flush()
        
        print(f"FRAUD ALERT: Order {order['order_id']} - Score: {fraud_score}")

if __name__ == "__main__":
    detector = FraudDetectionEngine()
    detector.process_order_events()