# e2e_test.py
import time
import json
from customer_events_producer import CustomerEventsProducer
from order_events_producer import OrderEventsProducer
from test_consumer import TestConsumer

def run_e2e_test():
    print("Starting End-to-End Test...")
    
    # Step 1: Produce test data
    print("1. Producing customer events...")
    customer_producer = CustomerEventsProducer()
    customer_producer.produce_events(10)
    
    print("2. Producing order events...")
    order_producer = OrderEventsProducer()
    order_producer.produce_events(5)
    
    # Step 2: Wait for processing
    print("3. Waiting for stream processing...")
    time.sleep(30)
    
    # Step 3: Verify outputs
    print("4. Verifying recommendations topic...")
    test_consumer = TestConsumer(['recommendations'])
    test_consumer.consume_messages(10)
    
    print("5. Verifying fraud alerts topic...")
    test_consumer = TestConsumer(['fraud-alerts'])
    test_consumer.consume_messages(10)
    
    print("End-to-End Test Completed!")

if __name__ == "__main__":
    run_e2e_test()