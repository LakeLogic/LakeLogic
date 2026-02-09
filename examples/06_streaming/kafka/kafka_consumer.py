"""
Kafka Consumer Example

Simple example of consuming messages from Apache Kafka.

Prerequisites:
    pip install "lakelogic[kafka]"

Setup:
    1. Start Kafka locally (or use Confluent Cloud)
    2. Create a topic: kafka-topics --create --topic orders --bootstrap-server localhost:9092
    3. Produce some messages: kafka-console-producer --topic orders --bootstrap-server localhost:9092

Run:
    python kafka_consumer.py

Supports:
- Apache Kafka
- Confluent Cloud
- AWS MSK
- Azure Event Hubs (Kafka protocol)
"""

from lakelogic.engines.streaming_connectors import KafkaConnector

def main():
    """
    Consume messages from Kafka.
    """
    
    print("=" * 80)
    print("Kafka Consumer Example")
    print("=" * 80)
    print()
    
    # Configure Kafka connection
    BROKERS = ["localhost:9092"]  # Change to your Kafka brokers
    TOPIC = "orders"               # Change to your topic
    CONSUMER_GROUP = "lakelogic"
    
    print(f"Connecting to Kafka...")
    print(f"  Brokers: {BROKERS}")
    print(f"  Topic: {TOPIC}")
    print(f"  Consumer Group: {CONSUMER_GROUP}")
    print()
    
    # Create Kafka connector
    connector = KafkaConnector(
        brokers=BROKERS,
        topic=TOPIC,
        consumer_group=CONSUMER_GROUP,
        auto_offset_reset="earliest"  # Start from beginning
    )
    
    # Stream messages
    message_count = 0
    
    try:
        print("📨 Consuming messages...")
        print("Press Ctrl+C to stop")
        print()
        
        for message in connector.stream():
            message_count += 1
            
            print(f"[{message_count:04d}] {message}")
            
            # Stop after 100 messages (for demo)
            if message_count >= 100:
                print()
                print(f"✅ Processed {message_count} messages")
                break
    
    except KeyboardInterrupt:
        print()
        print(f"✅ Stopped after {message_count} messages")
    
    finally:
        connector.close()


if __name__ == "__main__":
    main()
