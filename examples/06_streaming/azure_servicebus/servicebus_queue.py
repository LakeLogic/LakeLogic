"""
Azure Service Bus Queue Example

Simple example of consuming messages from Azure Service Bus queue.

Prerequisites:
    pip install "lakelogic[azure_messaging]"
    
    Azure authentication (one of):
    - az login (Azure CLI)
    - Managed Identity (if running in Azure)
    - Environment variables (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)

Setup:
    1. Create Service Bus namespace in Azure Portal
    2. Create a queue (e.g., "orders")
    3. Send some test messages

Run:
    python servicebus_queue.py

What it does:
- Connects to Azure Service Bus queue
- Receives messages
- Automatically completes (deletes) messages
"""

from lakelogic.engines.streaming_connectors import AzureServiceBusConnector

def main():
    """
    Consume messages from Azure Service Bus queue.
    """
    
    print("=" * 80)
    print("Azure Service Bus Queue Consumer")
    print("=" * 80)
    print()
    
    # Configure Service Bus connection
    NAMESPACE = "my-namespace.servicebus.windows.net"  # Change to your namespace
    QUEUE_NAME = "orders"                               # Change to your queue
    
    print(f"Connecting to Azure Service Bus...")
    print(f"  Namespace: {NAMESPACE}")
    print(f"  Queue: {QUEUE_NAME}")
    print()
    
    # Create Service Bus connector
    connector = AzureServiceBusConnector(
        namespace=NAMESPACE,
        queue_name=QUEUE_NAME,
        max_wait_time=60  # Wait up to 60 seconds for messages
    )
    
    # Stream messages
    message_count = 0
    
    try:
        print("📨 Receiving messages...")
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
