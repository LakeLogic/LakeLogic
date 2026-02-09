"""
Streaming connectors for real-time data ingestion.

This module provides connectors for various streaming protocols:
- Server-Sent Events (SSE) - Wikimedia, GitHub, etc.
- WebSocket - Coinbase, Binance, etc.
- Apache Kafka - Enterprise messaging, Confluent Cloud, AWS MSK
- Azure Event Grid - Event-driven architecture
- Azure Service Bus - Queue and topic/subscription messaging
- AWS SQS - Simple Queue Service
- GCP Pub/Sub - Google Cloud messaging
"""

import json
from typing import Optional, Dict, Any, Iterator, Callable
from loguru import logger


class SSEConnector:
    """
    Server-Sent Events (SSE) connector.
    
    Perfect for:
    - Wikimedia Recent Changes
    - GitHub Events
    - Any SSE-based stream
    
    Example:
        >>> connector = SSEConnector("https://stream.wikimedia.org/v2/stream/recentchange")
        >>> for event in connector.stream():
        ...     print(event['title'])
    """
    
    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        retry: bool = True
    ):
        """
        Initialize SSE connector.
        
        Args:
            url: SSE stream URL
            headers: Optional HTTP headers
            retry: Automatically retry on connection failure
        """
        self.url = url
        self.headers = headers or {}
        self.retry = retry
        self._client = None
    
    def stream(self) -> Iterator[Dict[str, Any]]:
        """
        Stream events from SSE endpoint.
        
        Yields:
            Parsed event dictionaries
        
        Example:
            >>> for event in connector.stream():
            ...     print(event)
        """
        try:
            import sseclient
            import requests
        except ImportError:
            raise ImportError(
                "SSE support not installed. Install with: pip install 'lakelogic[sse]'"
            )
        
        logger.info(f"Connecting to SSE stream: {self.url}")
        
        while True:
            try:
                response = requests.get(
                    self.url,
                    stream=True,
                    headers=self.headers
                )
                response.raise_for_status()
                
                client = sseclient.SSEClient(response)
                
                logger.info("✅ Connected to SSE stream")
                
                for event in client.events():
                    # Skip comment lines
                    if not event.data or event.data.startswith(':'):
                        continue
                    
                    try:
                        # Parse JSON event
                        data = json.loads(event.data)
                        yield data
                    
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse event: {e}")
                        continue
            
            except Exception as e:
                logger.error(f"SSE connection error: {e}")
                
                if not self.retry:
                    raise
                
                logger.info("Retrying in 5 seconds...")
                import time
                time.sleep(5)
    
    def close(self):
        """Close SSE connection."""
        if self._client:
            self._client.close()
        logger.info("SSE connection closed")


class WebSocketConnector:
    """
    WebSocket connector.
    
    Perfect for:
    - Coinbase (crypto prices)
    - Binance (crypto trading)
    - Custom WebSocket APIs
    
    Example:
        >>> connector = WebSocketConnector(
        ...     url="wss://ws-feed.exchange.coinbase.com",
        ...     subscribe_message={
        ...         "type": "subscribe",
        ...         "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]
        ...     }
        ... )
        >>> for event in connector.stream():
        ...     print(event['price'])
    """
    
    def __init__(
        self,
        url: str,
        subscribe_message: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        retry: bool = True
    ):
        """
        Initialize WebSocket connector.
        
        Args:
            url: WebSocket URL
            subscribe_message: Optional subscription message to send on connect
            headers: Optional HTTP headers
            retry: Automatically retry on connection failure
        """
        self.url = url
        self.subscribe_message = subscribe_message
        self.headers = headers or {}
        self.retry = retry
        self._ws = None
        self._messages = []
    
    def stream(self) -> Iterator[Dict[str, Any]]:
        """
        Stream events from WebSocket.
        
        Yields:
            Parsed event dictionaries
        
        Example:
            >>> for event in connector.stream():
            ...     print(event)
        """
        try:
            import websocket
        except ImportError:
            raise ImportError(
                "WebSocket support not installed. Install with: pip install 'lakelogic[websocket]'"
            )
        
        logger.info(f"Connecting to WebSocket: {self.url}")
        
        def on_message(ws, message):
            """Handle incoming message."""
            try:
                data = json.loads(message)
                self._messages.append(data)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse message: {e}")
        
        def on_error(ws, error):
            """Handle error."""
            logger.error(f"WebSocket error: {error}")
        
        def on_close(ws, close_status_code, close_msg):
            """Handle close."""
            logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")
        
        def on_open(ws):
            """Handle open."""
            logger.info("✅ Connected to WebSocket")
            
            # Send subscribe message if provided
            if self.subscribe_message:
                ws.send(json.dumps(self.subscribe_message))
                logger.info(f"Sent subscribe message: {self.subscribe_message}")
        
        # Create WebSocket connection
        self._ws = websocket.WebSocketApp(
            self.url,
            header=self.headers,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        
        # Run in background thread
        import threading
        ws_thread = threading.Thread(target=self._ws.run_forever)
        ws_thread.daemon = True
        ws_thread.start()
        
        # Yield messages as they arrive
        import time
        while True:
            if self._messages:
                yield self._messages.pop(0)
            else:
                time.sleep(0.01)  # Small sleep to avoid busy-waiting
    
    def close(self):
        """Close WebSocket connection."""
        if self._ws:
            self._ws.close()
        logger.info("WebSocket connection closed")


class KafkaConnector:
    """
    Apache Kafka connector.
    
    Supports:
    - Apache Kafka
    - Confluent Cloud
    - Azure Event Hubs (Kafka protocol)
    - AWS MSK
    
    Example:
        >>> connector = KafkaConnector(
        ...     brokers=["localhost:9092"],
        ...     topic="orders",
        ...     consumer_group="lakelogic"
        ... )
        >>> for event in connector.stream():
        ...     print(event)
    """
    
    def __init__(
        self,
        brokers: list,
        topic: str,
        consumer_group: str,
        auto_offset_reset: str = "earliest",
        **kafka_config
    ):
        """
        Initialize Kafka connector.
        
        Args:
            brokers: List of Kafka brokers (e.g., ["localhost:9092"])
            topic: Kafka topic to consume
            consumer_group: Consumer group ID
            auto_offset_reset: "earliest" or "latest"
            **kafka_config: Additional Kafka consumer configuration
        """
        self.brokers = brokers
        self.topic = topic
        self.consumer_group = consumer_group
        self.auto_offset_reset = auto_offset_reset
        self.kafka_config = kafka_config
        self._consumer = None
    
    def stream(self) -> Iterator[Dict[str, Any]]:
        """
        Stream events from Kafka.
        
        Yields:
            Parsed event dictionaries
        """
        try:
            from kafka import KafkaConsumer
        except ImportError:
            raise ImportError(
                "Kafka support not installed. Install with: pip install kafka-python"
            )
        
        logger.info(f"Connecting to Kafka: {self.brokers}")
        logger.info(f"Topic: {self.topic}, Consumer Group: {self.consumer_group}")
        
        # Create Kafka consumer
        self._consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.brokers,
            group_id=self.consumer_group,
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            **self.kafka_config
        )
        
        logger.info("✅ Connected to Kafka")
        
        # Stream messages
        for message in self._consumer:
            yield message.value
    
    def close(self):
        """Close Kafka consumer."""
        if self._consumer:
            self._consumer.close()
        logger.info("Kafka consumer closed")


class AzureEventGridConnector:
    """
    Azure Event Grid connector.
    
    Supports:
    - Event Grid topics
    - Event Grid domains
    - System topics (Storage, IoT Hub, etc.)
    
    Uses automatic Azure AD authentication via CloudCredentialResolver.
    
    Example:
        >>> connector = AzureEventGridConnector(
        ...     endpoint="https://my-topic.westus2-1.eventgrid.azure.net/api/events",
        ...     subscription_name="my-subscription"
        ... )
        >>> for event in connector.stream():
        ...     print(event)
    """
    
    def __init__(
        self,
        endpoint: str,
        subscription_name: str,
        max_events: int = 10,
        wait_time_seconds: int = 60
    ):
        """
        Initialize Azure Event Grid connector.
        
        Args:
            endpoint: Event Grid topic endpoint
            subscription_name: Event subscription name
            max_events: Maximum events to receive per request
            wait_time_seconds: Wait time for long polling
        """
        self.endpoint = endpoint
        self.subscription_name = subscription_name
        self.max_events = max_events
        self.wait_time_seconds = wait_time_seconds
        self._client = None
    
    def stream(self) -> Iterator[Dict[str, Any]]:
        """
        Stream events from Azure Event Grid.
        
        Yields:
            Parsed event dictionaries
        """
        try:
            from azure.eventgrid import EventGridConsumerClient
            from azure.identity import DefaultAzureCredential
        except ImportError:
            raise ImportError(
                "Azure Event Grid support not installed. "
                "Install with: pip install 'lakelogic[azure_messaging]'"
            )
        
        logger.info(f"Connecting to Azure Event Grid: {self.endpoint}")
        
        # Create Event Grid client with automatic Azure AD auth
        credential = DefaultAzureCredential()
        self._client = EventGridConsumerClient(
            endpoint=self.endpoint,
            credential=credential
        )
        
        logger.info("✅ Connected to Azure Event Grid")
        
        # Stream events (long polling)
        while True:
            try:
                events = self._client.receive(
                    subscription_name=self.subscription_name,
                    max_events=self.max_events,
                    max_wait_time=self.wait_time_seconds
                )
                
                for event in events:
                    yield event.data
            
            except Exception as e:
                logger.error(f"Event Grid error: {e}")
                import time
                time.sleep(5)
    
    def close(self):
        """Close Event Grid client."""
        if self._client:
            self._client.close()
        logger.info("Event Grid client closed")


class AzureServiceBusConnector:
    """
    Azure Service Bus connector (streaming mode).
    
    Supports:
    - Service Bus queues
    - Service Bus topics/subscriptions
    
    Uses automatic Azure AD authentication via CloudCredentialResolver.
    
    Example:
        >>> connector = AzureServiceBusConnector(
        ...     namespace="my-namespace.servicebus.windows.net",
        ...     queue_name="orders"
        ... )
        >>> for event in connector.stream():
        ...     print(event)
    """
    
    def __init__(
        self,
        namespace: str,
        queue_name: Optional[str] = None,
        topic_name: Optional[str] = None,
        subscription_name: Optional[str] = None,
        max_wait_time: int = 60
    ):
        """
        Initialize Azure Service Bus connector.
        
        Args:
            namespace: Service Bus namespace (e.g., "my-namespace.servicebus.windows.net")
            queue_name: Queue name (for queue mode)
            topic_name: Topic name (for topic/subscription mode)
            subscription_name: Subscription name (for topic/subscription mode)
            max_wait_time: Maximum wait time for receiving messages (seconds)
        """
        self.namespace = namespace
        self.queue_name = queue_name
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.max_wait_time = max_wait_time
        self._receiver = None
    
    def stream(self) -> Iterator[Dict[str, Any]]:
        """
        Stream events from Azure Service Bus.
        
        Yields:
            Parsed event dictionaries
        """
        try:
            from azure.servicebus import ServiceBusClient
            from azure.identity import DefaultAzureCredential
        except ImportError:
            raise ImportError(
                "Azure Service Bus support not installed. "
                "Install with: pip install 'lakelogic[azure_messaging]'"
            )
        
        logger.info(f"Connecting to Azure Service Bus: {self.namespace}")
        
        # Create Service Bus client with automatic Azure AD auth
        credential = DefaultAzureCredential()
        client = ServiceBusClient(
            fully_qualified_namespace=self.namespace,
            credential=credential
        )
        
        # Create receiver (queue or topic/subscription)
        if self.queue_name:
            self._receiver = client.get_queue_receiver(queue_name=self.queue_name)
            logger.info(f"✅ Connected to queue: {self.queue_name}")
        elif self.topic_name and self.subscription_name:
            self._receiver = client.get_subscription_receiver(
                topic_name=self.topic_name,
                subscription_name=self.subscription_name
            )
            logger.info(f"✅ Connected to topic/subscription: {self.topic_name}/{self.subscription_name}")
        else:
            raise ValueError("Must specify either queue_name or (topic_name + subscription_name)")
        
        # Stream messages
        with self._receiver:
            while True:
                messages = self._receiver.receive_messages(
                    max_wait_time=self.max_wait_time,
                    max_message_count=10
                )
                
                for message in messages:
                    try:
                        # Parse message body
                        body = str(message)
                        data = json.loads(body)
                        
                        # Complete message (remove from queue)
                        self._receiver.complete_message(message)
                        
                        yield data
                    
                    except json.JSONDecodeError:
                        # If not JSON, yield as string
                        self._receiver.complete_message(message)
                        yield {"body": str(message)}
                    
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        # Abandon message (return to queue)
                        self._receiver.abandon_message(message)
    
    def close(self):
        """Close Service Bus receiver."""
        if self._receiver:
            self._receiver.close()
        logger.info("Service Bus receiver closed")


class AWSSQSConnector:
    """
    AWS SQS connector.
    
    Supports:
    - Standard queues
    - FIFO queues
    
    Uses automatic AWS IAM authentication via CloudCredentialResolver.
    
    Example:
        >>> connector = AWSSQSConnector(
        ...     queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
        ...     region="us-east-1"
        ... )
        >>> for event in connector.stream():
        ...     print(event)
    """
    
    def __init__(
        self,
        queue_url: str,
        region: Optional[str] = None,
        max_messages: int = 10,
        wait_time_seconds: int = 20
    ):
        """
        Initialize AWS SQS connector.
        
        Args:
            queue_url: SQS queue URL
            region: AWS region (auto-detected if not provided)
            max_messages: Maximum messages to receive per request (1-10)
            wait_time_seconds: Long polling wait time (0-20 seconds)
        """
        self.queue_url = queue_url
        self.region = region
        self.max_messages = max_messages
        self.wait_time_seconds = wait_time_seconds
        self._client = None
    
    def stream(self) -> Iterator[Dict[str, Any]]:
        """
        Stream events from AWS SQS.
        
        Yields:
            Parsed event dictionaries
        """
        try:
            import boto3
        except ImportError:
            raise ImportError(
                "AWS support not installed. Install with: pip install 'lakelogic[aws_messaging]'"
            )
        
        logger.info(f"Connecting to AWS SQS: {self.queue_url}")
        
        # Create SQS client (uses automatic IAM auth)
        self._client = boto3.client('sqs', region_name=self.region)
        
        logger.info("✅ Connected to AWS SQS")
        
        # Stream messages (long polling)
        while True:
            try:
                response = self._client.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=self.max_messages,
                    WaitTimeSeconds=self.wait_time_seconds
                )
                
                messages = response.get('Messages', [])
                
                for message in messages:
                    try:
                        # Parse message body
                        body = message['Body']
                        data = json.loads(body)
                        
                        # Delete message from queue
                        self._client.delete_message(
                            QueueUrl=self.queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        
                        yield data
                    
                    except json.JSONDecodeError:
                        # If not JSON, yield as string
                        self._client.delete_message(
                            QueueUrl=self.queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        yield {"body": message['Body']}
                    
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
            
            except Exception as e:
                logger.error(f"SQS error: {e}")
                import time
                time.sleep(5)
    
    def close(self):
        """Close SQS client."""
        logger.info("SQS client closed")


class GCPPubSubConnector:
    """
    Google Cloud Pub/Sub connector.
    
    Supports:
    - Pub/Sub topics
    - Pub/Sub subscriptions
    
    Uses automatic Application Default Credentials (ADC).
    
    Example:
        >>> connector = GCPPubSubConnector(
        ...     project_id="my-project",
        ...     subscription_id="my-subscription"
        ... )
        >>> for event in connector.stream():
        ...     print(event)
    """
    
    def __init__(
        self,
        project_id: str,
        subscription_id: str,
        max_messages: int = 10
    ):
        """
        Initialize GCP Pub/Sub connector.
        
        Args:
            project_id: GCP project ID
            subscription_id: Pub/Sub subscription ID
            max_messages: Maximum messages to receive per request
        """
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.max_messages = max_messages
        self._subscriber = None
    
    def stream(self) -> Iterator[Dict[str, Any]]:
        """
        Stream events from GCP Pub/Sub.
        
        Yields:
            Parsed event dictionaries
        """
        try:
            from google.cloud import pubsub_v1
        except ImportError:
            raise ImportError(
                "GCP Pub/Sub support not installed. "
                "Install with: pip install 'lakelogic[gcp_messaging]'"
            )
        
        logger.info(f"Connecting to GCP Pub/Sub: {self.project_id}/{self.subscription_id}")
        
        # Create Pub/Sub subscriber (uses automatic ADC)
        self._subscriber = pubsub_v1.SubscriberClient()
        subscription_path = self._subscriber.subscription_path(
            self.project_id,
            self.subscription_id
        )
        
        logger.info("✅ Connected to GCP Pub/Sub")
        
        # Stream messages (pull-based)
        while True:
            try:
                response = self._subscriber.pull(
                    request={
                        "subscription": subscription_path,
                        "max_messages": self.max_messages
                    }
                )
                
                for received_message in response.received_messages:
                    try:
                        # Parse message data
                        data = json.loads(received_message.message.data.decode('utf-8'))
                        
                        # Acknowledge message
                        self._subscriber.acknowledge(
                            request={
                                "subscription": subscription_path,
                                "ack_ids": [received_message.ack_id]
                            }
                        )
                        
                        yield data
                    
                    except json.JSONDecodeError:
                        # If not JSON, yield as string
                        self._subscriber.acknowledge(
                            request={
                                "subscription": subscription_path,
                                "ack_ids": [received_message.ack_id]
                            }
                        )
                        yield {"body": received_message.message.data.decode('utf-8')}
                    
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
            
            except Exception as e:
                logger.error(f"Pub/Sub error: {e}")
                import time
                time.sleep(5)
    
    def close(self):
        """Close Pub/Sub subscriber."""
        if self._subscriber:
            self._subscriber.close()
        logger.info("Pub/Sub subscriber closed")
