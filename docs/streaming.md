# Real-Time Streaming 🌊

LakeLogic provides a unified API for streaming data through the same contract-driven processing engine used for batch. This allows for seamless transitions between batch ELT and real-time streaming pipelines.

## 🚀 Overview

LakeLogic supports multiple streaming connectors including SSE, WebSocket, and major cloud messaging services.

| Connector | Support | Typical Use Cases |
|-----------|---------|-------------------|
| **SSE** | Native | Wikimedia, GitHub Events, Custom SSE streams |
| **WebSocket** | Native | Financial tickers (Coinbase, Binance), Real-time feeds |
| **Kafka** | Native | Apache Kafka, Confluent, AWS MSK, Azure Event Hubs |
| **Azure** | Native | Event Grid, Service Bus |
| **AWS** | Native | SQS (Simple Queue Service) |
| **GCP** | Native | Pub/Sub messaging |

## 📦 Installation

Streaming support is available as an extra:

```bash
# Install all streaming features
pip install "lakelogic[streaming]"

# Or install specific extras
pip install "lakelogic[kafka]"
pip install "lakelogic[sse]"
pip install "lakelogic[websocket]"
```

## 🔌 Connector Examples

All LakeLogic connectors follow a unified iterator-based API for clean, readable pipelines.

### SSE (Server-Sent Events)
```python
from lakelogic.engines.streaming_connectors import SSEConnector

# Connect to a public event stream
connector = SSEConnector("https://stream.wikimedia.org/v2/stream/recentchange")

for event in connector.stream():
    print(f"{event['type']} | {event['title']}")
```

### WebSocket (Financial Data)
```python
from lakelogic.engines.streaming_connectors import WebSocketConnector

connector = WebSocketConnector(
    url="wss://ws-feed.exchange.coinbase.com",
    subscribe_message={
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]
    }
)

for event in connector.stream():
    print(f"BTC Price: ${event.get('price')}")
```

### Apache Kafka
```python
from lakelogic.engines.streaming_connectors import KafkaConnector

connector = KafkaConnector(
    brokers=["localhost:9092"],
    topic="production_orders",
    consumer_group="lakelogic_processor"
)

for event in connector.stream():
    # LakeLogic automatically parses JSON messages
    print(event)
```

## 🔐 Authentication

Cloud connectors leverage automatic credential resolution, meaning most production environments require zero configuration:

| Provider | Authentication Method |
|----------|----------------------|
| **Azure** | `DefaultAzureCredential` (Azure AD) |
| **AWS** | IAM Roles (boto3 automatic) |
| **GCP** | Application Default Credentials (ADC) |

## 🧪 Testing with Live Data

For a list of public datasets you can use to test your streaming pipelines, see the [Public Streaming Sources](streaming_sources.md) guide.
