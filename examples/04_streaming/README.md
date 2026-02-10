# Streaming Examples - Organized by Connector

Real-time data streaming examples organized by connector type.

---

## 📁 **Structure**

```
04_streaming/
├── sse/                    # Server-Sent Events (Wikimedia, GitHub, etc.)
├── websocket/              # WebSocket (Coinbase, Binance, etc.)
├── kafka/                  # Apache Kafka, Confluent Cloud, AWS MSK
├── azure_eventgrid/        # Azure Event Grid
├── azure_servicebus/       # Azure Service Bus
├── aws_sqs/                # AWS SQS
└── gcp_pubsub/             # GCP Pub/Sub
```

---

## 🚀 **Quick Start by Connector**

### **1. SSE (Server-Sent Events)**
**Best for:** Wikimedia, GitHub Events, custom SSE streams

```bash
cd 04_streaming/sse
python wikimedia_simple.py
```

**Use cases:**
- Wikipedia real-time edits
- GitHub event streams
- Custom SSE endpoints

---

### **2. WebSocket**
**Best for:** Coinbase, Binance, real-time crypto/trading

```bash
cd 04_streaming/websocket
python coinbase_example.py
```

**Use cases:**
- Cryptocurrency prices
- Trading data
- Real-time dashboards

---

### **3. Kafka**
**Best for:** Enterprise messaging, high-throughput streams

```bash
cd 04_streaming/kafka
python kafka_consumer_example.py
```

**Supports:**
- Apache Kafka
- Confluent Cloud
- AWS MSK
- Azure Event Hubs (Kafka protocol)

---

### **4. Azure Event Grid**
**Best for:** Event-driven architecture, Azure system events

```bash
cd 04_streaming/azure_eventgrid
python eventgrid_example.py
```

**Use cases:**
- Azure Storage events
- IoT Hub events
- Custom event topics

---

### **5. Azure Service Bus**
**Best for:** Queue and topic/subscription messaging

```bash
cd 04_streaming/azure_servicebus
python servicebus_queue_example.py
```

**Modes:**
- Queue (point-to-point)
- Topic/Subscription (pub/sub)

---

### **6. AWS SQS**
**Best for:** Simple Queue Service, FIFO queues

```bash
cd 04_streaming/aws_sqs
python sqs_example.py
```

**Supports:**
- Standard queues
- FIFO queues

---

### **7. GCP Pub/Sub**
**Best for:** Google Cloud messaging

```bash
cd 04_streaming/gcp_pubsub
python pubsub_example.py
```

**Use cases:**
- Event ingestion
- Asynchronous workflows
- Stream analytics

---

## 📦 **Installation**

### **All Streaming Connectors**
```bash
pip install "lakelogic[streaming]"
```

### **Individual Connectors**
```bash
# SSE (Wikimedia, etc.)
pip install "lakelogic[sse]"

# WebSocket (Coinbase, etc.)
pip install "lakelogic[websocket]"

# Kafka
pip install "lakelogic[kafka]"

# Azure (Event Grid + Service Bus)
pip install "lakelogic[azure_messaging]"

# AWS SQS
pip install "lakelogic[aws_messaging]"

# GCP Pub/Sub
pip install "lakelogic[gcp_messaging]"
```

---

## 🎯 **Choosing a Connector**

| Use Case | Recommended Connector |
|----------|----------------------|
| **Wikipedia edits** | SSE (Wikimedia) |
| **Crypto prices** | WebSocket (Coinbase/Binance) |
| **Enterprise messaging** | Kafka |
| **Azure events** | Azure Event Grid |
| **Azure queues** | Azure Service Bus |
| **AWS queues** | AWS SQS |
| **GCP messaging** | GCP Pub/Sub |

---

## 📚 **Documentation**

- **Connector API:** `lakelogic/engines/streaming_connectors.py`
- **Streaming Processor:** `lakelogic/core/streaming_processor.py`
- **Test Providers:** `docs/streaming_test_providers.md`
- **Implementation Guide:** `docs/streaming_implementation_complete.md`

---

## 🔐 **Authentication**

All cloud connectors use automatic authentication:

| Connector | Authentication |
|-----------|---------------|
| **Azure Event Grid** | DefaultAzureCredential (Azure AD) |
| **Azure Service Bus** | DefaultAzureCredential (Azure AD) |
| **AWS SQS** | IAM roles (boto3 automatic) |
| **GCP Pub/Sub** | Application Default Credentials |
| **Kafka** | Configurable (SASL, SSL, etc.) |
| **SSE** | Optional headers |
| **WebSocket** | Optional headers |

---

## 🚧 **Coming Soon**

- ❌ Contract-driven streaming (all connectors)
- ❌ Delta Lake integration (Bronze + Realtime layers)
- ❌ Data quality validation (streaming mode)
- ❌ Retention management
- ❌ Monitoring & observability

---

*Last Updated: February 9, 2026*
