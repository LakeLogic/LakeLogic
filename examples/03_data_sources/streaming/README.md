# 🌊 Streaming Data Sources

Real-time data ingestion examples for LakeLogic.

## 🎯 Overview

Modern businesses need to process data **as it arrives**, not hours later. This directory contains production-ready examples for connecting LakeLogic to various streaming platforms.

## 📂 Available Connectors

| Connector | Use Case | Example |
|-----------|----------|---------|
| **SSE (Server-Sent Events)** | Wikimedia changes, GitHub events | [sse_wikimedia/](sse_wikimedia/) |
| **WebSocket** | Crypto prices (Coinbase, Binance) | [websocket_crypto/](websocket_crypto/) |
| **Kafka** | Enterprise messaging, event logs | [kafka/](kafka/) |
| **Webhook** | GitHub, Stripe, Twilio push events | [webhook/](webhook/) |
| **Azure Event Grid** | Azure Storage events, IoT Hub | [azure_event_grid/](azure_event_grid/) |
| **Azure Service Bus** | Queue & topic messaging | [azure_service_bus/](azure_service_bus/) |
| **AWS SQS** | Simple Queue Service | [aws_sqs/](aws_sqs/) |
| **GCP Pub/Sub** | Google Cloud messaging | [gcp_pubsub/](gcp_pubsub/) |

## 🚀 Quick Start

Each example follows the same pattern:

1. **Define a contract** (`*_contract.yaml`)
2. **Run the streaming processor** (via notebook or Python script)
3. **See real-time validation** and quality gates in action

## 💡 Why Streaming with LakeLogic?

Unlike traditional message queues, LakeLogic provides:

- ✅ **Built-in Quality Gates**: Invalid events are quarantined before they corrupt your lakehouse
- ✅ **Automatic Schema Evolution**: New fields are handled gracefully
- ✅ **Unified Interface**: Same contract structure across all platforms
- ✅ **Audit Trail**: Every event is logged with lineage metadata

## 🎓 Learning Path

1. Start with **Webhook** (simplest - just HTTP POST)
2. Move to **SSE** (one-way server push)
3. Try **Kafka** (enterprise-grade messaging)
4. Explore cloud-native options (Event Grid, SQS, Pub/Sub)

## 📖 Documentation

For detailed API reference, see the [Streaming Processor docs](../../../docs/streaming.md).
