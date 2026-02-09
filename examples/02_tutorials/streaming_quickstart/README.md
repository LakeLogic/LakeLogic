# Streaming Quickstart Tutorial

**Time:** 5 minutes  
**Level:** Beginner  
**Goal:** Stream your first real-time data with LakeLogic

---

## 🎯 **What You'll Learn**

1. Install streaming dependencies
2. Connect to a real-time stream (Wikimedia)
3. Process events in real-time
4. Understand streaming basics

---

## 📋 **Prerequisites**

- Python 3.9+
- LakeLogic installed: `pip install lakelogic`

---

## 🚀 **Step 1: Install Streaming Support**

```bash
pip install "lakelogic[sse]"
```

**What this installs:**
- `sseclient-py` - Server-Sent Events client
- Dependencies for real-time streaming

---

## 🌊 **Step 2: Your First Stream**

Create a file called `my_first_stream.py`:

```python
from lakelogic.engines.streaming_connectors import SSEConnector

# Connect to Wikimedia Recent Changes stream
connector = SSEConnector(
    url="https://stream.wikimedia.org/v2/stream/recentchange"
)

print("🌊 Streaming Wikipedia edits in real-time...")
print("Press Ctrl+C to stop")
print()

# Stream events
event_count = 0

for event in connector.stream():
    event_count += 1
    
    # Extract key fields
    title = event.get('title', 'N/A')
    user = event.get('user', 'N/A')
    event_type = event.get('type', 'unknown')
    
    # Print event
    print(f"[{event_count:04d}] {event_type:10s} | {title[:50]:50s} | by {user}")
    
    # Stop after 20 events (for demo)
    if event_count >= 20:
        print()
        print(f"✅ Processed {event_count} events!")
        break

connector.close()
```

---

## ▶️ **Step 3: Run It!**

```bash
python my_first_stream.py
```

**Expected Output:**
```
🌊 Streaming Wikipedia edits in real-time...
Press Ctrl+C to stop

[0001] edit       | Python (programming language)                     | by User123
[0002] new        | Berlin                                            | by BotUser
[0003] edit       | Machine learning                                  | by DataScientist
...
[0020] edit       | Real-time computing                               | by StreamFan

✅ Processed 20 events!
```

---

## 🎉 **Congratulations!**

You just:
- ✅ Installed streaming support
- ✅ Connected to a real-time stream
- ✅ Processed live Wikipedia edits
- ✅ Learned streaming basics

---

## 🚀 **Next Steps**

### **1. Try Other Streams**

**Coinbase (Crypto Prices):**
```bash
pip install "lakelogic[websocket]"
```

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
    if event.get('type') == 'ticker':
        print(f"BTC: ${event['price']}")
```

---

### **2. Add Data Validation**

See: `examples/02_tutorials/streaming_with_validation/`

---

### **3. Use Contracts**

See: `examples/06_streaming/sse/wikimedia_contract_example.py`

---

### **4. Explore Other Connectors**

- **Kafka:** `examples/06_streaming/kafka/`
- **Azure Event Grid:** `examples/06_streaming/azure_eventgrid/`
- **AWS SQS:** `examples/06_streaming/aws_sqs/`
- **GCP Pub/Sub:** `examples/06_streaming/gcp_pubsub/`

---

## 💡 **Key Concepts**

### **What is Streaming?**
Processing data in real-time as it arrives, instead of in batches.

### **SSE (Server-Sent Events)**
One-way communication from server to client. Perfect for:
- Live updates
- Real-time notifications
- Event streams

### **When to Use Streaming?**
- Real-time dashboards
- Live monitoring
- Event-driven architectures
- Time-sensitive data

---

## 🐛 **Troubleshooting**

### **Connection Error**
```
Error: Connection refused
```
**Solution:** Check your internet connection and firewall settings.

---

### **Import Error**
```
ImportError: No module named 'sseclient'
```
**Solution:** Install streaming dependencies:
```bash
pip install "lakelogic[sse]"
```

---

### **No Events Received**
**Solution:** The stream might be slow. Wait a few seconds or try a different stream.

---

## 📚 **Learn More**

- **All Streaming Connectors:** `examples/06_streaming/README.md`
- **Streaming Test Providers:** `docs/streaming_test_providers.md`
- **API Reference:** `lakelogic/engines/streaming_connectors.py`

---

**Next Tutorial:** [Streaming with Data Validation](../streaming_with_validation/)

---

*Last Updated: February 9, 2026*
