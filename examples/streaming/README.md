# Streaming Examples

Real-time data streaming examples using LakeLogic.

---

## 🚀 **Quick Start**

### **1. Install Streaming Dependencies**

```bash
# All streaming features
pip install "lakelogic[streaming]"

# Or install specific connectors
pip install "lakelogic[sse]"        # Server-Sent Events (Wikimedia)
pip install "lakelogic[websocket]"  # WebSocket (Coinbase, Binance)
```

---

### **2. Run Wikimedia Example**

```bash
python examples/streaming/wikimedia_simple_example.py
```

**Output:**
```
================================================================================
Wikimedia Recent Changes Stream
================================================================================

Connecting to Wikimedia SSE stream...
URL: https://stream.wikimedia.org/v2/stream/recentchange

[0001] 22:36:52 | edit       | en.wikipedia.org     | Python (programming language)            | ExampleUser          | HUMAN
[0002] 22:36:53 | new        | de.wikipedia.org     | Berlin                                   | BotUser              | BOT
[0003] 22:36:54 | edit       | fr.wikipedia.org     | Paris                                    | AnotherUser          | HUMAN
...
```

---

## 📋 **Available Examples**

### **1. Wikimedia Simple Example** ✅

**File:** `wikimedia_simple_example.py`

**Description:** Real-time Wikipedia edits using SSE connector

**Features:**
- No API key required
- ~5-10 events/second
- Perfect for testing

**Run:**
```bash
python examples/streaming/wikimedia_simple_example.py
```

---

### **2. Coinbase WebSocket Example** 🔄 (Coming Soon)

**File:** `coinbase_example.py`

**Description:** Real-time cryptocurrency prices

**Features:**
- WebSocket connector
- BTC, ETH, and other crypto prices
- High-frequency data (100+ events/sec)

**Run:**
```bash
python examples/streaming/coinbase_example.py
```

---

### **3. Contract-Driven Streaming** 🔄 (Coming Soon)

**File:** `wikimedia_contract_example.py`

**Description:** Full contract-driven streaming pipeline

**Features:**
- YAML contract configuration
- Automatic data validation
- Bronze + Realtime layers
- Delta Lake integration

**Run:**
```bash
python examples/streaming/wikimedia_contract_example.py
```

---

## 🌊 **Public Streaming Data Sources**

### **No API Key Required:**

1. **Wikimedia Recent Changes** (Recommended)
   - URL: `https://stream.wikimedia.org/v2/stream/recentchange`
   - Protocol: SSE
   - Volume: 5-10 events/sec

2. **Coinbase WebSocket**
   - URL: `wss://ws-feed.exchange.coinbase.com`
   - Protocol: WebSocket
   - Volume: 100+ events/sec

3. **Binance WebSocket**
   - URL: `wss://stream.binance.com:9443/ws/btcusdt@trade`
   - Protocol: WebSocket
   - Volume: 1000+ events/sec

**See:** `docs/streaming_test_providers.md` for complete list (10+ providers)

---

## 💻 **Code Examples**

### **SSE Connector (Wikimedia)**

```python
from lakelogic.engines.streaming_connectors import SSEConnector

# Connect to Wikimedia stream
connector = SSEConnector("https://stream.wikimedia.org/v2/stream/recentchange")

# Stream events
for event in connector.stream():
    print(f"{event['type']} | {event['title']} | {event['user']}")
```

---

### **WebSocket Connector (Coinbase)**

```python
from lakelogic.engines.streaming_connectors import WebSocketConnector

# Connect to Coinbase WebSocket
connector = WebSocketConnector(
    url="wss://ws-feed.exchange.coinbase.com",
    subscribe_message={
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]
    }
)

# Stream events
for event in connector.stream():
    if event.get('type') == 'ticker':
        print(f"BTC: ${event['price']}")
```

---

## 📚 **Documentation**

- **Streaming Test Providers:** `docs/streaming_test_providers.md`
- **Implementation Status:** `docs/streaming_implementation_status.md`
- **Session Summary:** `docs/streaming_session_summary.md`
- **Product Vision:** `.product_vision/05_streaming_capabilities.md`

---

## 🎯 **Next Steps**

1. **Try Wikimedia Example** - Run `wikimedia_simple_example.py`
2. **Explore Connectors** - Check `lakelogic/engines/streaming_connectors.py`
3. **Read Documentation** - See `docs/streaming_test_providers.md`
4. **Wait for Contract Integration** - Coming in Phase 2!

---

## 🚧 **Work in Progress**

The following features are under development:

- ❌ Contract-driven streaming
- ❌ Automatic framework selection (Bytewax vs Pathway)
- ❌ Realtime layer (Bronze + Realtime dual-write)
- ❌ Data quality validation (streaming mode)
- ❌ Delta Lake integration (streaming)

**Expected:** 1-2 weeks for Minimum Viable Streaming (MVS)

---

## 💡 **Tips**

### **Performance**
- SSE: ~5-10 events/sec (Wikimedia)
- WebSocket: 100-1000+ events/sec (Coinbase, Binance)
- Kafka: 10K-100K+ events/sec (coming soon)

### **Error Handling**
- All connectors have automatic retry on failure
- Use `try/except` for graceful shutdown
- Press `Ctrl+C` to stop streaming

### **Testing**
- Start with Wikimedia (simplest, no API key)
- Move to Coinbase for higher volume
- Use Binance for stress testing

---

*Last Updated: February 9, 2026*
