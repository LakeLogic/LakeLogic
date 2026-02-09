# Streaming Examples

Real-time data streaming with LakeLogic.

---

## 📁 **Files**

### **1. wikimedia_simple.py** ✅ (Start Here)
**Simplest example** - No contract, just connect and print events

```bash
pip install "lakelogic[sse]"
python examples/05_streaming/wikimedia_simple.py
```

**What it does:**
- Connects to Wikimedia Recent Changes stream
- Prints events in real-time (~5-10/second)
- No validation, no processing
- Perfect for testing

---

### **2. wikimedia_stream_contract.yaml** ✅
**Complete streaming contract** with:
- Source configuration (SSE)
- Schema definition
- Transformations
- Quality rules
- Bronze layer (history)
- Realtime layer (materialized views)

---

### **3. wikimedia_stream_example.py** ✅ (Production)
**Contract-driven streaming** - Full pipeline with validation

```bash
pip install "lakelogic[streaming]"
python examples/05_streaming/wikimedia_stream_example.py
```

**What it does:**
- Loads contract (`wikimedia_stream_contract.yaml`)
- Auto-selects framework (Bytewax)
- Validates schema and quality
- Writes to Bronze (complete history)
- Creates Realtime materialized views:
  - `current_activity` (last 10 seconds)
  - `edits_by_language_1min` (last 5 minutes)
  - `top_pages_1min` (last 10 minutes)

---

## 🚀 **Quick Start**

### **Step 1: Install**
```bash
# Simple example (SSE only)
pip install "lakelogic[sse]"

# Full streaming (Bytewax + SSE)
pip install "lakelogic[streaming]"
```

### **Step 2: Run Simple Example**
```bash
python examples/05_streaming/wikimedia_simple.py
```

**Output:**
```
[0001] 22:42:52 | edit | en.wikipedia.org | Python (programming language) | ExampleUser | HUMAN
[0002] 22:42:53 | new  | de.wikipedia.org | Berlin                        | BotUser     | BOT
...
```

### **Step 3: Run Contract Example**
```bash
python examples/05_streaming/wikimedia_stream_example.py
```

**Output:**
```
✅ Processor initialized
   Framework: bytewax
   Dataset: wikimedia_recentchanges

▶️  Starting streaming pipeline...
✅ Connected to SSE stream
📊 Processing events...
```

---

## 📊 **Data Flow**

```
Wikimedia SSE Stream
         │
         ▼
   SSE Connector
         │
         ▼
  Schema Validation
         │
         ▼
  Transformations
         │
         ▼
  Quality Validation
         │
         ├─────────────────┬──────────────────┐
         ▼                 ▼                  ▼
    BRONZE           REALTIME          QUARANTINE
  (History)         (Current)            (Bad)
  append            update             append
  forever           retention          forever
```

---

## 🎯 **Use Cases**

### **1. Real-Time Monitoring**
Monitor Wikipedia edits in real-time:
- Track vandalism
- Monitor trending topics
- Analyze bot vs human activity

### **2. Language Analysis**
Analyze edits by language:
- Which languages are most active?
- Bot percentage by language
- Unique editors per language

### **3. Page Popularity**
Track most edited pages:
- Breaking news detection
- Controversial topics
- Edit wars

---

## 📋 **Contract Structure**

```yaml
# Source: Where data comes from
source:
  type: stream
  connector: sse
  url: https://stream.wikimedia.org/v2/stream/recentchange

# Bronze: Complete history (append forever)
materialization:
  bronze:
    path: ./data/bronze/wikimedia_changes/
    mode: append

# Realtime: Materialized views (current only)
realtime:
  - name: current_activity
    window: tumbling_10s
    metrics:
      - name: total_edits
        expression: COUNT(*)
    output:
      path: ./data/realtime/current_activity/
      mode: overwrite
      retention: 1m  # Auto-delete after 1 minute
```

---

## 🔧 **Configuration**

### **Framework Selection**
```python
# Automatic (recommended)
processor = StreamingDataProcessor(contract="contract.yaml")

# Manual
processor = StreamingDataProcessor(contract="contract.yaml", framework="bytewax")
```

### **Data Paths**
All data written to `./data/`:
- `./data/bronze/` - Complete history
- `./data/realtime/` - Current materialized views
- `./data/checkpoints/` - Streaming checkpoints

---

## 📚 **Documentation**

- **Streaming Test Providers:** `docs/streaming_test_providers.md`
- **Implementation Status:** `docs/streaming_implementation_status.md`
- **Product Vision:** `.product_vision/05_streaming_capabilities.md`

---

## 🐛 **Troubleshooting**

### **Connection Issues**
```python
# Test SSE connection
from lakelogic.engines.streaming_connectors import SSEConnector

connector = SSEConnector("https://stream.wikimedia.org/v2/stream/recentchange")
for event in connector.stream():
    print(event)
    break  # Print first event and exit
```

### **Import Errors**
```bash
# Install missing dependencies
pip install "lakelogic[streaming]"

# Or specific components
pip install "lakelogic[sse]"
pip install "lakelogic[bytewax]"
```

---

## 🚧 **Coming Soon**

- ❌ Coinbase WebSocket example
- ❌ Kafka connector
- ❌ Pathway integration
- ❌ Delta Lake integration
- ❌ Real-time dashboards

---

*Last Updated: February 9, 2026*
