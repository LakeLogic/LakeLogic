# SSE (Server-Sent Events) Examples

Server-Sent Events streaming examples.

---

## 📋 **Examples**

### **1. wikimedia_simple.py** ✅ (Start Here)
**Simplest example** - No contract, just connect and print events

```bash
pip install "lakelogic[sse]"
python wikimedia_simple.py
```

**What it does:**
- Connects to Wikimedia Recent Changes stream
- Prints events in real-time (~5-10/second)
- No validation, no processing
- Perfect for testing

**Output:**
```
[0001] 22:47:36 | edit | en.wikipedia.org | Python (programming language) | User123 | HUMAN
[0002] 22:47:37 | new  | de.wikipedia.org | Berlin                        | BotUser | BOT
```

---

### **2. wikimedia_contract.yaml** ✅
**Complete streaming contract** with:
- Source configuration (SSE)
- Schema definition
- Transformations
- Quality rules
- Bronze layer (history)
- Realtime layer (materialized views)

---

### **3. wikimedia_contract_example.py** 🔄 (Coming Soon)
**Contract-driven streaming** - Full pipeline with validation

```bash
pip install "lakelogic[streaming]"
python wikimedia_contract_example.py
```

**What it does:**
- Loads contract (`wikimedia_contract.yaml`)
- Auto-selects framework (Bytewax)
- Validates schema and quality
- Writes to Bronze (complete history)
- Creates Realtime materialized views

---

## 🌊 **Wikimedia Stream Details**

**URL:** `https://stream.wikimedia.org/v2/stream/recentchange`  
**Protocol:** SSE (Server-Sent Events)  
**Volume:** ~5-10 events/second  
**API Key:** Not required  
**Cost:** Free

**Event Types:**
- `edit` - Page edit
- `new` - New page
- `log` - Log event
- `categorize` - Category change

---

## 💻 **Code Example**

```python
from lakelogic.engines.streaming_connectors import SSEConnector

# Connect to Wikimedia stream
connector = SSEConnector("https://stream.wikimedia.org/v2/stream/recentchange")

# Stream events
for event in connector.stream():
    print(f"{event['type']} | {event['title']} | {event['user']}")
```

---

## 🎯 **Use Cases**

1. **Real-Time Monitoring**
   - Track vandalism
   - Monitor trending topics
   - Analyze bot vs human activity

2. **Language Analysis**
   - Which languages are most active?
   - Bot percentage by language
   - Unique editors per language

3. **Page Popularity**
   - Track most edited pages
   - Breaking news detection
   - Edit wars

---

## 📚 **Documentation**

- **Wikimedia Stream Docs:** https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams
- **SSE Connector API:** `lakelogic/engines/streaming_connectors.py`
- **Test Providers:** `docs/streaming_test_providers.md`

---

*Last Updated: February 9, 2026*
