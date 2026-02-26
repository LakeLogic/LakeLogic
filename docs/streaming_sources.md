# Public Streaming Sources for Testing 🌊

This guide lists free, publicly available streaming datasets that can be used to test LakeLogic's streaming connectors.

---

## 📈 Financial Data (WebSockets)

### 1. Coinbase WebSocket
Best for testing high-frequency financial data and order book updates.

- **Status**: No API key required
- **Endpoint**: `wss://ws-feed.exchange.coinbase.com`
- **Subscription Message**:
  ```json
  {
    "type": "subscribe",
    "channels": [{"name": "ticker", "product_ids": ["BTC-USD", "ETH-USD"]}]
  }
  ```

### 2. Binance WebSocket
Another robust source for cryptocurrency price data and trade activity.

- **Status**: No API key required for public streams
- **Endpoint**: `wss://stream.binance.com:9443/ws/btcusdt@trade`

---

## 🌎 Global Event Streams (SSE)

### 1. Wikimedia Recent Changes
Excellent for testing Server-Sent Events (SSE) with real-time editing activity.

- **Status**: No API key required
- **Endpoint**: `https://stream.wikimedia.org/v2/stream/recentchange`
- **Volume**: ~5-20 events/second depending on the time of day.

---

## ✈️ IoT and Transport (REST / Poll-based)

### 1. OpenSky Network (Flight Tracking)
Real-time aircraft positions worldwide. Best for testing geospatial data and poll-based ingestion.

- **Status**: No API key required (rate limited)
- **Endpoint**: `https://opensky-network.org/api/states/all`
- **Protocol**: REST (Poll every 10-30 seconds)

### 2. USGS Earthquake Stream
Real-time seismic data from around the globe.

- **Status**: No API key required
- **Endpoint**: `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson`
- **Format**: GeoJSON

---

## 🏘️ Social Media and News

### 1. Reddit (Pushshift API)
Real-time posts and comments from specific subreddits or all of Reddit.

- **Status**: No API key required
- **Endpoint**: `https://api.pushshift.io/reddit/search/submission/?sort=desc&size=100`

### 2. GDELT Project (Global News)
A massive dataset monitoring world news in real-time.

- **Status**: No API key required
- **Endpoint**: `http://data.gdeltproject.org/gdeltv2/lastupdate.txt`
- **Frequency**: Updates every 15 minutes.

---

## 🔐 Providers Requiring API Keys

For more advanced testing scenarios, the following providers offer free tiers but require a developer account.

| Provider | Data Type | Protocol |
|----------|-----------|----------|
| **Twitter API** | Social Media | JSON Stream |
| **Alpha Vantage** | Stock Market | REST / Poll |
| **IEX Cloud** | Stock Market | WebSocket / SSE |

---

## ✅ Comparison Matrix

| Provider | Data Volume | API Key | Protocol | Use Case |
|----------|-------------|---------|----------|----------|
| **Wikimedia** | Medium (5-10/s) | ❌ No | SSE | General Testing |
| **Coinbase** | High (100+/s) | ❌ No | WebSocket | Financial Analysis |
| **Binance** | High (200+/s) | ❌ No | WebSocket | Stress Testing |
| **OpenSky** | Low (Poll-based) | ❌ No | REST | IoT Dashboards |
| **Reddit** | High (50+/s) | ❌ No | REST | Sentiment Analysis |

---

## 🚀 Next Steps

Ready to build a stream? Check the [Streaming Guide](streaming.md) for implementation details and code examples.
