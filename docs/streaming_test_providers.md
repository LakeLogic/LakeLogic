# Public Streaming Data Providers

Complete guide to free, publicly available streaming datasets for testing LakeLogic's streaming capabilities.

---

## 🌊 **Recommended Providers**

### **1. Wikimedia Recent Changes** ✅✅✅ (Best for Testing)

**What:** Real-time edits to Wikipedia, Wikidata, and other Wikimedia projects

**Volume:** 5-10 events/second (varies by time of day)

**Format:** Server-Sent Events (SSE), JSON

**API Key:** None required

**Endpoint:**
```
https://stream.wikimedia.org/v2/stream/recentchange
```

**Example Event:**
```json
{
  "id": 1234567890,
  "type": "edit",
  "title": "Python (programming language)",
  "user": "ExampleUser",
  "bot": false,
  "timestamp": 1707516437,
  "server_name": "en.wikipedia.org",
  "namespace": 0,
  "comment": "Fixed typo",
  "length": {"old": 12345, "new": 12350}
}
```

**Use Cases:**
- Real-time edit monitoring
- Bot vs human edit analysis
- Language-specific activity tracking
- Top edited pages

**Documentation:** https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams

---

### **2. Coinbase WebSocket** ✅✅ (Financial Data)

**What:** Real-time cryptocurrency prices, trades, and order book updates

**Volume:** 100+ events/second

**Format:** WebSocket, JSON

**API Key:** None required for public data

**Endpoint:**
```
wss://ws-feed.exchange.coinbase.com
```

**Subscribe Message:**
```json
{
  "type": "subscribe",
  "channels": [
    {"name": "ticker", "product_ids": ["BTC-USD", "ETH-USD"]}
  ]
}
```

**Example Event:**
```json
{
  "type": "ticker",
  "product_id": "BTC-USD",
  "price": "43250.50",
  "time": "2026-02-09T22:31:33.123456Z",
  "volume_24h": "12345.67",
  "best_bid": "43250.00",
  "best_ask": "43251.00"
}
```

**Use Cases:**
- Real-time price monitoring
- Trading volume analysis
- Price volatility detection
- Arbitrage opportunities

**Documentation:** https://docs.cloud.coinbase.com/exchange/docs/websocket-overview

---

### **3. OpenSky Network** ✅ (Flight Tracking)

**What:** Real-time aircraft positions worldwide

**Volume:** Poll-based (recommend every 10 seconds)

**Format:** REST API, JSON

**API Key:** None required (rate limited)

**Endpoint:**
```
https://opensky-network.org/api/states/all
```

**Example Response:**
```json
{
  "time": 1707516437,
  "states": [
    [
      "abc123",           // ICAO24 address
      "UAL123",           // Callsign
      "United States",    // Origin country
      1707516437,         // Time position
      1707516437,         // Last contact
      -122.4194,          // Longitude
      37.7749,            // Latitude
      10000,              // Altitude (meters)
      false,              // On ground
      250,                // Velocity (m/s)
      90,                 // Heading (degrees)
      0,                  // Vertical rate (m/s)
      null,               // Sensors
      10500,              // Geo altitude
      "1234",             // Squawk
      false,              // SPI
      0                   // Position source
    ]
  ]
}
```

**Use Cases:**
- Flight tracking
- Airspace congestion analysis
- Route optimization
- Delay prediction

**Documentation:** https://openskynetwork.github.io/opensky-api/

---

### **4. Binance WebSocket** ✅ (Crypto Trading)

**What:** Real-time cryptocurrency trades and market data

**Volume:** 1000+ events/second

**Format:** WebSocket, JSON

**API Key:** None required for public streams

**Endpoint:**
```
wss://stream.binance.com:9443/ws/btcusdt@trade
```

**Example Event:**
```json
{
  "e": "trade",
  "E": 1707516437000,
  "s": "BTCUSDT",
  "t": 12345,
  "p": "43250.50",
  "q": "0.001",
  "b": 88,
  "a": 50,
  "T": 1707516437000,
  "m": true,
  "M": true
}
```

**Use Cases:**
- High-frequency trading analysis
- Market microstructure
- Liquidity analysis

**Documentation:** https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams

---

### **5. Alpha Vantage (Stock Market)** ⚠️ (API Key Required)

**What:** Real-time and historical stock prices

**Volume:** Poll-based (5 requests/minute on free tier)

**Format:** REST API, JSON/CSV

**API Key:** Free tier available

**Endpoint:**
```
https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=1min&apikey=YOUR_API_KEY
```

**Use Cases:**
- Stock price monitoring
- Technical analysis
- Portfolio tracking

**Documentation:** https://www.alphavantage.co/documentation/

---

### **6. Reddit Pushshift API** ✅ (Social Media)

**What:** Real-time Reddit posts and comments

**Volume:** 100+ events/second

**Format:** REST API (poll every few seconds), JSON

**API Key:** None required

**Endpoint:**
```
https://api.pushshift.io/reddit/search/submission/?sort=desc&sort_type=created_utc&size=100
```

**Example Response:**
```json
{
  "data": [
    {
      "author": "example_user",
      "created_utc": 1707516437,
      "subreddit": "python",
      "title": "How to use LakeLogic for streaming?",
      "selftext": "I'm trying to...",
      "score": 42,
      "num_comments": 5
    }
  ]
}
```

**Use Cases:**
- Social media monitoring
- Sentiment analysis
- Trending topic detection

**Documentation:** https://pushshift.io/api-parameters/

---

### **7. GDELT Project** ✅ (Global News)

**What:** Real-time global news events

**Volume:** Updated every 15 minutes

**Format:** CSV files

**API Key:** None required

**Endpoint:**
```
http://data.gdeltproject.org/gdeltv2/lastupdate.txt
```

**Use Cases:**
- News monitoring
- Event detection
- Geopolitical analysis

**Documentation:** https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/

---

### **8. Earthquake USGS Stream** ✅ (Seismic Data)

**What:** Real-time earthquake data worldwide

**Volume:** Varies (1-100 events/day)

**Format:** GeoJSON

**API Key:** None required

**Endpoint:**
```
https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson
```

**Example Event:**
```json
{
  "type": "Feature",
  "properties": {
    "mag": 4.5,
    "place": "10km NE of San Francisco, CA",
    "time": 1707516437000,
    "updated": 1707516437000,
    "tz": null,
    "url": "https://earthquake.usgs.gov/earthquakes/eventpage/...",
    "detail": "...",
    "felt": 100,
    "cdi": 5.2,
    "mmi": 4.1,
    "alert": "green",
    "status": "reviewed",
    "tsunami": 0,
    "sig": 312,
    "net": "us",
    "code": "...",
    "ids": "...",
    "sources": "...",
    "types": "...",
    "nst": null,
    "dmin": 0.123,
    "rms": 0.45,
    "gap": 67,
    "magType": "mb",
    "type": "earthquake",
    "title": "M 4.5 - 10km NE of San Francisco, CA"
  },
  "geometry": {
    "type": "Point",
    "coordinates": [-122.4194, 37.7749, 10.0]
  }
}
```

**Use Cases:**
- Seismic monitoring
- Disaster response
- Risk analysis

**Documentation:** https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php

---

### **9. Twitter Stream API** ⚠️ (API Key Required)

**What:** Real-time tweets

**Volume:** 1000+ tweets/second (depends on filters)

**Format:** JSON

**API Key:** Required (free tier: 500K tweets/month)

**Use Cases:**
- Social media monitoring
- Sentiment analysis
- Trend detection

**Documentation:** https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/introduction

---

### **10. IEX Cloud (Stock Market)** ⚠️ (API Key Required)

**What:** Real-time stock quotes and trades

**Volume:** Varies

**Format:** WebSocket, JSON

**API Key:** Free tier available

**Endpoint:**
```
wss://cloud-sse.iexapis.com/stable/stocksUSNoUTP?token=YOUR_TOKEN
```

**Use Cases:**
- Stock price monitoring
- Trading algorithms
- Market analysis

**Documentation:** https://iexcloud.io/docs/api/

---

## 🎯 **Comparison Matrix**

| Provider | Volume | API Key | Protocol | Best For |
|----------|--------|---------|----------|----------|
| **Wikimedia** | Medium (5-10/s) | ❌ No | SSE | General testing |
| **Coinbase** | High (100+/s) | ❌ No | WebSocket | Financial data |
| **OpenSky** | Low (poll) | ❌ No | REST | IoT/geospatial |
| **Binance** | Very High (1000+/s) | ❌ No | WebSocket | High-frequency |
| **Alpha Vantage** | Low (poll) | ✅ Yes | REST | Stock market |
| **Reddit** | High (100+/s) | ❌ No | REST | Social media |
| **GDELT** | Low (15 min) | ❌ No | CSV | News events |
| **USGS** | Low (hourly) | ❌ No | GeoJSON | Seismic data |
| **Twitter** | Very High | ✅ Yes | JSON | Social media |
| **IEX Cloud** | High | ✅ Yes | WebSocket | Stock market |

---

## ✅ **Recommended for LakeLogic Testing**

### **Beginner:** Wikimedia Recent Changes
- No API key
- Medium volume
- Well-structured
- Reliable

### **Intermediate:** Coinbase WebSocket
- No API key
- High volume
- Financial use case
- WebSocket protocol

### **Advanced:** Binance WebSocket
- No API key
- Very high volume
- Stress testing
- Production-like

---

## 📚 **Additional Resources**

- **Awesome Public Datasets:** https://github.com/awesomedata/awesome-public-datasets
- **Public APIs:** https://github.com/public-apis/public-apis
- **Real-Time Data Sources:** https://github.com/caesar0301/awesome-public-datasets#realtime

---

*Last Updated: February 2026*
