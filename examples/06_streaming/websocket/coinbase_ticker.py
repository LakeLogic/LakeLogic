"""
Coinbase WebSocket Example

Stream real-time cryptocurrency prices from Coinbase.

Prerequisites:
    pip install "lakelogic[websocket]"

Run:
    python coinbase_ticker.py

What it does:
- Connects to Coinbase WebSocket
- Subscribes to BTC-USD and ETH-USD ticker
- Prints real-time prices
"""

from lakelogic.engines.streaming_connectors import WebSocketConnector
from datetime import datetime

def main():
    """
    Stream real-time crypto prices from Coinbase.
    """
    
    print("=" * 80)
    print("Coinbase Real-Time Crypto Prices")
    print("=" * 80)
    print()
    
    # Create WebSocket connector
    connector = WebSocketConnector(
        url="wss://ws-feed.exchange.coinbase.com",
        subscribe_message={
            "type": "subscribe",
            "channels": [
                {
                    "name": "ticker",
                    "product_ids": ["BTC-USD", "ETH-USD"]
                }
            ]
        }
    )
    
    print("📈 Streaming crypto prices...")
    print("Press Ctrl+C to stop")
    print()
    
    # Stream events
    event_count = 0
    
    try:
        for event in connector.stream():
            # Filter for ticker events
            if event.get('type') == 'ticker':
                event_count += 1
                
                product = event.get('product_id', 'N/A')
                price = event.get('price', 'N/A')
                volume = event.get('volume_24h', 'N/A')
                time = event.get('time', '')
                
                # Format price
                if price != 'N/A':
                    price_float = float(price)
                    price_str = f"${price_float:,.2f}"
                else:
                    price_str = price
                
                # Print event
                print(f"[{event_count:04d}] {datetime.now().strftime('%H:%M:%S')} | {product:10s} | {price_str:15s} | 24h Vol: {volume}")
                
                # Stop after 50 events (for demo)
                if event_count >= 50:
                    print()
                    print(f"✅ Processed {event_count} price updates")
                    break
    
    except KeyboardInterrupt:
        print()
        print(f"✅ Stopped after {event_count} price updates")
    
    finally:
        connector.close()


if __name__ == "__main__":
    main()
