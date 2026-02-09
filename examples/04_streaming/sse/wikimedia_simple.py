"""
Wikimedia Real-Time Streaming Example (Simple)

This is the simplest possible streaming example - just connect and print events.
No contract, no validation, no processing - just raw streaming.

Use this to:
- Test SSE connector
- Understand Wikimedia event structure
- Debug connection issues

For production use, see: wikimedia_contract_example.py (contract-driven)

Prerequisites:
    pip install "lakelogic[sse]"

Run:
    python examples/04_streaming/sse/wikimedia_simple.py
"""

import json
from datetime import datetime
from lakelogic.engines.streaming_connectors import SSEConnector

def main():
    """
    Simple Wikimedia streaming - no contract, just print events.
    """
    
    print("=" * 80)
    print("Wikimedia Recent Changes Stream (Simple)")
    print("=" * 80)
    print()
    print("Connecting to Wikimedia SSE stream...")
    print("URL: https://stream.wikimedia.org/v2/stream/recentchange")
    print()
    
    # Create SSE connector
    connector = SSEConnector(
        url="https://stream.wikimedia.org/v2/stream/recentchange"
    )
    
    # Stream events
    event_count = 0
    
    try:
        for event in connector.stream():
            event_count += 1
            
            # Extract key fields
            event_type = event.get('type', 'unknown')
            title = event.get('title', 'N/A')
            user = event.get('user', 'N/A')
            server = event.get('server_name', 'N/A')
            bot = event.get('bot', False)
            timestamp = event.get('timestamp', 0)
            
            # Format timestamp
            dt = datetime.fromtimestamp(timestamp) if timestamp else datetime.now()
            
            # Print event
            print(f"[{event_count:04d}] {dt.strftime('%H:%M:%S')} | {event_type:10s} | {server:20s} | {title[:40]:40s} | {user[:20]:20s} | {'BOT' if bot else 'HUMAN'}")
            
            # Print full event every 10 events
            if event_count % 10 == 0:
                print()
                print("Sample event (full):")
                print(json.dumps(event, indent=2))
                print()
            
            # Stop after 100 events (for demo)
            if event_count >= 100:
                print()
                print(f"✅ Processed {event_count} events")
                print("Stopping demo (remove this limit for continuous streaming)")
                break
    
    except KeyboardInterrupt:
        print()
        print(f"✅ Stopped after {event_count} events")
    
    finally:
        connector.close()


if __name__ == "__main__":
    main()
