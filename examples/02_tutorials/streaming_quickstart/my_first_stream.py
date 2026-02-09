"""
My First Stream - Streaming Quickstart Tutorial

This is the simplest possible streaming example.
Perfect for learning the basics!

Prerequisites:
    pip install "lakelogic[sse]"

Run:
    python my_first_stream.py

What it does:
- Connects to Wikimedia Recent Changes stream
- Prints 20 real-time Wikipedia edits
- Shows you how streaming works
"""

from lakelogic.engines.streaming_connectors import SSEConnector

def main():
    """Stream your first real-time data!"""
    
    # Connect to Wikimedia Recent Changes stream
    connector = SSEConnector(
        url="https://stream.wikimedia.org/v2/stream/recentchange"
    )
    
    print("🌊 Streaming Wikipedia edits in real-time...")
    print("Press Ctrl+C to stop")
    print()
    
    # Stream events
    event_count = 0
    
    try:
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
                print()
                print("🎉 Congratulations! You just streamed real-time data!")
                print()
                print("Next steps:")
                print("  1. Try other streams (Coinbase, Kafka, etc.)")
                print("  2. Add data validation")
                print("  3. Use contracts for production")
                print()
                print("See: examples/06_streaming/ for more examples")
                break
    
    except KeyboardInterrupt:
        print()
        print(f"✅ Stopped after {event_count} events")
    
    finally:
        connector.close()


if __name__ == "__main__":
    main()
