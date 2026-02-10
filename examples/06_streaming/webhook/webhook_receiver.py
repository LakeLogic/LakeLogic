"""
Example: Receiving GitHub events via Webhooks with LakeLogic.

This script starts a local LakeLogic listener that receives HTTP POST 
requests (webhooks) and processes them in real-time.
"""

from lakelogic.core.streaming_processor import StreamingDataProcessor
import threading
import requests
import time
import json

def simulate_pushes():
    """Simulate some webhook pushes after the server starts."""
    time.sleep(5)
    url = "http://localhost:8080/github-webhook"
    
    payloads = [
        {"action": "opened", "issue": {"title": "Bug in streaming"}, "sender": {"login": "dev1"}},
        {"action": "closed", "issue": {"title": "Fix webhook docs"}, "sender": {"login": "dev2"}},
        {"action": "reopened", "issue": {"title": "Bytewax join issue"}, "sender": {"login": "dev1"}}
    ]
    
    print("\n🧪 Simulating webhook pushes...")
    for p in payloads:
        print(f"📤 Sending event: {p['action']} - {p['issue']['title']}")
        try:
            requests.post(url, json=p)
        except Exception as e:
            print(f"❌ Error sending: {e}")
        time.sleep(2)
    print("✅ Simulation complete.\n")

def run_processor():
    # Initialize the streaming processor with the webhook contract
    processor = StreamingDataProcessor(
        contract="webhook_contract.yaml",
        framework="bytewax"
    )
    
    print("🚀 Starting Webhook Processor...")
    print("Listening on http://localhost:8080/github-webhook")
    
    # Start the processor (blocking)
    processor.start()

if __name__ == "__main__":
    # Start simulator in background
    sim_thread = threading.Thread(target=simulate_pushes)
    sim_thread.daemon = True
    sim_thread.start()
    
    # Run the main processor
    try:
        run_processor()
    except KeyboardInterrupt:
        print("\n👋 Stopping...")
