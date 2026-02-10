"""
Wikimedia Real-Time Streaming Example

This example demonstrates contract-driven streaming with LakeLogic.

Features:
- Real-time Wikipedia edits (5-10 events/second)
- Contract-driven pipeline (YAML configuration)
- Automatic framework selection (Bytewax)
- Bronze (history) + Realtime (current) dual-write
- Data quality validation

Prerequisites:
    pip install "lakelogic[streaming]"

Run:
    python examples/05_streaming/wikimedia_stream_example.py

What it does:
1. Connects to Wikimedia Recent Changes stream (SSE)
2. Validates data against contract schema
3. Applies transformations (wiki_language, is_article_edit, etc.)
4. Validates quality rules
5. Writes to Bronze layer (complete history)
6. Creates Realtime materialized views:
   - current_activity (last 10 seconds)
   - edits_by_language_1min (last 5 minutes)
   - top_pages_1min (last 10 minutes)
"""

from pathlib import Path
from lakelogic.core.streaming_processor import StreamingDataProcessor
from loguru import logger

def main():
    """
    Run Wikimedia streaming pipeline with contract.
    """
    
    print("=" * 80)
    print("Wikimedia Real-Time Streaming with LakeLogic")
    print("=" * 80)
    print()
    
    # Get contract path
    contract_path = Path(__file__).parent / "wikimedia_stream_contract.yaml"
    
    if not contract_path.exists():
        print(f"❌ Contract not found: {contract_path}")
        print("Please ensure wikimedia_stream_contract.yaml exists")
        return
    
    print(f"📋 Contract: {contract_path}")
    print()
    
    # Create streaming processor
    print("🚀 Initializing streaming processor...")
    processor = StreamingDataProcessor(
        contract=str(contract_path)
        # framework="auto"  # Automatic selection (default)
    )
    
    print()
    print("✅ Processor initialized")
    print(f"   Framework: {processor.framework}")
    print(f"   Dataset: {processor.contract.get('dataset')}")
    print()
    
    # Start streaming
    print("▶️  Starting streaming pipeline...")
    print("   Press Ctrl+C to stop")
    print()
    print("-" * 80)
    print()
    
    try:
        processor.start()
    
    except KeyboardInterrupt:
        print()
        print()
        print("-" * 80)
        print("⏹️  Stopped by user")
        print("✅ Pipeline shutdown complete")
    
    except Exception as e:
        print()
        print()
        print("-" * 80)
        print(f"❌ Error: {e}")
        logger.exception("Pipeline error")


if __name__ == "__main__":
    main()
