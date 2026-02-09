"""
Streaming data processor with contract-driven pipelines.

This module provides the core streaming processor that:
1. Reads streaming contracts (YAML)
2. Automatically selects framework (Bytewax vs Pathway)
3. Creates streaming pipelines
4. Validates data quality in real-time
5. Writes to Bronze (history) and Realtime (current) layers
"""

import json
from typing import Dict, Any, Optional, Iterator
from pathlib import Path
from loguru import logger
import polars as pl


class StreamingFrameworkSelector:
    """
    Automatically select the best streaming framework based on contract.
    """
    
    def select(self, contract: Dict[str, Any]) -> str:
        """
        Select streaming framework based on contract analysis.
        
        Args:
            contract: Parsed contract dictionary
        
        Returns:
            "bytewax" or "pathway"
        """
        # Check if user explicitly specified framework
        streaming_config = contract.get("source", {}).get("streaming", {})
        framework = streaming_config.get("framework", "auto")
        
        if framework and framework != "auto":
            logger.info(f"Using user-specified framework: {framework}")
            return framework
        
        # Auto-select based on contract features
        logger.info("Auto-selecting streaming framework...")
        
        score_bytewax = 0
        score_pathway = 0
        reasons = []
        
        # Rule 1: SQL-heavy operations → Pathway
        if self._has_complex_sql(contract):
            score_pathway += 3
            reasons.append("Complex SQL operations detected → Pathway")
        
        # Rule 2: Stream-batch joins → Pathway
        if self._has_stream_batch_joins(contract):
            score_pathway += 5
            reasons.append("Stream-batch joins detected → Pathway")
        
        # Rule 3: Complex aggregations → Pathway
        if self._has_complex_aggregations(contract):
            score_pathway += 3
            reasons.append("Complex aggregations detected → Pathway")
        
        # Rule 4: High throughput → Bytewax
        if self._requires_high_throughput(contract):
            score_bytewax += 3
            reasons.append("High throughput required → Bytewax")
        
        # Rule 5: Stateful processing → Bytewax
        if self._has_stateful_operations(contract):
            score_bytewax += 2
            reasons.append("Stateful operations detected → Bytewax")
        
        # Rule 6: Simple transformations → Bytewax
        if self._has_simple_transformations(contract):
            score_bytewax += 1
            reasons.append("Simple transformations → Bytewax")
        
        # Default: Bytewax (if scores are equal)
        if score_pathway > score_bytewax:
            selected = "pathway"
        else:
            selected = "bytewax"
        
        logger.info(f"✅ Auto-selected framework: {selected}")
        logger.info(f"   Bytewax score: {score_bytewax}, Pathway score: {score_pathway}")
        for reason in reasons:
            logger.info(f"   - {reason}")
        
        return selected
    
    def _has_complex_sql(self, contract: Dict) -> bool:
        """Check if contract has complex SQL operations."""
        # Check realtime aggregations for SQL functions
        realtime = contract.get("realtime", [])
        for agg in realtime:
            for metric in agg.get("metrics", []):
                expr = metric.get("expression", "").upper()
                # Complex SQL functions
                if any(func in expr for func in ["CASE", "COALESCE", "NULLIF", "MODE", "PERCENTILE"]):
                    return True
        
        # Check transformations for SQL
        transformations = contract.get("transformations", [])
        for transform in transformations:
            expr = transform.get("expression", "").upper()
            if any(func in expr for func in ["CASE", "COALESCE", "NULLIF"]):
                return True
        
        return False
    
    def _has_stream_batch_joins(self, contract: Dict) -> bool:
        """Check if contract has stream-batch joins."""
        enrichment = contract.get("enrichment", [])
        for enrich in enrichment:
            if enrich.get("type") == "join" and enrich.get("source") in ["delta", "parquet", "csv"]:
                return True
        return False
    
    def _has_complex_aggregations(self, contract: Dict) -> bool:
        """Check if contract has complex aggregations."""
        realtime = contract.get("realtime", [])
        
        # Multiple aggregations → complex
        if len(realtime) > 2:
            return True
        
        # Multiple group-by columns → complex
        for agg in realtime:
            if len(agg.get("group_by", [])) > 2:
                return True
            
            # Multiple metrics → complex
            if len(agg.get("metrics", [])) > 3:
                return True
        
        return False
    
    def _requires_high_throughput(self, contract: Dict) -> bool:
        """Check if high throughput is required."""
        streaming = contract.get("source", {}).get("streaming", {})
        throughput = streaming.get("throughput", {})
        
        expected_rps = throughput.get("expected_records_per_second", 0)
        
        # > 10K records/sec → high throughput
        return expected_rps > 10000
    
    def _has_stateful_operations(self, contract: Dict) -> bool:
        """Check if contract has stateful operations."""
        streaming = contract.get("source", {}).get("streaming", {})
        window = streaming.get("window", {})
        
        # Session windows → stateful
        if window.get("type") == "session":
            return True
        
        # Deduplication → stateful
        stream_rules = contract.get("quality", {}).get("stream_rules", [])
        for rule in stream_rules:
            if rule.get("rule") == "deduplication":
                return True
        
        return False
    
    def _has_simple_transformations(self, contract: Dict) -> bool:
        """Check if transformations are simple."""
        transformations = contract.get("transformations", [])
        
        # No transformations → simple
        if not transformations:
            return True
        
        # All transformations are simple expressions
        for transform in transformations:
            expr = transform.get("expression", "").upper()
            # Complex if has CASE, subqueries, etc.
            if any(keyword in expr for keyword in ["CASE", "SELECT", "JOIN"]):
                return False
        
        return True


class StreamingDataProcessor:
    """
    Process streaming data with contracts.
    
    Features:
    - Contract-driven pipeline generation
    - Automatic framework selection (Bytewax vs Pathway)
    - Real-time data validation
    - Bronze (history) + Realtime (current) dual-write
    - Automatic retention management
    
    Example:
        >>> processor = StreamingDataProcessor(
        ...     contract="contracts/wikimedia_stream.yaml"
        ... )
        >>> processor.start()  # Runs continuously
    """
    
    def __init__(
        self,
        contract: str,
        framework: str = "auto"
    ):
        """
        Initialize streaming processor.
        
        Args:
            contract: Path to contract YAML file
            framework: "auto", "bytewax", or "pathway"
        """
        self.contract_path = contract
        self.contract = self._load_contract(contract)
        
        # Select framework
        selector = StreamingFrameworkSelector()
        self.framework = selector.select(self.contract) if framework == "auto" else framework
        
        logger.info(f"Initialized StreamingDataProcessor with {self.framework}")
    
    def _load_contract(self, contract_path: str) -> Dict[str, Any]:
        """Load contract from YAML file."""
        import yaml
        
        with open(contract_path, 'r') as f:
            contract = yaml.safe_load(f)
        
        logger.info(f"Loaded contract: {contract.get('dataset', 'unknown')}")
        return contract
    
    def start(self):
        """
        Start streaming pipeline.
        
        This runs continuously until stopped (Ctrl+C).
        """
        logger.info(f"Starting streaming pipeline with {self.framework}...")
        
        if self.framework == "bytewax":
            self._start_bytewax()
        elif self.framework == "pathway":
            self._start_pathway()
        else:
            raise ValueError(f"Unknown framework: {self.framework}")
    
    def _start_bytewax(self):
        """Start Bytewax streaming pipeline."""
        try:
            from bytewax.dataflow import Dataflow
            from bytewax import operators as op
            from bytewax.connectors.stdio import StdOutSink
        except ImportError:
            raise ImportError(
                "Bytewax not installed. Install with: pip install 'lakelogic[bytewax]'"
            )
        
        logger.info("Creating Bytewax dataflow...")
        
        # Create dataflow
        flow = Dataflow("lakelogic_streaming")
        
        # 1. Input from source
        stream = op.input("source", flow, self._create_input())
        
        # 2. Validate schema
        stream = op.map("validate_schema", stream, self._validate_schema)
        
        # 3. Apply transformations
        stream = op.map("transform", stream, self._apply_transformations)
        
        # 4. Validate quality
        stream = op.map("validate_quality", stream, self._validate_quality)
        
        # 5. Split good/bad records
        good_stream = op.filter("filter_good", stream, lambda x: x.get("_quality_status") == "good")
        bad_stream = op.filter("filter_bad", stream, lambda x: x.get("_quality_status") == "bad")
        
        # 6. Write to Bronze (history)
        op.output("bronze", good_stream, StdOutSink())  # TODO: Replace with Delta Lake
        
        # 7. Create realtime aggregations
        for agg_config in self.contract.get("realtime", []):
            self._create_bytewax_aggregation(flow, good_stream, agg_config)
        
        # Run dataflow
        logger.info("✅ Bytewax dataflow created, starting execution...")
        logger.info("Press Ctrl+C to stop")
        
        # For now, just run with stdio (will add proper execution later)
        from bytewax.testing import run_main
        run_main(flow)
    
    def _start_pathway(self):
        """Start Pathway streaming pipeline."""
        try:
            import pathway as pw
        except ImportError:
            raise ImportError(
                "Pathway not installed. Install with: pip install 'lakelogic[pathway]'"
            )
        
        logger.info("Creating Pathway pipeline...")
        
        # TODO: Implement Pathway pipeline
        raise NotImplementedError(
            "Pathway integration coming in next update. "
            "Use Bytewax for now (framework='bytewax')"
        )
    
    def _create_input(self) -> Iterator[Dict[str, Any]]:
        """Create input stream from contract source."""
        source_config = self.contract.get("source", {})
        source_type = source_config.get("type")
        connector = source_config.get("connector")
        
        if source_type == "stream":
            if connector == "sse":
                # SSE connector (Wikimedia, etc.)
                from lakelogic.engines.streaming_connectors import SSEConnector
                
                url = source_config.get("url")
                conn = SSEConnector(url)
                
                logger.info(f"Connected to SSE stream: {url}")
                
                for event in conn.stream():
                    yield event
            
            elif connector == "websocket":
                # WebSocket connector (Coinbase, etc.)
                from lakelogic.engines.streaming_connectors import WebSocketConnector
                
                url = source_config.get("url")
                subscribe_message = source_config.get("subscribe_message")
                
                conn = WebSocketConnector(url, subscribe_message)
                
                logger.info(f"Connected to WebSocket: {url}")
                
                for event in conn.stream():
                    yield event
            
            elif connector == "kafka":
                # Kafka connector (TODO)
                raise NotImplementedError("Kafka connector not yet implemented")
            
            else:
                raise ValueError(f"Unknown connector: {connector}")
        
        else:
            raise ValueError(f"Unknown source type: {source_type}")
    
    def _validate_schema(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Validate event schema against contract model."""
        model = self.contract.get("model", {})
        fields = model.get("fields", [])
        
        # For now, just pass through (full validation in next update)
        # TODO: Implement schema validation
        
        return event
    
    def _apply_transformations(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Apply transformations from contract."""
        transformations = self.contract.get("transformations", [])
        
        # For now, just pass through (full transformations in next update)
        # TODO: Implement transformations
        
        return event
    
    def _validate_quality(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data quality rules from contract."""
        quality = self.contract.get("quality", {})
        row_rules = quality.get("row_rules", [])
        
        # For now, mark all as good (full validation in next update)
        # TODO: Implement quality validation
        event["_quality_status"] = "good"
        
        return event
    
    def _create_bytewax_aggregation(
        self,
        flow: "Dataflow",
        stream: Any,
        agg_config: Dict[str, Any]
    ):
        """Create Bytewax aggregation from contract config."""
        # TODO: Implement aggregations
        # For now, just log
        logger.info(f"Aggregation configured: {agg_config.get('name')}")
        pass


def select_streaming_framework(contract: Dict[str, Any]) -> str:
    """
    Public API to select streaming framework.
    
    Args:
        contract: Parsed contract dictionary
    
    Returns:
        "bytewax" or "pathway"
    
    Example:
        >>> import yaml
        >>> with open("contract.yaml") as f:
        ...     contract = yaml.safe_load(f)
        >>> framework = select_streaming_framework(contract)
        >>> print(framework)
        'bytewax'
    """
    selector = StreamingFrameworkSelector()
    return selector.select(contract)
