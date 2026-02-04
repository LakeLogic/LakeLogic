import yaml
from typing import Any, Tuple, Union, Dict, Type
from pathlib import Path # Kept this import as it's used in _load_contract

from lakeguard.core.models import DataContract
from lakeguard.engines.base import EngineAdapter
from lakeguard.engines.polars import PolarsAdapter
from lakeguard.engines.duckdb import DuckDBAdapter
from lakeguard.engines.pandas import PandasAdapter
from lakeguard.engines.spark import SparkAdapter
from lakeguard.notifications.base import get_notification_adapter
from loguru import logger

class DataProcessor:
    """
    The main entry point for running LakeGuard contracts.
    
    This class handles contract loading and dispatches the data processing
    to the appropriate engine adapter.
    """

    def __init__(self, engine: str, contract: Union[str, Path, dict, DataContract]):
        """
        Initialize the DataProcessor.
        
        Args:
            engine: The execution engine to use ('polars', 'pandas', 'duckdb', 'spark').
            contract: The Data Contract definition (path to YAML, dict, or DataContract object).
        """
        self.engine_name = engine.lower()
        self.contract = self._load_contract(contract)
        self.adapter = self._get_adapter()

    def _get_adapter(self) -> EngineAdapter:
        """
        Instantiates the correct adapter based on the engine name.
        """
        if self.engine_name == "polars":
            from lakeguard.engines.polars import PolarsAdapter
            return PolarsAdapter(self.contract)
        elif self.engine_name == "pandas":
            from lakeguard.engines.pandas import PandasAdapter
            return PandasAdapter(self.contract)
        elif self.engine_name == "duckdb":
            from lakeguard.engines.duckdb import DuckDBAdapter
            return DuckDBAdapter(self.contract)
        elif self.engine_name in ["spark", "pyspark"]:
            from lakeguard.engines.spark import SparkAdapter
            return SparkAdapter(self.contract)
        else:
            raise ValueError(f"Unsupported engine: {self.engine_name}")

    def _load_contract(self, contract: Union[str, Path, dict, DataContract]) -> DataContract:
        """
        Loads the contract from various formats into a DataContract object.
        """
        if isinstance(contract, DataContract):
            return contract
        if isinstance(contract, dict):
            return DataContract(**contract)
        
        path = Path(contract)
        if not path.exists():
            raise FileNotFoundError(f"Contract file not found: {path}")
        
        with open(path, "r") as f:
            data = yaml.safe_load(f)
            return DataContract(**data)

    def run(self, df: Any) -> Tuple[Any, Any]:
        """
        Runs the contract against the provided dataframe.
        """
        logger.info(f"🛡️  Starting LakeGuard run [Engine: {self.engine_name}, Contract: {self.contract.name}]")
        
        # 1. Load and Register Links (Reference Tables)
        # In a real implementation, this would load from S3/DB
        # For now, we allow the adapter to handle the registration
        
        # 2. Execute
        good_df, bad_df = self.adapter.execute(df)
        
        total = 0
        bad = 0
        try:
            total = len(df)
            bad = len(bad_df)
            logger.info(f"✅ Run complete. Total: {total}, Quarantined: {bad}")
        except:
            pass

        if bad > 0:
            self.notify(
                event="quarantine",
                message=f"🛡️ LakeGuard Alert: {bad} records quarantined in '{self.contract.name or 'unknown'}'. Source contains {total} total records."
            )
            
        return good_df, bad_df

    def notify(self, event: str, message: str):
        """
        Sends notifications based on contract configuration.
        """
        if not self.contract.quarantine or not self.contract.quarantine.notifications:
            return

        for notif in self.contract.quarantine.notifications:
            if event in notif.on_events:
                try:
                    adapter = get_notification_adapter(notif.type, {"target": notif.target})
                    adapter.send(message, subject=f"LakeGuard {event.capitalize()} Alert")
                except Exception as e:
                    logger.error(f"Failed to send notification: {e}")
