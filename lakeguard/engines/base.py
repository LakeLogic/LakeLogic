from abc import ABC, abstractmethod
from typing import Any, Tuple, List, Dict, Optional
from lakeguard.core.models import DataContract, QualityRule

class EngineAdapter(ABC):
    """
    Abstract Base Class for all execution engines.
    """
    
    ERROR_COLUMN = "_lakeguard_errors"
    CATEGORY_COLUMN = "_lakeguard_categories"

    def __init__(self, contract: DataContract):
        self.contract = contract

    @abstractmethod
    def execute(self, df: Any) -> Tuple[Any, Any]:
        """
        Primary execution method: Validates and Transforms data.
        Returns: (good_df, bad_df)
        """
        pass

    def get_row_rules(self) -> List[QualityRule]:
        """
        Returns all rules that should trigger row-level quarantine.
        """
        if self.contract.quality and self.contract.quality.row_rules:
            return self.contract.quality.row_rules
        return []

    def get_dataset_rules(self) -> List[QualityRule]:
        """
        Returns all rules that are aggregate/metric based.
        """
        if self.contract.quality and self.contract.quality.dataset_rules:
            return self.contract.quality.dataset_rules
        return []
