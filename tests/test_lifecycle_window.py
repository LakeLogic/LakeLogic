from typing import Any, Tuple

from lakeguard.core.models import DataContract, Quality, RowRuleLifecycleWindow
from lakeguard.engines.base import EngineAdapter


class DummyAdapter(EngineAdapter):
    def execute(self, df: Any) -> Tuple[Any, Any]:
        return df, df


def test_lifecycle_window_rule_expansion() -> None:
    contract = DataContract(
        version="1.0.0",
        dataset="events",
        quality=Quality(
            row_rules=[
                RowRuleLifecycleWindow(
                    lifecycle_window={
                        "event_ts": "event_ts",
                        "event_key": "subscriber_id",
                        "reference": "subscribers",
                        "reference_key": "subscriber_id",
                        "start_field": "start_date",
                        "end_field": "end_date",
                        "end_default": "9999-12-31",
                    }
                )
            ]
        ),
    )
    adapter = DummyAdapter(contract)
    adapter.engine_name = "polars"
    rules = adapter.get_row_rules()
    assert len(rules) == 1
    assert "lifecycle_window" in rules[0].name
    assert "subscribers" in rules[0].sql
