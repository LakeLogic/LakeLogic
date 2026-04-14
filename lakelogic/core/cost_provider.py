"""
Cost provider abstraction for pipeline cost observability.

Provides an engine-agnostic interface for attributing compute costs to
individual pipeline runs.  Each run gets a CostEstimate that flows into
the run log and the SaaS Observatory.

Providers
---------
- NoneCostProvider   : Returns zero cost (default — disabled)
- ManualCostProvider : Calculates from configured rates × duration
- DatabricksUCCostProvider : Queries system.billing.usage (v2)
- AzureMonitorCostProvider : Queries Azure Cost Management API (v2)

Configuration split
-------------------
Cost configuration is split between domain and system levels:

- **Domain** (``_domain.yaml``): ``cost.currency``, ``cost.budget`` — governance
- **System** (``_system.yaml``): ``cost.provider``, ``cost.rates``, ``cost.layer_rates`` — compute

During registry loading, the domain cost block is deep-merged into the
system cost block.  If the system defines a different ``currency`` than
the domain, a warning is logged and the domain currency takes precedence
for roll-ups and budget enforcement.

.. code-block:: yaml

    # _domain.yaml
    cost:
      currency: "USD"
      budget:
        daily_limit: 50.00

    # _system.yaml
    cost:
      provider: "manual"
      rates:
        dbu_per_hour: 0.22
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional

from loguru import logger

# ── Data structures ──────────────────────────────────────────────────────────


@dataclass
class CostEstimate:
    """Result of a cost estimation for a single pipeline entity run."""

    estimated_cost: float = 0.0
    currency: str = "USD"
    confidence: str = "none"  # "exact", "estimated", "none"
    attribution_method: str = "none"  # "direct", "duration_proportional", "row_proportional"


# ── Base class ───────────────────────────────────────────────────────────────


class CostProvider(ABC):
    """Abstract base for cost estimation providers."""

    @abstractmethod
    def estimate(
        self,
        run_id: str,
        duration_seconds: float,
        rows: int,
        domain: str = "",
        system: str = "",
        layer: str = "",
        **kwargs: Any,
    ) -> CostEstimate:
        """Estimate the cost of a pipeline run.

        Parameters
        ----------
        run_id : str
            Unique run identifier (may be used for billing API lookups).
        duration_seconds : float
            Wall-clock duration of the run.
        rows : int
            Number of rows processed (output rows).
        domain, system, layer : str
            Mesh topology context for the run.
        """
        ...


# ── Cluster scaling helpers ──────────────────────────────────────────────────


def _resolve_avg_nodes(cluster_config: Optional[Dict[str, Any]] = None) -> float:
    """Compute the estimated average node count from a cluster: config block.

    Returns 1.0 if no cluster config is provided (fixed single-node).

    Scaling assumptions:
    - ``"avg"``  : ``(min + max) / 2`` — most common default
    - ``"peak"`` : ``max`` — conservative worst-case
    - ``"min"``  : ``min`` — optimistic steady-state
    - ``"p75"``  : ``min + 0.75 * (max - min)`` — near-peak
    """
    if not cluster_config:
        return 1.0

    min_n = float(cluster_config.get("min_nodes", 1))
    max_n = float(cluster_config.get("max_nodes", min_n))
    assumption = (cluster_config.get("scaling_assumption") or "avg").lower()

    if assumption == "peak":
        return max_n
    elif assumption == "min":
        return min_n
    elif assumption == "p75":
        return min_n + 0.75 * (max_n - min_n)
    else:  # "avg" (default)
        return (min_n + max_n) / 2.0


# ── Concrete providers ──────────────────────────────────────────────────────


class NoneCostProvider(CostProvider):
    """Returns zero cost — used when cost tracking is disabled."""

    def estimate(self, **kwargs: Any) -> CostEstimate:
        return CostEstimate(estimated_cost=0.0, confidence="none", attribution_method="none")


class ManualCostProvider(CostProvider):
    """Duration × configured DBU rate × estimated node count.

    Suitable for environments where billing API access is unavailable.
    Uses ``cost.rates.dbu_per_hour`` from ``_system.yaml`` and optionally
    applies a ``cost.cluster`` scaling model to account for autoscaling
    clusters where node count varies during a run.

    Formula::

        cost = (duration_seconds / 3600) × dbu_per_hour × avg_nodes

    When ``layer_rates`` are configured, the layer-specific rate and cluster
    config take precedence over the base rate.
    """

    def __init__(
        self,
        dbu_per_hour: float = 0.22,
        currency: str = "USD",
        attribution: str = "duration_proportional",
        cluster_config: Optional[Dict[str, Any]] = None,
        layer_rates: Optional[Dict[str, Any]] = None,
    ):
        self.dbu_per_hour = dbu_per_hour
        self.currency = currency
        self.attribution = attribution
        self.cluster_config = cluster_config
        self.layer_rates = layer_rates or {}

    def _rate_for_layer(self, layer: str) -> tuple[float, float]:
        """Return (dbu_per_hour, avg_nodes) for a given layer."""
        layer_cfg = self.layer_rates.get(layer)
        if layer_cfg and isinstance(layer_cfg, dict):
            rate = float(layer_cfg.get("dbu_per_hour", self.dbu_per_hour))
            layer_cluster = layer_cfg.get("cluster", self.cluster_config)
            avg_nodes = _resolve_avg_nodes(layer_cluster)
        else:
            rate = self.dbu_per_hour
            avg_nodes = _resolve_avg_nodes(self.cluster_config)
        return rate, avg_nodes

    def estimate(
        self,
        duration_seconds: float = 0.0,
        rows: int = 0,
        layer: str = "",
        **kwargs: Any,
    ) -> CostEstimate:
        hours = duration_seconds / 3600.0
        rate, avg_nodes = self._rate_for_layer(layer)
        cost = round(hours * rate * avg_nodes, 6)
        return CostEstimate(
            estimated_cost=cost,
            currency=self.currency,
            confidence="estimated",
            attribution_method=self.attribution,
        )


class DatabricksUCCostProvider(CostProvider):
    """Queries ``system.billing.usage`` and joins on a run_id tag.

    Falls back to duration-proportional estimation if no tag match is found.
    This provider requires an active Spark session with access to the billing
    catalog.

    .. note:: This is a v2 provider — shipped as a placeholder in MVP.
    """

    def __init__(
        self,
        spark: Any = None,
        billing_table: str = "system.billing.usage",
        tag_key: str = "lakelogic_run_id",
        fallback_dbu_rate: float = 0.22,
        currency: str = "USD",
    ):
        self.spark = spark
        self.billing_table = billing_table
        self.tag_key = tag_key
        self.fallback_dbu_rate = fallback_dbu_rate
        self.currency = currency

    def estimate(
        self,
        run_id: str = "",
        duration_seconds: float = 0.0,
        **kwargs: Any,
    ) -> CostEstimate:
        if self.spark and run_id:
            try:
                result = self.spark.sql(f"""
                    SELECT SUM(usage_quantity) AS total_dbus
                    FROM {self.billing_table}
                    WHERE custom_tags['{self.tag_key}'] = '{run_id}'
                """).collect()
                if result and result[0]["total_dbus"] is not None:
                    dbus = float(result[0]["total_dbus"])
                    # Standard Databricks pricing: DBUs × list price
                    cost = round(dbus * self.fallback_dbu_rate, 6)
                    return CostEstimate(
                        estimated_cost=cost,
                        currency=self.currency,
                        confidence="exact",
                        attribution_method="direct",
                    )
            except Exception as exc:
                logger.debug(f"Databricks billing query failed, falling back to duration estimate: {exc}")

        # Fallback: duration-proportional
        hours = duration_seconds / 3600.0
        cost = round(hours * self.fallback_dbu_rate, 6)
        return CostEstimate(
            estimated_cost=cost,
            currency=self.currency,
            confidence="estimated",
            attribution_method="duration_proportional",
        )


# ── Factory ──────────────────────────────────────────────────────────────────


def resolve_cost_provider(cost_config: Optional[Dict[str, Any]] = None, spark: Any = None) -> CostProvider:
    """Create a CostProvider from a ``cost:`` config block.

    Parameters
    ----------
    cost_config : dict, optional
        The ``cost:`` block from ``_system.yaml``.
    spark : optional
        Active Spark session (needed for Databricks UC provider).

    Returns
    -------
    CostProvider
        Configured provider instance.
    """
    if not cost_config:
        return NoneCostProvider()

    provider_name = (cost_config.get("provider") or "none").lower()
    currency = cost_config.get("currency", "USD")
    attribution = cost_config.get("attribution", "duration_proportional")
    rates = cost_config.get("rates") or {}

    if provider_name == "none":
        return NoneCostProvider()

    if provider_name == "manual":
        return ManualCostProvider(
            dbu_per_hour=float(rates.get("dbu_per_hour", 0.22)),
            currency=currency,
            attribution=attribution,
            cluster_config=cost_config.get("cluster"),
            layer_rates=cost_config.get("layer_rates"),
        )

    if provider_name in ("databricks_uc", "databricks"):
        return DatabricksUCCostProvider(
            spark=spark,
            billing_table=cost_config.get("billing_table", "system.billing.usage"),
            tag_key=cost_config.get("billing_tag_key", "lakelogic_run_id"),
            fallback_dbu_rate=float(rates.get("dbu_per_hour", 0.22)),
            currency=currency,
        )

    logger.warning(f"Unknown cost provider '{provider_name}', falling back to NoneCostProvider")
    return NoneCostProvider()


def validate_cost_currency(
    domain_cost_config: Optional[Dict[str, Any]] = None,
    system_cost_config: Optional[Dict[str, Any]] = None,
) -> str:
    """Validate currency consistency between domain and system cost configs.

    The domain's ``cost.currency`` is authoritative for budget enforcement
    and Observatory roll-ups.  If the system defines a different currency,
    a warning is logged and the domain currency is returned.

    Parameters
    ----------
    domain_cost_config : dict, optional
        The ``cost:`` block from ``_domain.yaml``.
    system_cost_config : dict, optional
        The ``cost:`` block from ``_system.yaml``.

    Returns
    -------
    str
        The authoritative currency code to use for this context.
    """
    domain_currency = (domain_cost_config or {}).get("currency")
    system_currency = (system_cost_config or {}).get("currency")

    if domain_currency and system_currency:
        if domain_currency != system_currency:
            logger.warning(
                f"Cost currency mismatch: system uses '{system_currency}' "
                f"but domain mandates '{domain_currency}'. "
                f"Domain currency will be used for roll-ups and budget enforcement."
            )
        return domain_currency

    # Fallback: domain > system > default
    return domain_currency or system_currency or "USD"


def check_budget_threshold(
    estimated_cost: float,
    cost_config: Dict[str, Any],
    entity: str = "",
    historical_avg: Optional[float] = None,
) -> Optional[str]:
    """Check if an estimated cost exceeds budget thresholds.

    Returns a warning message if a threshold is breached, or None if OK.
    This does NOT raise — the caller decides whether to log, alert, or fail.

    Parameters
    ----------
    estimated_cost : float
        The cost estimate for the current run.
    cost_config : dict
        The merged cost config (domain + system).
    entity : str
        Entity name for the warning message.
    historical_avg : float, optional
        Historical average cost for this entity (for anomaly detection).
    """
    budget = cost_config.get("budget") or {}
    if not budget:
        return None

    anomaly_mult = budget.get("per_run_anomaly_multiplier")
    if anomaly_mult and historical_avg and historical_avg > 0:
        if estimated_cost > historical_avg * anomaly_mult:
            return (
                f"Cost anomaly: {entity or 'entity'} run cost "
                f"${estimated_cost:.4f} exceeds {anomaly_mult}× "
                f"historical average ${historical_avg:.4f}"
            )

    return None
