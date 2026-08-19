![LakeLogic Core: execute Open Lakehouse Contracts across Polars, DuckDB and Spark from local development through CI to production](docs/assets/lakelogic_core_banner.png)

# LakeLogic

**The open-source reference framework for Open Lakehouse Contracts.**

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://LakeLogic.github.io/LakeLogic/)
[![PyPI](https://img.shields.io/pypi/v/lakelogic?logo=pypi&logoColor=white)](https://pypi.org/project/lakelogic/)
[![CI](https://github.com/LakeLogic/LakeLogic/actions/workflows/ci-gate.yml/badge.svg)](https://github.com/LakeLogic/LakeLogic/actions/workflows/ci-gate.yml)
[![codecov](https://codecov.io/gh/LakeLogic/LakeLogic/graph/badge.svg)](https://codecov.io/gh/LakeLogic/LakeLogic)
[![Python](https://img.shields.io/badge/python-3.10+-blue?logo=python&logoColor=white)](https://www.python.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)

Validate and execute OLC contracts across Polars, DuckDB and Spark, from local development through CI and production pipelines.

LakeLogic is the open-source reference framework for the [**Open Lakehouse Contract (OLC)**](https://lakelogic.github.io/open-lakehouse-contract/) — the open, engine-neutral standard for executable data contracts. Describe a data product's sources, schema, ownership, quality rules, PII handling, lineage, service levels (SLAs and SLOs), transformations and materialization in a portable OLC `.yaml` document, then run that contract identically with Polars, DuckDB, or Spark.

Use the same contract to get fast feedback locally, check changes in CI/CD, and govern pipeline execution in your lakehouse. Records that fail row-level rules can be retained with their failure reasons instead of being silently discarded.

[**Run the five-minute Colab quickstart**](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/00_quickstart.ipynb) · [**Read the documentation**](https://lakelogic.github.io/LakeLogic/) · [**Browse the examples**](https://lakelogic.github.io/LakeLogic/examples.html)

![LakeLogic Architecture](docs/assets/lakelogic_architecture.png)

## The Problem It Solves

Data teams repeatedly rebuild the same controls in notebooks and pipelines:

- schema checks and business rules;
- accepted and quarantined outputs;
- PII handling and lineage metadata;
- incremental processing and materialization;
- deployment checks for contract changes.

LakeLogic puts those expectations in a version-controlled contract and provides the execution machinery around it. Business meaning stays visible in YAML; complex transformations can remain in normal, testable Python or SQL.

## Quick Start

Install the base package:

```bash
pip install lakelogic
```

The fastest complete introduction is the [Google Colab quickstart](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/00_quickstart.ipynb). It creates sample data, executes a contract, and shows accepted and quarantined records without requiring a local Spark environment.

A contract starts with the fields and rules that matter to the data product:

```yaml
version: 1.0.0
dataset: orders

info:
  title: E-Commerce Orders
  owner: data-team@company.com
  target_layer: silver

model:
  fields:
    - name: order_id
      type: integer
      required: true
    - name: customer_email
      type: string
      required: true
      pii: true
      masking: partial
    - name: amount
      type: float
      required: true

quality:
  row_rules:
    - name: valid_email
      sql: "customer_email LIKE '%@%.%'"
    - name: positive_amount
      sql: "amount > 0"
```

Run it through the Python API:

```python
from lakelogic import DataProcessor

processor = DataProcessor("orders_contract.yaml", engine="polars")
result = processor.run_source("orders.csv")

print(f"Accepted: {result.good_count}")
print(f"Quarantined: {result.bad_count}")
print(f"Quality score: {result.quality_score:.1f}")
```

For a validation run, LakeLogic returns the accepted and quarantined rows with counts and diagnostic context. Materialization, alerts, retention, and catalog behaviour depend on the contract, selected engine, and connected infrastructure.

To check a contract before deployment:

```bash
lakelogic validate \
  --contract orders_contract.yaml \
  --gates breaking_change,pii_classification,lineage_break
```

CI gates analyse contract declarations and the comparison context supplied to them. Add the command to your pull-request workflow to reject a change when a configured gate fails.

## Core Capabilities

| Capability | What LakeLogic provides |
| :--- | :--- |
| **Executable contracts** | Strictly parsed YAML for schemas, row rules, dataset rules, service levels, lineage, and materialization. |
| **Quality and quarantine** | Accepted and failed records, rule-level diagnostics, run counts, and quality scores. |
| **Contract checks in CI/CD** | Static gates for breaking schema changes, PII declarations, and lineage changes when the required comparison context is available. |
| **Multiple execution engines** | A common contract model across Polars, DuckDB, and Spark, with documented engine-specific boundaries. |
| **Lakehouse patterns** | Incremental processing, Delta or Iceberg outputs, merge strategies, SCD Type 2, dependencies, and external transformation logic. |
| **Operational evidence** | Structured run logs, execution metadata, lineage evidence, and optional notification integrations. |

See the [complete capability matrix](docs/capabilities.md) before choosing an engine or storage format.

## Engine Support

| Engine | Best suited to | Installation and boundaries |
| :--- | :--- | :--- |
| **Polars** | Local development, notebooks, CI, and fast single-node processing | Included in the base package. Delta support uses delta-rs. |
| **DuckDB** | Local analytical SQL and embedded workflows | Included in the base package. Some catalog and materialization combinations differ from Spark. |
| **Spark** | Distributed lakehouse workloads and managed catalogs such as Unity Catalog | Install with `pip install "lakelogic[spark]"`. Managed catalog features depend on the Spark platform and its configuration. |

The contract model is shared, but engines are not identical. Review [engine and format capabilities](docs/capabilities.md) for supported combinations.

## Where Data Mesh Fits

LakeLogic can provide shared contract machinery for a data mesh while domain teams retain ownership of business meaning.

| Data-mesh principle | LakeLogic's role |
| :--- | :--- |
| **Domain ownership** | Domain teams version contracts alongside the data products they own. |
| **Data as a product** | Contracts make schemas, rules, service expectations, and dependencies explicit. |
| **Self-service platform** | Teams reuse common validation and execution interfaces across supported engines. |
| **Federated governance** | Platform standards can be expressed as shared defaults and checked alongside domain-specific rules. |

LakeLogic does not create organisational ownership, access policies, alert delivery, or regulatory compliance by itself. It supplies contract declarations, runtime controls, and evidence that can participate in those wider systems.

## Learn by Doing

| Guide | Use it to explore |
| :--- | :--- |
| [Quickstart](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/00_quickstart.ipynb) | Your first contract, generated data, validation, and quarantine. |
| [Data Quality and Trust](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/01_data_quality_trust.ipynb) | Schema rules, business rules, reconciliation, and medallion flows. |
| [Compliance and Governance](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/02_compliance_governance.ipynb) | PII-handling and governance patterns that must be combined with organisational controls. |
| [Engine and Scale](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/03_engine_scale.ipynb) | Polars, DuckDB, Spark, incremental execution, and dimensional modelling. |
| [Developer Experience](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/04_developer_experience.ipynb) | Validation, diagnostics, CI/CD, and development workflows. |
| [Data Generation and AI](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/05_data_generation_ai.ipynb) | Synthetic test data and optional AI-assisted workflows. |
| [Integrations](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/06_integrations.ipynb) | dbt, dlt, databases, streaming sources, and notifications. |

## Documentation

- [Installation and optional dependencies](docs/installation.md)
- [Complete contract reference](docs/contract_template.md)
- [Capabilities and engine boundaries](docs/capabilities.md)
- [Pipeline concepts](docs/pipelines.md)
- [Reconciliation](docs/reconciliation.md)
- [Notifications](docs/notifications.md)
- [Full documentation site](https://lakelogic.github.io/LakeLogic/)

## Contributing

Contributions and issue reports are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow and [developer installation](docs/installation.md#developer-installation) for environment setup.

## License

LakeLogic is available under the [Apache 2.0 License](LICENSE).
