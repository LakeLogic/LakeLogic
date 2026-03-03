# Changelog

All notable changes to **LakeLogic** are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.11.0] — 2026-03-03

### Added
- **PII masking** — two modes available out of the box:
  - *Whole-column masking*: zero dependencies, instant, covers structured fields (email, phone, NI number, etc.)
  - *NLP span masking*: Microsoft Presidio + spaCy for unstructured text fields — detects and redacts entities within free-text at the token level
- HIPAA compliance tutorial — demonstrates a complete ingest pipeline with PII masking and quarantine routing
- GDPR compliance tutorial — right-to-erasure pattern and field-level consent tagging
- PII masking playbook notebook with worked examples for both masking modes

---

## [0.10.0] — 2026-03-01

### Added
- **Full engine integration layer** — Polars, Spark (Databricks), DuckDB, Snowflake, and BigQuery all operate from the same contract YAML with no code changes between engines
- Interactive example notebooks synced automatically to the documentation site via `sync_examples.py`
- `DataProcessor` now supports end-to-end Bronze → Silver → Gold medallion pipeline orchestration via `PipelineDriver`

---

## [0.9.0] — 2026-03-01

### Added
- **Core materialization module** — write validated output to any target format (Delta, Parquet, CSV, DuckDB table) directly from a contract run
- `01_hello_world.ipynb` — quickstart notebook for remote data ingestion, runnable in under 5 minutes on any machine or in Google Colab
- `02_database_governance.ipynb` — end-to-end governance playbook with schema enforcement, quarantine, and lineage tracking
- dbt PII quality example notebook — applying data contracts as a quality gate over dbt-transformed outputs
- MkDocs navigation updated with new interactive examples section

---

## [0.8.0] — 2026-02-28

### Added
- **BigQuery engine adapter** — validate and process data directly in BigQuery using native SQL, no data movement required
- **Snowflake engine adapter** — same contract, runs natively against Snowflake tables
- `DependencyManager` utility — gracefully handles optional engine packages; LakeLogic installs without requiring all engine dependencies upfront

---

## [0.7.0] — 2026-02-28

### Added
- **Polars engine** — Polars-native validation and processing throughout; no Pandas overhead. Validates millions of rows locally in seconds
- AI contract enrichment examples — enrich an inferred contract with LLM-suggested quality rules
- HIPAA/GDPR compliance examples added to `/examples/compliance/`
- Installation documentation — covers pip, Poetry, and Conda setup; optional engine extras documented

---

## [0.6.0] — 2026-02-28

### Added
- **GitHub Actions CI pipeline** (`ci-gate.yml`) — Ruff lint and pytest with coverage on every push and PR
- **PyPI publish workflow** — automated release to PyPI on version tag; `pip install lakelogic` now available
- Documentation sync script — keeps example notebooks in `/docs/` in sync with source in `/examples/`

---

## [0.5.0] — 2026-02-28

### Added
- **Schema API** — programmatic access to contract schema definitions; introspect field types, rules, and policies at runtime
- Quickstart examples for all major data sources (CSV, Parquet, Delta, NDJSON, Excel, cloud storage)
- Core pattern examples — deduplication, SCD2 dimension, reference join validation, soft-delete
- Advanced workflow examples — multi-domain governance, shared contract registries, cross-engine lineage
- Compliance example library — HIPAA, GDPR, SOX patterns with worked contract definitions

---

## [0.4.0] — 2026-02-28

### Added
- **AI-powered contract enrichment** — drop any file into `enrich_contract()` and an LLM suggests quality rules based on the data's actual value distributions
- LLM provider abstraction — works with OpenAI, Azure OpenAI, or any OpenAI-compatible endpoint; swap providers without changing contract code

---

## [0.3.0] — 2026-02-28

### Added
- **CLI** — `lakelogic run`, `lakelogic generate`, `lakelogic bootstrap`, `lakelogic import-dbt`, `lakelogic setup-oss`
- **Universal notification system** — integrates with Slack, Teams, email, and 50+ services via Apprise; configurable per-contract with Jinja2 alert templates
- `lakelogic setup-oss` — pre-installs DuckDB extensions (`iceberg`, `delta`, `httpfs`, `aws`, `azure`) in one command

---

## [0.2.0] — 2026-02-27

### Added
- Quickstart examples for data ingestion across CSV, Parquet, Delta, JSON, and cloud storage formats — each paired with a contract YAML and a notebook
- Documentation: notifications & secrets management guide, playbook format reference, main index
- Architecture documentation — medallion lakehouse pattern, contract lifecycle, engine selection guide
- Comparison guide — LakeLogic vs Great Expectations, Soda Core, dbt tests

---

## [0.2.0b0] — 2026-02-26

### Added
- **Contract inference** — `infer_contract()` bootstraps a full YAML contract from any file; detects schema, null patterns, and suggests quality rules from data distributions
- **dbt adapter** — `lakelogic import-dbt` converts `schema.yml` / `sources.yml` to LakeLogic contracts; existing dbt projects get quality enforcement without rewriting tests
- Multi-engine support architecture — single contract file runs against Polars, DuckDB, Spark, Snowflake, or BigQuery; engine selected at runtime
- Databricks deployment configuration — Terraform module and bundle config for deploying LakeLogic pipelines on Databricks

---

## [0.1.0] — 2026-02-22

### Added
- **Initial public release of LakeLogic OSS**
- `DataContract` — Pydantic model defining schema, quality rules, transformations, SLOs, materialization targets, and PII policy in a single YAML
- `DataProcessor` — validates any file or DataFrame against a contract at ingest; bad rows routed to quarantine with a per-row `_reject_reason` column, never silently dropped
- `DataGenerator` — produces realistic synthetic rows from a contract; seed from a real file's distributions for high-fidelity test data
- `ValidationResult` — structured output with pass/fail counts, quarantine path, run ID, and lineage metadata
- **DuckDB engine** — fully local, zero-infrastructure validation for development and CI pipelines
- Adapter stubs for Polars, Pandas, Spark, Snowflake, and BigQuery — engine interface defined, implementations to follow in subsequent releases
- `lakelogic bootstrap` — scans a landing zone directory and generates a starter contract for each file found
- `PipelineDriver` — orchestrates multi-contract bronze/silver/gold pipelines from a single configuration
- Initial documentation site (MkDocs) with quickstart, API reference, and architecture guide

---

<!-- Link definitions -->
[Unreleased]: https://github.com/lakelogic/LakeLogic/compare/v0.11.0...HEAD
[0.11.0]: https://github.com/lakelogic/LakeLogic/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/lakelogic/LakeLogic/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/lakelogic/LakeLogic/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/lakelogic/LakeLogic/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/lakelogic/LakeLogic/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/lakelogic/LakeLogic/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/lakelogic/LakeLogic/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/lakelogic/LakeLogic/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/lakelogic/LakeLogic/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/lakelogic/LakeLogic/compare/v0.2.0b0...v0.2.0
[0.2.0b0]: https://github.com/lakelogic/LakeLogic/compare/v0.1.0...v0.2.0b0
[0.1.0]: https://github.com/lakelogic/LakeLogic/releases/tag/v0.1.0
