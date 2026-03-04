# Changelog

All notable changes to **LakeLogic** are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.12.0] — 2026-03-03

### Added
- **Hello World quickstart notebook** (`01_hello_world.ipynb`) — remote data ingestion and quality validation end-to-end in a single notebook
- **Spark engine** — full row-rule evaluation and quarantine routing via PySpark Column expressions; supports classic and Serverless (Spark Connect) runtimes
- **PII masking hook** — dual-mode: zero-dependency whole-column masking for structured fields; NLP-based span masking via Microsoft Presidio + spaCy for unstructured text
- **HIPAA / GDPR compliance tutorials** — worked examples showing PII masking in healthcare and EU data contexts
- Adapter stubs promoted to full implementations: **Polars**, **Pandas**, **Spark** engines production-ready

---

## [0.11.0] — 2026-03-03

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

## [0.10.0] — 2026-02-28

### Added
- **BigQuery and Snowflake engine adapters** along with a dependency management utility for optional packages
- Quickstart examples for remote data ingestion, database governance, and dbt PII quality

---

## [0.7.0] — 2026-02-28

### Added
- **Polars engine** — full row-rule evaluation and quarantine routing via native Polars expressions
- New examples for HIPAA/GDPR compliance and AI contract enrichment
- Installation documentation

---

## [0.6.0] — 2026-02-28

### Added
- **CI quality gate** and **PyPI publish** GitHub Actions workflows
- Extensive examples for quickstart, core patterns, advanced workflows, and compliance
- New documentation and a schema API (`lakelogic schema`)

---

## [0.4.0] — 2026-02-28

### Added
- **AI-powered contract enrichment** — LLM provider abstraction for auto-annotating contracts with descriptions, tags, and quality rule suggestions

---

## [0.3.0] — 2026-02-28

### Added
- `lakelogic` CLI — contract execution, output management, environment setup
- **Universal notification system** using Apprise with Jinja2 templates and a base adapter
- Quickstart examples for data ingestion across CSV, Parquet, JSON, Excel
- Documentation for notifications & secrets, playbooks, architecture comparison, and installation guide
- Introduced new examples covering advanced workflows and compliance/governance scenarios

---

## [0.1.0] — 2026-02-22

### Added
- Core `DataContract`, `DataProcessor`, `DataGenerator`, and `ValidationResult` models
- DuckDB engine with full SQL-based row-rule evaluation
- Contract inference (`infer_contract`) from data files
- dbt adapter, schema API, and advanced workflow examples
- Comprehensive documentation site with custom styling
- Initial Azure infrastructure via Terraform modules for dev/test/prod environments

---

## [0.1.0b1] — 2026-02-08

### Added
- Initial project structure: core logic, CLI, multiple engines, documentation, examples, and test suite
- PyPI publishing workflow
- Basic validation contract for the `silver_crm_customers` dataset example
- Introduced Lakeguard (now LakeLogic) framework with multi-engine support

---

<!-- Link definitions -->
[Unreleased]: https://github.com/lakelogic/LakeLogic/compare/v0.12.0...HEAD
[0.12.0]: https://github.com/lakelogic/LakeLogic/compare/v0.11.0...v0.12.0
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
