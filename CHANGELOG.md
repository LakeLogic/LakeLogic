# Changelog

All notable changes to **LakeLogic** are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---
## [1.2.0] — 2026-03-08

### Added

- **GenericSQLAdapter**: push-down validation, DDL generation, and CTAS materialization for any DB-API 2.0 database via `sqlglot` transpilation — supports PostgreSQL, MySQL, BigQuery, Snowflake, Redshift, ClickHouse, SQL Server, Oracle, Trino, and Databricks
- **DML operations**: `merge()`, `insert_validated()`, `update_where()`, `delete_where()`, and `reprocess_quarantine()` methods on `GenericSQLAdapter` for incremental loading and data lifecycle management
- **Schema evolution**: `sync_schema()` compares contract fields against live table columns, generates `ALTER TABLE ADD COLUMN` statements; `alter_add_column()` and `alter_drop_column()` for manual column management
- **Tier normalization**: `TIER_CANONICAL_MAP` normalizes 20+ naming conventions (raw→bronze, staging→silver, curated→gold, masterdata→reference) to canonical medallion tiers at parse time
- **Tier field on `DataContract`**: new `tier` field with `AliasChoices` accepting `tier`, `layer`, or `target_layer` keys — backward compatible with `info.target_layer`
- **Contract validation for tier**: `validate_contract()` warns when tier is missing and validates against known naming conventions
- **Downstream consumers**: `DownstreamConsumer` model and `downstream` list on `DataContract` for end-to-end lineage tracking from source → gold → dashboard/report/API/ML model, with `columns_used` for column-level lineage
- **Downstream validation**: `validate_contract()` checks downstream consumer structure, warns on unknown types and platforms
- **`TransformationFilter` string shorthand**: `filter: "order_id IS NOT NULL"` now accepted as shorthand for `filter: {sql: "..."}`
- **`TransformationDeduplicate` `by` alias**: `deduplicate.by` accepted as alias for `deduplicate.on` for backward compatibility
- **`ENGINE_DIALECT_MAP`**: 17 engine-to-dialect mappings exposed via `lakelogic.engines.base`
- **`_transpile()` method**: centralized sqlglot transpilation on `EngineAdapter` base class, replacing manual if/else branching in `_regex_sql()`
- **Test suite**: `test_tier_and_filter.py` with 37 test cases covering tier normalization, filter shorthand, dedup alias, transpilation, dialect map, and validation

### Fixed

- **Deprecated `datetime.utcnow()`**: replaced all occurrences in `polars.py` and `incremental.py` with `datetime.now(timezone.utc)` (Python 3.12+ compatibility)

## [1.1.0] — 2026-03-07

### Added

- Add core materialization utilities for data persistence, including Spark table management, path resolution, and dataframe output.
## [1.0.0] — 2026-03-05

### Added

- Add Spark engine adapter for data contract execution and transformation.
- Add Spark engine row-level validation and engine-agnostic row counts, implement automated changelog and release workflows, and fix Spark `.isFalse()` bug and CHANGELOG structure.
- Implement core processor for remote data ingestion, add quickstart examples, and set up automated changelog generation.
- Introduce Spark execution engine for data contract validation and transformation.
- Add "Hello World" example notebook for remote data ingestion and quality validation, and update the changelog.
## [0.11.0] — 2026-03-03

### Added

- Add PII masking hook with direct and NLP-based replacement modes, and refine changelog documentation.
- Add tutorials for HIPAA, GDPR compliance, and PII masking.
- Add tutorials demonstrating HIPAA/GDPR compliance and PII masking.
- Introduce the foundational LakeLogic data processing framework, including core modules, engine integrations, AI, and CLI.
- Add script to synchronize flagship examples to the documentation directory.
- Add a script to synchronize flagship examples to the documentation and update MkDocs navigation to feature new interactive examples.
- Add new core materialization module, comprehensive examples, and initial documentation, while updating the main README.
- Add 01_hello_world.ipynb quickstart example for remote data ingestion.
- Add quickstart examples for remote data ingestion, database governance, and dbt PII quality, supported by new core materialization and models.
- Add BigQuery and Snowflake engine adapters along with a dependency management utility for optional packages.

### Documentation

- Add documentation index page
## [0.7.0] — 2026-02-28

### Added

- Add Polars engine, new examples for HIPAA/GDPR compliance and AI contract enrichment, and installation documentation.
## [0.6.0] — 2026-02-28

### Added

- Add CI quality gate and PyPI publish workflows, and update documentation examples via a new sync script.
- Add extensive examples for quickstart, core patterns, advanced workflows, and compliance, alongside new documentation and a schema API.
## [0.4.0] — 2026-02-28

### Added

- Add AI-powered contract enrichment functionality with LLM provider abstraction.
## [0.3.0] — 2026-02-28

### Added

- Implement the initial command-line interface for contract execution, output management, and environment setup.
- Implement a universal notification system using Apprise with new Jinja2 templates and a base adapter.
- Add quickstart examples for data ingestion across various file formats using contracts and notebooks.
- Add new documentation for notifications & secrets, playbooks, and a main index, and update mkdocs navigation.
- Add extensive documentation including comparison, installation, and architecture guides, update project branding, and introduce core processor logic.
- Introduce new examples covering advanced workflows and compliance/governance scenarios, including data, contracts, and notebooks.
- Implement initial LakeLogic framework with contract inference, multi-engine support, and Databricks deployment configuration.
- Establish core data contract models and initial data processing infrastructure with engine support.
- Implement contract inference from data files, add dbt adapter, schema API, and advanced workflow examples.

### Documentation

- Add initial documentation index page.
- Add `docs/index.md` and correct capitalization of 'LakeLogic' in `mkdocs.yml` URLs and repository names.
## [0.1.0] — 2026-02-22

### Added

- Add extensive examples for data sources, core patterns, and advanced workflows, along with new core engine and CLI components.
- Add extensive examples for data sources, core patterns, and advanced workflows, along with new data engines and core utilities.
- Introduce new quickstart examples for remote data ingestion and database governance, add notebook cleaning utilities, and expand installation documentation.
- Implement comprehensive documentation site with custom styling and introduce a base engine adapter for data quality rule processing with updated quickstart examples.
- Add final_cleanup.py script to remove specific example files and directories.
- Introduce core data processing logic with engine auto-discovery, `ValidationResult`, and new DuckDB engine, alongside comprehensive examples for data sources and compliance.
- Add contract templating tool and a comprehensive advanced workflow example for shared governance at scale.
- Add comprehensive examples for quickstarts, core patterns, data sources, cloud platforms, orchestration, advanced workflows, and production scenarios.
- Introduce core data materialization logic and initial streaming components.
- Introduce comprehensive examples and tutorials for databases, streaming, APIs, and cloud platforms, along with new streaming implementation documentation.
- Introduce extensive new features, examples, and documentation for data integration, streaming, and cataloging, including detailed logging configuration.
- Add a comprehensive contract template reference documentation page and update mkdocs navigation.
- Implement initial Azure infrastructure with Terraform modules for dev, test, and prod environments, including CI/CD workflows and documentation.

### CI/CD

- Add GitHub Actions workflow for backend Python quality checks including linting and formatting.

### Documentation

- Add comprehensive documentation for DataProcessor return values and update mkdocs navigation.
- Add architecture diagram documentation and ignore the `.product_vision` directory.
## [0.1.0b1] — 2026-02-08

### Added

- Establish initial project structure with core logic, CLI, multiple engines, comprehensive documentation, examples, and a test suite.
- Introduce comprehensive documentation, examples, and support for multiple data engines, core logic, and CLI functionalities.
- Add PyPI publishing workflow and comprehensive MkDocs documentation site
- Add GitHub Actions workflow for publishing to PyPI.
- Add basic validation contract for the `silver_crm_customers` dataset example.
- Introduce core Lakeguard framework with multiple data engines, comprehensive examples, and extensive documentation.
- Add comprehensive examples, tutorials, documentation, and new engine implementations for various data platforms.

### CI/CD

- Add GitHub Actions workflow to build and publish Python packages to PyPI.
---

<!-- Link definitions -->
[1.2.0]: https://github.com/lakelogic/LakeLogic/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/lakelogic/LakeLogic/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/lakelogic/LakeLogic/compare/v0.11.0...v1.0.0
[0.11.0]: https://github.com/lakelogic/LakeLogic/compare/v0.7.0...v0.11.0
[0.7.0]: https://github.com/lakelogic/LakeLogic/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/lakelogic/LakeLogic/compare/v0.4.0...v0.6.0
[0.4.0]: https://github.com/lakelogic/LakeLogic/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/lakelogic/LakeLogic/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/lakelogic/LakeLogic/compare/v0.1.0b1...v0.1.0
[0.1.0b1]: https://github.com/lakelogic/LakeLogic/releases/tag/v0.1.0b1

