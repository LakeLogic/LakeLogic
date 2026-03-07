# Changelog

All notable changes to **LakeLogic** are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.1.0] — 2026-03-07

### Added

- **materialization**: Add `table_properties` field to `Materialization` model for Spark/Databricks `TBLPROPERTIES`
- **ddl**: Emit `TBLPROPERTIES` clause in `generate_ddl()` for Spark and Databricks backends
- **materialization**: Auto-apply column comments from contract `model.fields[].description` after Spark table writes
- **materialization**: Add `_spark_apply_table_metadata()` to propagate column comments and table properties post-write
- **materialization**: Add `optimize_delta()` for serverless compaction and vacuum without Spark (delta-rs)
- **materialization**: Add contract-driven auto-compaction via `compaction.auto` config

### Changed

- **release**: Updated `release.bat` with conventional commit examples and AI-commit guidance
- **release**: Fixed tag orphaning in release flow — script now re-tags after amending

## [1.0.0] — 2026-03-05

### Added

- Spark engine: row-level quality validation via PySpark Column expressions
- Engine-agnostic row counts: `result.source_count`, `.good_count`, `.bad_count` properties on `ValidationResult` — works across Polars, Pandas, Spark, and DuckDB
- git-cliff changelog: automated changelog generation from conventional commits with Keep a Changelog format
- Release automation: `release.bat` script and GitHub Action for one-command releases
- Spark execution engine: full adapter for PySpark with row-rule evaluation, schema enforcement, quarantine routing, and transformation support
- Multi-engine Hello World: `01_hello_world.ipynb` now supports `ENGINE = 'polars' | 'duckdb' | 'pandas' | 'spark'` from a single notebook
- Spark URL workaround: automatic download-to-local-file when Spark engine is selected (Spark can't read HTTPS URLs directly)

### Fixed

- **Spark `.isFalse()` bug**: replaced Polars-specific `.isFalse()` with PySpark-compatible `== False` in `engines/spark.py`
- **CHANGELOG structure**: restored clean formatting after `cz bump` mangled the file

### Changed

- **commitizen config**: disabled `update_changelog_on_bump` — git-cliff now owns the changelog
- **Hello World notebook**: switched from `len(result.good)` to `result.good_count` for Spark compatibility

## [0.11.0] — 2026-03-03

### Added

- PII masking: dual-mode masking with direct whole-column replacement and NLP-based span masking via Microsoft Presidio + spaCy
- HIPAA/GDPR tutorials: compliance notebooks demonstrating PII detection and masking workflows
- BigQuery & Snowflake adapters: cloud engine support with dialect-specific SQL generation
- Quickstart examples: remote data ingestion (`01_hello_world`), database governance (`02_database_governance`), dbt PII quality
- Core materialization: Parquet, CSV, Delta output formats with configurable target paths
- Bootstrap CLI: `lakelogic bootstrap` for auto-generating contracts from a landing zone
- AI contract enrichment: LLM-powered field descriptions, PII flags, and SQL rule suggestions via `--ai` flag

### Documentation

- Initial mkdocs-material documentation site with architecture guides and API reference

## [0.7.0] — 2026-02-28

### Added

- Polars engine: native Polars adapter with LazyFrame support for row-rule evaluation
- Installation documentation: engine-specific install guides (`pip install lakelogic[polars]`, etc.)

## [0.6.0] — 2026-02-28

### Added

- CI quality gate: GitHub Actions workflow for linting, formatting, and test runs
- PyPI publish: automated package publishing workflow
- Schema API: programmatic contract creation and validation via `lakelogic.core.schema_api`

## [0.4.0] — 2026-02-28

### Added

- AI contract enricher: LLM provider abstraction supporting OpenAI, Azure, Anthropic, and Ollama for automated contract generation

## [0.3.0] — 2026-02-28

### Added

- CLI: `lakelogic run`, `lakelogic bootstrap`, `lakelogic generate`, `lakelogic doctor`, `lakelogic import-dbt` commands
- Notification system: Apprise-based alerts with Jinja2 templates for Slack, Teams, email, and webhooks
- dbt adapter: `lakelogic import-dbt` converts dbt `schema.yml` / `sources.yml` into LakeLogic contracts
- Contract inference: `infer_contract()` generates a contract YAML from any CSV, Parquet, or JSON file
- Data generator: `lakelogic generate` creates synthetic data from a contract with optional `--invalid-ratio` for quarantine testing

### Documentation

- Architecture guides, comparison docs, notification & secrets reference, playbook tutorials

## [0.1.0] — 2026-02-22

### Added

- Core framework: `DataProcessor`, `ValidationResult`, engine auto-discovery, row-rule evaluation, quarantine routing with `_lakelogic_errors` column
- DuckDB engine: default local engine with SQL-based rule evaluation
- Pandas engine: adapter using DuckDB under the hood for SQL execution
- Contract model: Pydantic-based `DataContract` with schema, quality rules, materialization, lineage, and quarantine config
- Comprehensive examples: quickstarts, core patterns (SCD2, soft delete, reference joins), data sources (CSV, Parquet, JSON, Excel, XML, Delta), cloud platforms, orchestration, and compliance
- Streaming foundations: Kafka, WebSocket, SSE connectors with contract-driven validation
- Database connectors: pyodbc (SQL Server), psycopg2 (PostgreSQL), pymysql (MySQL), pymongo (MongoDB)

### CI/CD

- GitHub Actions for Python quality checks and PyPI publishing

### Documentation

- Comprehensive mkdocs-material site with architecture diagrams and `DataProcessor` return value reference

## [0.1.0b1] — 2026-02-08

### Added

- Initial project structure: core logic, CLI scaffolding, engine abstraction, test suite
- PyPI publishing workflow
- First examples and documentation site

---

<!-- Link definitions -->
[1.1.0]: https://github.com/lakelogic/LakeLogic/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/lakelogic/LakeLogic/compare/v0.11.0...v1.0.0
[0.11.0]: https://github.com/lakelogic/LakeLogic/compare/v0.7.0...v0.11.0
[0.7.0]: https://github.com/lakelogic/LakeLogic/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/lakelogic/LakeLogic/compare/v0.4.0...v0.6.0
[0.4.0]: https://github.com/lakelogic/LakeLogic/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/lakelogic/LakeLogic/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/lakelogic/LakeLogic/compare/v0.1.0b1...v0.1.0
[0.1.0b1]: https://github.com/lakelogic/LakeLogic/releases/tag/v0.1.0b1
