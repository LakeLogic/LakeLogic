# Changelog

All notable changes to **LakeLogic** are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
with pre-release labels (`0.1.0b3`, `0.1.0rc1`, …) until v1.

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
and this file can be regenerated at any time with:

```bash
git cliff --output CHANGELOG.md
```

---


### Added

- Initial public pre-release of LakeLogic OSS.
- Core `DataProcessor` with Polars, Pandas, DuckDB, Spark, Snowflake, and BigQuery adapter stubs.
- `DataContract` Pydantic model with quality rules, transformations, schema policy, SLOs, and materialization.
- `lakelogic run` — CLI command for validating a source file against a contract YAML.
- `lakelogic bootstrap` — landing-zone scanner and contract generator.
- `lakelogic generate` — synthetic data generator from a contract definition.
- `lakelogic import-dbt` — dbt `schema.yml` / `sources.yml` to LakeLogic contract converter.
- `lakelogic setup-oss` — DuckDB extension pre-installer (`iceberg`, `delta`, `httpfs`, `aws`, `azure`).
- `PipelineDriver` in `driver.py` for multi-contract bronze/silver/gold pipeline orchestration.
- GitHub Actions CI pipeline (`ci-gate.yml`) — Ruff lint and pytest with coverage.

---

<!-- link definitions (auto-updated by git-cliff) -->
[Unreleased]: https://github.com/LineageLogic/LakeLogic/compare/v0.1.0b3...HEAD
[0.1.0b3]: https://github.com/LineageLogic/LakeLogic/compare/v0.1.0b2...v0.1.0b3
[0.1.0b2]: https://github.com/LineageLogic/LakeLogic/releases/tag/v0.1.0b2

## v0.1.0 (2026-02-22)

### Feat

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
- Establish initial project structure with core logic, CLI, multiple engines, comprehensive documentation, examples, and a test suite.
- Introduce comprehensive documentation, examples, and support for multiple data engines, core logic, and CLI functionalities.
- add PyPI publishing workflow and comprehensive MkDocs documentation site
- Add GitHub Actions workflow for publishing to PyPI.
- Add basic validation contract for the `silver_crm_customers` dataset example.
- Introduce core Lakeguard framework with multiple data engines, comprehensive examples, and extensive documentation.
- Add comprehensive examples, tutorials, documentation, and new engine implementations for various data platforms.
