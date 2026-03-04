# Changelog

All notable changes to **LakeLogic** are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---


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

## v0.13.0 (2026-03-04)

### Feat

- introduce Spark execution engine for data contract validation and transformation.

## v0.12.0 (2026-03-03)

### Feat

- Add "Hello World" example notebook for remote data ingestion and quality validation, and update the changelog.

## v0.11.0 (2026-03-03)

### Feat

- add PII masking hook with direct and NLP-based replacement modes, and refine changelog documentation.
- add tutorials for HIPAA, GDPR compliance, and PII masking.
- Add tutorials demonstrating HIPAA/GDPR compliance and PII masking.
- Introduce the foundational LakeLogic data processing framework, including core modules, engine integrations, AI, and CLI.
- add script to synchronize flagship examples to the documentation directory.
- Add a script to synchronize flagship examples to the documentation and update MkDocs navigation to feature new interactive examples.
- Add new core materialization module, comprehensive examples, and initial documentation, while updating the main README.
- Add 01_hello_world.ipynb quickstart example for remote data ingestion.
- Add quickstart examples for remote data ingestion, database governance, and dbt PII quality, supported by new core materialization and models.
- Add BigQuery and Snowflake engine adapters along with a dependency management utility for optional packages.

## v0.7.0 (2026-02-28)

### Feat

- Add Polars engine, new examples for HIPAA/GDPR compliance and AI contract enrichment, and installation documentation.

## v0.6.0 (2026-02-28)

### Feat

- Add CI quality gate and PyPI publish workflows, and update documentation examples via a new sync script.
- Add extensive examples for quickstart, core patterns, advanced workflows, and compliance, alongside new documentation and a schema API.

## v0.4.0 (2026-02-28)

### Feat

- Add AI-powered contract enrichment functionality with LLM provider abstraction.

## v0.3.0 (2026-02-28)

### Feat

- Implement the initial command-line interface for contract execution, output management, and environment setup.
- Implement a universal notification system using Apprise with new Jinja2 templates and a base adapter.
- Add quickstart examples for data ingestion across various file formats using contracts and notebooks.
- Add new documentation for notifications & secrets, playbooks, and a main index, and update mkdocs navigation.
- Add extensive documentation including comparison, installation, and architecture guides, update project branding, and introduce core processor logic.
- Introduce new examples covering advanced workflows and compliance/governance scenarios, including data, contracts, and notebooks.
- Implement initial LakeLogic framework with contract inference, multi-engine support, and Databricks deployment configuration.
- Establish core data contract models and initial data processing infrastructure with engine support.
- Implement contract inference from data files, add dbt adapter, schema API, and advanced workflow examples.

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

## v0.1.0b1 (2026-02-08)

### Feat

- Establish initial project structure with core logic, CLI, multiple engines, comprehensive documentation, examples, and a test suite.
- Introduce comprehensive documentation, examples, and support for multiple data engines, core logic, and CLI functionalities.
- add PyPI publishing workflow and comprehensive MkDocs documentation site
- Add GitHub Actions workflow for publishing to PyPI.
- Add basic validation contract for the `silver_crm_customers` dataset example.
- Introduce core Lakeguard framework with multiple data engines, comprehensive examples, and extensive documentation.
- Add comprehensive examples, tutorials, documentation, and new engine implementations for various data platforms.
