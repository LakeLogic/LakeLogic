# Changelog

All notable changes to **LakeLogic** are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---
## [1.23.0] — 2026-05-06

### Added

- Enhance DuckDB and Polars integration with zero-copy Arrow registration
## [1.22.3] — 2026-05-06

### Fixed

- Resolve incorrect rendering of driver profile contract table name
## [1.22.2] — 2026-05-05

### Fixed

- Resolve E2E tests and dependency regression bugs
## [1.22.0] — 2026-05-01

### Added

- Add comprehensive Colab example suite for onboarding and integration workflows
- Add new marketplace domain configuration for rideflow example

### Documentation

- Revert back to Apache-2.0 per user request
- Standardize MIT license and finalize dlt observability patterns
- Pre-execute data quality tutorial notebook for github rendering

### Fixed

- Resolve Linux-specific psutil fallback test failure in CI
- Resolve test failures in CI due to missing optional dependencies and pandas 3.0 incompatibility

### Styling

- Resolve all ruff linting and formatting issues including line length violations
## [1.21.0] — 2026-04-27

### Added

- Add CI quality gate workflow and compliance governance tutorial notebook
- Add silver layer contract for rideflow trips and configure coverage exclusions
- Add core generation, processing, and SLO modules, bump version to 1.19.0, and include new executive and compliance dashboard examples.

### CI/CD

- Add GitHub Actions CI workflow and constrain dependency python version markers in uv.lock

### Changed

- Update coverage exclusion patterns to remove deprecated cli path and simplify ai module comment

### Testing

- Commit all missing test files by fixing gitignore exclusion
## [1.19.0] — 2026-04-27

### Added

- Implement run logging and engine abstractions for LakeLogic core and CLI
- Implement core pipeline engine and expand rideflow domain examples with interactive notebooks and dashboards
- Implement core data generation, processing, and bootstrapping modules with accompanying documentation and examples.
- Implement AI-powered edge-case generation for data contract stress-testing
- Add engine portability and dimensional modeling demonstration notebook
- Add example notebooks for engine portability, developer experience, AI data generation, and integrations
- Add engine portability demo notebook and implement core processor logic
- Add sync script and populate documentation examples directory with flagship notebooks
- Add engine portability examples and implement SCD2 materialization support in core models and LLM engine

### Changed

- Update README links to use HTML anchor tags for improved compatibility

### Documentation

- Fix broken badge image rendering in README links
- Add comprehensive set of Colab example notebooks for LakeLogic features
## [1.14.0] — 2026-04-15

### Added

- Implement core Pydantic data models and project documentation structure
- Implement core LakeLogic framework with engine adapters, pipeline runner, and governance modules
- Implement core engine components, notification system, and comprehensive data contract documentation
- Implement core platform engine, notification system, and comprehensive data contract documentation with expanded examples
- Implement core processing logic, dlt adapter, and quickstart examples with updated dependency resolution markers

### Documentation

- Sync fixed python 3.9 syntax and lint configs
- Add examples folder to version control and fix mkdocs links

### Fixed

- **core**: Migrate legacy schema_evolution to allow correct quarantine dropping
## [1.11.1] — 2026-04-07

### Fixed

- Resolve ruff lint errors (F821 undefined report, F841 unused vars, E501 long lines)
## [1.11.0] — 2026-04-06

### Added

- Implement core data contract engine with multi-engine support, CLI, and documentation
- Implement incremental processing boundary resolution and tracking logic
- Add quickstart example with YAML contract and hello world notebook
- Add quickstart notebook examples for basic data processing and dbt-based quality workflows
- Implement pipeline runner and add medallion architecture quickstart examples
- Implement core pipeline framework including materialization, incremental processing, masking, and run logging with documentation.
- Implement core Lakelogic framework including registry management, pipeline execution, multi-engine support, and documentation styling.
- Add LakehousePipeline engine for declarative data mesh execution.
- Add `lakelogic/core/constants.py` and pin `anyio` to `3.7.1` and `google-genai` to `1.4.0` in `uv.lock`.
- Add Polars, Snowflake, and DuckDB execution engines, and core pipeline components.

### Build

- Update project configuration and dependencies.

### Documentation

- Add project landing page and contract configuration documentation

### Fixed

- **docs**: Force overwrite of downloaded files in colab setup
- **docs**: Resolve colab spark lazy execution and fix github raw urls
- **tests**: Skip deprecated DuckDB tests, fix merge soft_delete scope, fix unknown_member test
- **tests**: Correct quarantine path assertion and mkdir ordering
- **tests**: Resolve collection failures, obsolete execution engines, and windows path limits
## [1.6.1] — 2026-03-27

### Added

- Introduce core data processing, materialization, and AI-driven contract generation features with comprehensive tests.
- Implement materialization core logic and bootstrap tests, updating dependencies for anthropic, boto3, botocore, and streamlining cryptography.
- Implement core data processing, materialization, and governance framework including a pipeline runner, CLI, and AI contract enrichment.
- Add new quickstart example notebooks for basic data governance and data pipelines.
- Add GitHub Actions CI quality gate for Python linting and tests.
- Add GitHub Actions CI quality gate for backend linting and core tests with coverage checks.
- Add quickstart and pipeline examples including notebooks and associated data contracts for bronze, silver, and gold layers.

### Clean

- Delete stale test output and artifact files.
## [1.4.0] — 2026-03-24

### Added

- Implement core data contract inference, generation, and pipeline execution framework with multi-engine support, removing temporary development files.
- Enhance data contract specification with schema policies, reference data, and soft deletes, and add new documentation and a quickstart example.
- Introduce core LakeLogic data processing, validation, and materialization features with extensive documentation and architecture diagrams.
- Implement initial declarative data mesh pipeline engine with core processing, AI data generation, and GDPR compliance modules.

### Documentation

- Update README to detail LakeLogic's alignment with Data Mesh pillars.
- Add initial project documentation and enhance the README contract example with clarifications and new service level objectives.
- Remove "The Problem" section and "The Solution" header from README.
## [1.3.0] — 2026-03-18

### Added

- Add comprehensive project documentation, custom styling, and core observer and schema API modules.
- Add Polars execution engine adapter with SQL transformations, link registration, and schema application.
- Introduce core data processing logic with engine adapters and dbt integration.
- Add documentation for engine capabilities, lakehouse catalog table name resolution, and a contract template.
- Add documentation for engine capabilities, lakehouse catalog table name resolution, and a contract template.
- Add LLM extraction engine and model registry for unstructured data processing
- Enhance schema validation and add GenericSQL adapter
- Add core materialization utilities for data persistence, including Spark table management, path resolution, and dataframe output.

### Changed

- Clean up import statements and exception handling in GenericSQLAdapter
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
[1.23.0]: https://github.com/lakelogic/LakeLogic/compare/v1.22.3...v1.23.0
[1.22.3]: https://github.com/lakelogic/LakeLogic/compare/v1.22.2...v1.22.3
[1.22.2]: https://github.com/lakelogic/LakeLogic/compare/v1.22.0...v1.22.2
[1.22.0]: https://github.com/lakelogic/LakeLogic/compare/v1.21.0...v1.22.0
[1.21.0]: https://github.com/lakelogic/LakeLogic/compare/v1.19.0...v1.21.0
[1.19.0]: https://github.com/lakelogic/LakeLogic/compare/v1.14.0...v1.19.0
[1.14.0]: https://github.com/lakelogic/LakeLogic/compare/v1.11.1...v1.14.0
[1.11.1]: https://github.com/lakelogic/LakeLogic/compare/v1.11.0...v1.11.1
[1.11.0]: https://github.com/lakelogic/LakeLogic/compare/v1.6.1...v1.11.0
[1.6.1]: https://github.com/lakelogic/LakeLogic/compare/v1.4.0...v1.6.1
[1.4.0]: https://github.com/lakelogic/LakeLogic/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/lakelogic/LakeLogic/compare/v1.0.0...v1.3.0
[1.0.0]: https://github.com/lakelogic/LakeLogic/compare/v0.11.0...v1.0.0
[0.11.0]: https://github.com/lakelogic/LakeLogic/compare/v0.7.0...v0.11.0
[0.7.0]: https://github.com/lakelogic/LakeLogic/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/lakelogic/LakeLogic/compare/v0.4.0...v0.6.0
[0.4.0]: https://github.com/lakelogic/LakeLogic/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/lakelogic/LakeLogic/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/lakelogic/LakeLogic/compare/v0.1.0b1...v0.1.0
[0.1.0b1]: https://github.com/lakelogic/LakeLogic/releases/tag/v0.1.0b1

