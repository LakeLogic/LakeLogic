# Changelog

All notable changes to **LakeLogic** are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---
## [1.42.0] — 2026-08-11

### Added

- **generator**: Reproducible timestamps + opt-in per-entity seeding in generate_related

### Fixed

- **bigquery**: Make the BigQuery engine run live (7 dialect/runtime fixes)
- **slo**: Treat naive timestamps as UTC in freshness/retention, never host-localize
## [1.41.0] — 2026-08-03

### Added

- Add Streaming API documentation and navigation entry

### Fixed

- **snowflake**: TRY_CAST via TO_VARCHAR + shared-connection for Notebook use
## [1.40.6] — 2026-07-30

### Fixed

- Export streaming API via __all__ (resolves F401 on package init)
## [1.40.4] — 2026-07-23

### Documentation

- Update cloud integration and comparison documentation for clarity
- Update cloud integration and comparison documentation for clarity
## [1.40.3] — 2026-07-21

### Documentation

- Add 1.40.3 changelog entry for all-null extraction column fix

### Fixed

- Handle all-null columns in Spark DataFrame creation and add regression test
## [1.40.2] — 2026-07-20

### Fixed

- Ensure effective_from is set correctly for initial loads and inject unknown member
- Update SARIF upload condition and version in review workflow
## [1.40.1] — 2026-07-19

### Fixed

- Improve logging for DeletionVectors configuration in Spark session
## [1.40.0] — 2026-07-17

### Added

- **database**: Add Spark JDBC ingestion with fetchsize batching and partitioned reads
- **sensitive**: Introduce handling for confidential non-personal fields
- Add option method to FakeWriter for test transparency in spark_save_as_table
- Disable deletion vectors in _spark_save_as_table to support delta-rs compatibility
- Enhance _spark_scd2_dataframe to retain current rows with unchanged keys during processing
- Enhance error handling in PipelineRunSummary with failure detection and detailed reporting
- Update logo colors and enhance documentation clarity with open core details
- Add observatory spool for telemetry pushes with bounded retry
- Knowledge scanner, DuckDB adapter tests, notebook hooks, registry/processor refactors

### Changed

- **examples**: Make Colab notebooks engine-neutral and fix demo correctness

### Documentation

- Update index.md to enhance clarity and detail of data contract framework

### Fixed

- **duckdb**: Apply deduplicate transform in duckdb engine
- **generator,engine**: Robust ingest of string-typed numeric/temporal/boolean columns
- **processor**: Glob-expand non-partitioned cloud landing directories
- **registry**: Preserve 'on' join key when loading contract YAML
- Resolve pyarrow CVE GHSA-rgxp-2hwp-jwgg (IPC pre-buffering use-after-free)
- Update monkeypatch for dlt_adapter and warnings to match expected signatures

### Styling

- **tests**: Wrap multi-line lambda definitions to satisfy ruff

### Testing

- Add 15 tests for quarantine dlt + guards; fix unreachable dlt branch
- Add 20 tests for lineage.py uncovered branches; exclude Spark
- Add 24 tests for gdpr.py polars + dispatch branches; exclude Spark
- Add 19 tests for materialize_dataframe public entry point
- Add 24 tests for _write_frame dispatcher + fix UnboundLocalError bug
- Add 40 tests for materialization secondary-targets and Delta compaction
- Skip pandas-dependent duckdb test when pandas isn't installed

### Coverage

- Exclude Spark-only branches from processor.py coverage
- Exclude Spark-only functions from coverage measurement

### Security

- Bump sqlfluff to 4.2.1 to fix parser DoS CVEs
## [1.31.0] — 2026-05-14

### Added

- Add interactive PR comment handler for explain and ignore commands
## [1.30.0] — 2026-05-11

### Added

- Implement DataProcessor core class and ValidationResult for engine-agnostic data contract execution
- Add processor, registry, and run_log modules with comprehensive branch coverage testing
- Add local configuration, coverage reporting, and registry branch tests
- Bump version to 1.28.0 and add integration tests for SparkAdapter schema casting
- Implement core registry, processing, and execution engines for LakeLogic pipeline system
- Implement core registry management and metadata-driven pipeline framework
- Add GDPR erasure and data backfill notebooks, test helpers, and coverage tracking utility

### CI/CD

- Remove -x from pytest to ensure codecov report is generated on failure

### Fixed

- **engines/duckdb**: Add CATEGORY_COLUMN to errors array, and properly type empty _lakelogic_errors as VARCHAR[] to match Polars/Spark output schemas
- **tests**: Skip test_spark_delta_logic if pyspark is not installed
- **tests**: Remove global sys.modules patch for PySpark and explicitly set PYSPARK_PYTHON

### Testing

- Add targeted branch coverage tests for validation, runner, and processor components
- Add unit test suites for PolarsAdapter logic and code reviewer functionality
- Add unit tests for code reviewer orchestration and Polars adapter logic
## [1.25.0] — 2026-05-07

### Added

- **gdpr**: Fallback to contract compliance property if compliance_event missing
- Add comprehensive suite of interactive Colab tutorial notebooks and remove obsolete compliance test file

### Fixed

- **engines**: Cross-engine compatibility fixes for test matrix

### Testing

- Fix data generator test formatting for new output formats
## [1.24.0] — 2026-05-06

### Added

- Enhance DuckDB and Polars integration with zero-copy Arrow registration

### Fixed

- Resolve incorrect rendering of driver profile contract table name
- Resolve E2E tests and dependency regression bugs
## [1.22.0] — 2026-05-01

### Added

- Add comprehensive Colab example suite for onboarding and integration workflows
- Add new marketplace domain configuration for rideflow example
- Add CI quality gate workflow and compliance governance tutorial notebook
- Add silver layer contract for rideflow trips and configure coverage exclusions

### Changed

- Update coverage exclusion patterns to remove deprecated cli path and simplify ai module comment

### Documentation

- Revert back to Apache-2.0 per user request
- Standardize MIT license and finalize dlt observability patterns
- Pre-execute data quality tutorial notebook for github rendering

### Fixed

- Resolve Linux-specific psutil fallback test failure in CI
- Resolve test failures in CI due to missing optional dependencies and pandas 3.0 incompatibility

### Styling

- Resolve all ruff linting and formatting issues including line length violations

### Testing

- Commit all missing test files by fixing gitignore exclusion
## [1.20.0] — 2026-04-27

### Added

- Add core generation, processing, and SLO modules, bump version to 1.19.0, and include new executive and compliance dashboard examples.
- Implement run logging and engine abstractions for LakeLogic core and CLI
- Implement core pipeline engine and expand rideflow domain examples with interactive notebooks and dashboards

### CI/CD

- Add GitHub Actions CI workflow and constrain dependency python version markers in uv.lock
## [1.18.0] — 2026-04-16

### Added

- Implement core data generation, processing, and bootstrapping modules with accompanying documentation and examples.

### Documentation

- Fix broken badge image rendering in README links
## [1.17.1] — 2026-04-15

### Changed

- Update README links to use HTML anchor tags for improved compatibility
## [1.17.0] — 2026-04-15

### Added

- Implement AI-powered edge-case generation for data contract stress-testing
- Add engine portability and dimensional modeling demonstration notebook
- Add example notebooks for engine portability, developer experience, AI data generation, and integrations

### Documentation

- Add comprehensive set of Colab example notebooks for LakeLogic features
## [1.16.0] — 2026-04-15

### Added

- Add engine portability demo notebook and implement core processor logic
- Add sync script and populate documentation examples directory with flagship notebooks
## [1.15.0] — 2026-04-15

### Added

- Add engine portability examples and implement SCD2 materialization support in core models and LLM engine
- Implement core Pydantic data models and project documentation structure

### Documentation

- Sync fixed python 3.9 syntax and lint configs
- Add examples folder to version control and fix mkdocs links
## [1.13.0] — 2026-04-14

### Added

- Implement core LakeLogic framework with engine adapters, pipeline runner, and governance modules
- Implement core engine components, notification system, and comprehensive data contract documentation
- Implement core platform engine, notification system, and comprehensive data contract documentation with expanded examples

### Fixed

- **core**: Migrate legacy schema_evolution to allow correct quarantine dropping
## [1.12.0] — 2026-04-07

### Added

- Implement core processing logic, dlt adapter, and quickstart examples with updated dependency resolution markers
- Implement core data contract engine with multi-engine support, CLI, and documentation
- Implement incremental processing boundary resolution and tracking logic
- Implement core Lakelogic framework including registry management, pipeline execution, multi-engine support, and documentation styling.
- Add quickstart example with YAML contract and hello world notebook
- Add quickstart notebook examples for basic data processing and dbt-based quality workflows
- Implement pipeline runner and add medallion architecture quickstart examples

### Documentation

- Add project landing page and contract configuration documentation

### Fixed

- **docs**: Force overwrite of downloaded files in colab setup
- **docs**: Resolve colab spark lazy execution and fix github raw urls
- **tests**: Skip deprecated DuckDB tests, fix merge soft_delete scope, fix unknown_member test
- **tests**: Correct quarantine path assertion and mkdir ordering
- **tests**: Resolve collection failures, obsolete execution engines, and windows path limits
- Resolve ruff lint errors (F821 undefined report, F841 unused vars, E501 long lines)
## [1.10.0] — 2026-03-30

### Added

- Implement core pipeline framework including materialization, incremental processing, masking, and run logging with documentation.
## [1.9.0] — 2026-03-27

### Added

- Add LakehousePipeline engine for declarative data mesh execution.
## [1.8.0] — 2026-03-27

### Added

- Add `lakelogic/core/constants.py` and pin `anyio` to `3.7.1` and `google-genai` to `1.4.0` in `uv.lock`.

### Build

- Update project configuration and dependencies.
## [1.7.0] — 2026-03-27

### Added

- Add Polars, Snowflake, and DuckDB execution engines, and core pipeline components.
- Introduce core data processing, materialization, and AI-driven contract generation features with comprehensive tests.

### Clean

- Delete stale test output and artifact files.
## [1.6.0] — 2026-03-26

### Added

- Implement materialization core logic and bootstrap tests, updating dependencies for anthropic, boto3, botocore, and streamlining cryptography.
- Implement core data processing, materialization, and governance framework including a pipeline runner, CLI, and AI contract enrichment.
- Add new quickstart example notebooks for basic data governance and data pipelines.
## [1.5.0] — 2026-03-24

### Added

- Add GitHub Actions CI quality gate for Python linting and tests.
- Add GitHub Actions CI quality gate for backend linting and core tests with coverage checks.
- Add quickstart and pipeline examples including notebooks and associated data contracts for bronze, silver, and gold layers.
- Implement core data contract inference, generation, and pipeline execution framework with multi-engine support, removing temporary development files.
- Enhance data contract specification with schema policies, reference data, and soft deletes, and add new documentation and a quickstart example.
- Introduce core LakeLogic data processing, validation, and materialization features with extensive documentation and architecture diagrams.
- Implement initial declarative data mesh pipeline engine with core processing, AI data generation, and GDPR compliance modules.
- Add comprehensive project documentation, custom styling, and core observer and schema API modules.
- Add Polars execution engine adapter with SQL transformations, link registration, and schema application.
- Introduce core data processing logic with engine adapters and dbt integration.
- Add documentation for engine capabilities, lakehouse catalog table name resolution, and a contract template.
- Add documentation for engine capabilities, lakehouse catalog table name resolution, and a contract template.
- Add LLM extraction engine and model registry for unstructured data processing

### Documentation

- Update README to detail LakeLogic's alignment with Data Mesh pillars.
- Add initial project documentation and enhance the README contract example with clarifications and new service level objectives.
- Remove "The Problem" section and "The Solution" header from README.
## [1.2.0] — 2026-03-08

### Added

- Enhance schema validation and add GenericSQL adapter

### Changed

- Clean up import statements and exception handling in GenericSQLAdapter
## [1.1.0] — 2026-03-07

### Added

- Add core materialization utilities for data persistence, including Spark table management, path resolution, and dataframe output.
## [0.14.0] — 2026-03-04

### Added

- Add Spark engine adapter for data contract execution and transformation.
- Add Spark engine row-level validation and engine-agnostic row counts, implement automated changelog and release workflows, and fix Spark `.isFalse()` bug and CHANGELOG structure.
- Implement core processor for remote data ingestion, add quickstart examples, and set up automated changelog generation.
## [0.13.0] — 2026-03-04

### Added

- Introduce Spark execution engine for data contract validation and transformation.
## [0.12.0] — 2026-03-03

### Added

- Add "Hello World" example notebook for remote data ingestion and quality validation, and update the changelog.
- Add PII masking hook with direct and NLP-based replacement modes, and refine changelog documentation.
- Add tutorials for HIPAA, GDPR compliance, and PII masking.
- Add tutorials demonstrating HIPAA/GDPR compliance and PII masking.
## [0.10.0] — 2026-03-01

### Added

- Introduce the foundational LakeLogic data processing framework, including core modules, engine integrations, AI, and CLI.
- Add script to synchronize flagship examples to the documentation directory.
## [0.9.0] — 2026-03-01

### Added

- Add a script to synchronize flagship examples to the documentation and update MkDocs navigation to feature new interactive examples.
- Add new core materialization module, comprehensive examples, and initial documentation, while updating the main README.
- Add 01_hello_world.ipynb quickstart example for remote data ingestion.
- Add quickstart examples for remote data ingestion, database governance, and dbt PII quality, supported by new core materialization and models.

### Documentation

- Add documentation index page
## [0.8.0] — 2026-02-28

### Added

- Add BigQuery and Snowflake engine adapters along with a dependency management utility for optional packages.
- Add Polars engine, new examples for HIPAA/GDPR compliance and AI contract enrichment, and installation documentation.
- Add CI quality gate and PyPI publish workflows, and update documentation examples via a new sync script.
## [0.5.0] — 2026-02-28

### Added

- Add extensive examples for quickstart, core patterns, advanced workflows, and compliance, alongside new documentation and a schema API.
- Add AI-powered contract enrichment functionality with LLM provider abstraction.
- Implement the initial command-line interface for contract execution, output management, and environment setup.
## [0.2.0] — 2026-02-27

### Added

- Implement a universal notification system using Apprise with new Jinja2 templates and a base adapter.
- Add quickstart examples for data ingestion across various file formats using contracts and notebooks.
- Add new documentation for notifications & secrets, playbooks, and a main index, and update mkdocs navigation.
- Add extensive documentation including comparison, installation, and architecture guides, update project branding, and introduce core processor logic.

### Documentation

- Add initial documentation index page.
- Add `docs/index.md` and correct capitalization of 'LakeLogic' in `mkdocs.yml` URLs and repository names.
## [0.2.0b0] — 2026-02-26

### Added

- Introduce new examples covering advanced workflows and compliance/governance scenarios, including data, contracts, and notebooks.
- Implement initial LakeLogic framework with contract inference, multi-engine support, and Databricks deployment configuration.
- Establish core data contract models and initial data processing infrastructure with engine support.
- Implement contract inference from data files, add dbt adapter, schema API, and advanced workflow examples.
- Add extensive examples for data sources, core patterns, and advanced workflows, along with new core engine and CLI components.
- Add extensive examples for data sources, core patterns, and advanced workflows, along with new data engines and core utilities.
- Introduce new quickstart examples for remote data ingestion and database governance, add notebook cleaning utilities, and expand installation documentation.
## [0.1.0b2] — 2026-02-14

### Added

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
[1.42.0]: https://github.com/lakelogic/LakeLogic/compare/v1.41.0...v1.42.0
[1.41.0]: https://github.com/lakelogic/LakeLogic/compare/v1.40.6...v1.41.0
[1.40.6]: https://github.com/lakelogic/LakeLogic/compare/v1.40.4...v1.40.6
[1.40.4]: https://github.com/lakelogic/LakeLogic/compare/v1.40.3...v1.40.4
[1.40.3]: https://github.com/lakelogic/LakeLogic/compare/v1.40.2...v1.40.3
[1.40.2]: https://github.com/lakelogic/LakeLogic/compare/v1.40.1...v1.40.2
[1.40.1]: https://github.com/lakelogic/LakeLogic/compare/v1.39.0...v1.40.1
[1.40.0]: https://github.com/lakelogic/LakeLogic/compare/v1.31.0...v1.40.0
[1.31.0]: https://github.com/lakelogic/LakeLogic/compare/v1.30.0...v1.31.0
[1.30.0]: https://github.com/lakelogic/LakeLogic/compare/v1.25.0...v1.30.0
[1.25.0]: https://github.com/lakelogic/LakeLogic/compare/v1.24.0...v1.25.0
[1.24.0]: https://github.com/lakelogic/LakeLogic/compare/v1.22.0...v1.24.0
[1.22.0]: https://github.com/lakelogic/LakeLogic/compare/v1.20.0...v1.22.0
[1.20.0]: https://github.com/lakelogic/LakeLogic/compare/v1.18.0...v1.20.0
[1.18.0]: https://github.com/lakelogic/LakeLogic/compare/v1.17.1...v1.18.0
[1.17.1]: https://github.com/lakelogic/LakeLogic/compare/v1.17.0...v1.17.1
[1.17.0]: https://github.com/lakelogic/LakeLogic/compare/v1.16.0...v1.17.0
[1.16.0]: https://github.com/lakelogic/LakeLogic/compare/v1.15.0...v1.16.0
[1.15.0]: https://github.com/lakelogic/LakeLogic/compare/v1.13.0...v1.15.0
[1.13.0]: https://github.com/lakelogic/LakeLogic/compare/v1.12.0...v1.13.0
[1.12.0]: https://github.com/lakelogic/LakeLogic/compare/v1.10.0...v1.12.0
[1.10.0]: https://github.com/lakelogic/LakeLogic/compare/v1.9.0...v1.10.0
[1.9.0]: https://github.com/lakelogic/LakeLogic/compare/v1.8.0...v1.9.0
[1.8.0]: https://github.com/lakelogic/LakeLogic/compare/v1.7.0...v1.8.0
[1.7.0]: https://github.com/lakelogic/LakeLogic/compare/v1.6.0...v1.7.0
[1.6.0]: https://github.com/lakelogic/LakeLogic/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/lakelogic/LakeLogic/compare/v1.2.0...v1.5.0
[1.2.0]: https://github.com/lakelogic/LakeLogic/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/lakelogic/LakeLogic/compare/v0.14.0...v1.1.0
[0.14.0]: https://github.com/lakelogic/LakeLogic/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/lakelogic/LakeLogic/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/lakelogic/LakeLogic/compare/v0.10.0...v0.12.0
[0.10.0]: https://github.com/lakelogic/LakeLogic/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/lakelogic/LakeLogic/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/lakelogic/LakeLogic/compare/v0.5.0...v0.8.0
[0.5.0]: https://github.com/lakelogic/LakeLogic/compare/v0.2.0...v0.5.0
[0.2.0]: https://github.com/lakelogic/LakeLogic/compare/v0.2.0b0...v0.2.0
[0.2.0b0]: https://github.com/lakelogic/LakeLogic/compare/v0.1.0b2...v0.2.0b0
[0.1.0b2]: https://github.com/lakelogic/LakeLogic/compare/v0.1.0b1...v0.1.0b2
[0.1.0b1]: https://github.com/lakelogic/LakeLogic/releases/tag/v0.1.0b1

