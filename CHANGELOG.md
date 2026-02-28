# Changelog

All notable changes to **LakeLogic** are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/).

---

## Workflow

### Making commits

Use `cz commit` instead of `git commit` for guided conventional commit formatting:

```bash
cz commit
```

This prompts you for **type**, optional **scope**, and **description**, then writes a correctly formatted message such as:

```
feat(duckdb): add lazy relation materialiser helpers
fix(cli): prevent UnicodeEncodeError on Windows console
test(core): add property-based tests for schema policy
docs: rewrite installation guide with Windows notes
chore: bump polars to 0.21.x
```

Common types and their version-bump impact:

| Type | Bump | Use for |
|---|---|---|
| `feat` | minor | New user-facing capability |
| `fix` | patch | Bug fix |
| `perf` | patch | Performance improvement |
| `refactor` | – | Internal restructure, no behaviour change |
| `test` | – | Adding or fixing tests |
| `docs` | – | Documentation only |
| `chore` | – | Tooling, deps, CI |
| `BREAKING CHANGE:` (footer) | major | Any breaking API change |

### Releasing a new version

```bash
# 1. Make sure all commits since last tag use Conventional Commits
# 2. Run bump — calculates next version, updates files, writes changelog, tags
cz bump

# 3. Push code and tag together
git push --follow-tags
```

`cz bump` automatically:
- Calculates the next version from commit history (`feat` → minor, `fix` → patch)
- Updates `version` in **`pyproject.toml`** and **`lakelogic/__init__.py`**
- Prepends a new dated section to **`CHANGELOG.md`**
- Creates a **git tag** (`v0.1.1`, `v0.2.0`, etc.)

### Regenerating the changelog without bumping

```bash
cz changelog
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

<!-- link definitions (updated by cz bump) -->
[Unreleased]: https://github.com/lakelogic/LakeLogic/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/lakelogic/LakeLogic/compare/v0.1.0b2...v0.1.0
[0.1.0b2]: https://github.com/lakelogic/LakeLogic/releases/tag/v0.1.0b2

## v0.8.0 (2026-02-28)

### Feat

- Add BigQuery and Snowflake engine adapters along with a dependency management utility for optional packages.

## v0.7.0 (2026-02-28)

### Feat

- Add Polars engine, new examples for HIPAA/GDPR compliance and AI contract enrichment, and installation documentation.

## v0.6.0 (2026-02-28)

### Feat

- Add CI quality gate and PyPI publish workflows, and update documentation examples via a new sync script.

## v0.5.0 (2026-02-28)

### Feat

- Add extensive examples for quickstart, core patterns, advanced workflows, and compliance, alongside new documentation and a schema API.

## v0.4.0 (2026-02-28)

### Feat

- Add AI-powered contract enrichment functionality with LLM provider abstraction.

## v0.3.0 (2026-02-28)

### Feat

- Implement the initial command-line interface for contract execution, output management, and environment setup.
- Implement a universal notification system using Apprise with new Jinja2 templates and a base adapter.

## v0.2.0 (2026-02-27)

### Feat

- Add quickstart examples for data ingestion across various file formats using contracts and notebooks.
- Add new documentation for notifications & secrets, playbooks, and a main index, and update mkdocs navigation.
- Add extensive documentation including comparison, installation, and architecture guides, update project branding, and introduce core processor logic.
- Introduce new examples covering advanced workflows and compliance/governance scenarios, including data, contracts, and notebooks.

## v0.2.0b0 (2026-02-26)

### Feat

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
- Establish initial project structure with core logic, CLI, multiple engines, comprehensive documentation, examples, and a test suite.
- Introduce comprehensive documentation, examples, and support for multiple data engines, core logic, and CLI functionalities.
- add PyPI publishing workflow and comprehensive MkDocs documentation site
- Add GitHub Actions workflow for publishing to PyPI.
- Add basic validation contract for the `silver_crm_customers` dataset example.
- Introduce core Lakeguard framework with multiple data engines, comprehensive examples, and extensive documentation.
- Add comprehensive examples, tutorials, documentation, and new engine implementations for various data platforms.
