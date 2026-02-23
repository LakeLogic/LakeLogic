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

## [Unreleased]

---

## [0.1.0] — 2026-02-22

### Added

- `test_duckdb_adapter.py` — 29-test module mirroring `test_engines.py` for the DuckDB engine, covering quality rules, transformations (rename, derive, trim, lower, select, filter), schema policy (drop/quarantine unknown), quality helper expansion (not_null, accepted_values, range), `run_source` file loading (CSV, Parquet, glob, explicit path), and `ValidationResult` correctness.
- CLI `rich_help_panel` groupings for all six commands (`run`, `setup-oss`, `bootstrap`, `help`, `generate`, `import-dbt`), making `lakelogic --help` output clearly segmented.
- `ValidationResult` now supports 2-tuple unpacking (`good, bad = proc.run(df)`), in addition to the existing 3-tuple.
- `docs/cli.md` — comprehensive CLI reference covering all commands, options, examples, and Windows-specific notes.
- `docs/installation.md` — Windows Developer Notes section (venv launcher caveat, PATH configuration, console encoding fix).
- `test_core.py` — `TestRunSource` (6 tests), `TestTracing` (5 tests), `TestDuckDBAdapter` (3 tests), `TestPandasAdapter`, and SLO tests.
- `test_engines.py` — `PolarsAdapter` unit tests covering quality splits, transformations, helper transforms (trim, lower, map_values, coalesce, select), schema policy, quality helper expansion, and Spark adapter coverage.
- `test_driver_integration.py` — DuckDB pipeline E2E integration tests (bronze > silver > gold), summary table writes, metrics emission, `RunLogReader`, Prometheus formatting, CLI parser round-trips, backfill windows, entity/contract filtering, and resume functionality.
- `test_driver_properties.py` — Hypothesis property-based tests for `parse_layers`, `parse_entities`, `parse_metrics_tags`, `parse_overrides`, `parse_window`, `flatten_summary`, `format_prometheus`, and `build_backfill_windows`.
- `CHANGELOG.md` with commitizen workflow guide.
- `commitizen` configured in `pyproject.toml` for automated versioning and changelog generation.

### Fixed

- DuckDB connection-lifetime gotcha: lazy `DuckDBPyRelation` objects returned by `DuckDBAdapter.execute()` become invalid when the `DataProcessor` instance is GC'd. All DuckDB tests now hold a named `proc` reference for the full assertion lifetime.
- `ValidationResult.__iter__` bare `except` clause replaced with `except Exception` to prevent silently swallowing real errors.
- Stray whitespace removed from `processor.py` (linting clean).
- `rel = None` initialised before the DuckDB `run_source` branch to prevent `UnboundLocalError` on unsupported source types.
- `main.py` — Windows console encoding fix: `sys.stdout`/`sys.stderr` reconfigured to UTF-8 with error replacement at import time, preventing `UnicodeEncodeError` on characters in Rich help output.
- Unicode arrow replaced with ASCII `->` in `import-dbt` docstring for legacy Windows console compatibility.
- Broken cp312 Python extensions (`numpy`, `pandas`, `duckdb`, `pyarrow`, `pydantic-core`) in `.venv_lakelogic` after Python 3.12 -> 3.13 upgrade, fixed by force-reinstalling each package.

### Changed

- Version promoted from `0.1.0b3` to `0.1.0` (first stable release).

---

## [0.1.0b2] — 2026-01-17

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
[Unreleased]: https://github.com/LineageLogic/LakeLogic/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/LineageLogic/LakeLogic/compare/v0.1.0b2...v0.1.0
[0.1.0b2]: https://github.com/LineageLogic/LakeLogic/releases/tag/v0.1.0b2
