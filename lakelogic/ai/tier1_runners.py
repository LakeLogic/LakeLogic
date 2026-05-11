"""
lakelogic.ai.tier1_runners
--------------------------
Deterministic Tier 1 review runners.

Each runner takes a list of files and returns ``ReviewFinding`` objects.
Runners are subprocess wrappers around standard linters where possible
(ruff, sqlfluff) plus a small regex-based PII scanner.

If a tool isn't installed, the runner logs a warning and returns ``[]``
rather than crashing — Tier 1 is meant to be best-effort.
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from loguru import logger

if TYPE_CHECKING:
    from lakelogic.ai.code_reviewer import ReviewFinding


# ---------------------------------------------------------------------------
# ruff (Python)
# ---------------------------------------------------------------------------

# Ruff rule code prefixes → our severity buckets. Conservative defaults.
_RUFF_CRITICAL_PREFIXES = ("S",)  # bandit security rules
_RUFF_WARNING_PREFIXES = ("E", "F", "B", "W")  # errors, pyflakes, bugbear, warnings


def _ruff_severity(code: str) -> str:
    if code.startswith(_RUFF_CRITICAL_PREFIXES):
        return "critical"
    if code.startswith(_RUFF_WARNING_PREFIXES):
        return "warning"
    return "info"


def run_ruff(files: list[Path]) -> list["ReviewFinding"]:
    """Run ``ruff check --output-format json`` on Python files."""
    from lakelogic.ai.code_reviewer import ReviewFinding

    py_files = [str(f) for f in files if f.suffix == ".py"]
    if not py_files:
        return []

    try:
        proc = subprocess.run(
            ["ruff", "check", "--output-format", "json", "--exit-zero", *py_files],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except FileNotFoundError:
        logger.warning("ruff not installed; skipping Python lint. pip install lakelogic[review]")
        return []
    except subprocess.TimeoutExpired:
        logger.warning("ruff timed out; skipping Python lint")
        return []

    if not proc.stdout.strip():
        return []

    try:
        raw = json.loads(proc.stdout)
    except json.JSONDecodeError:
        logger.warning(f"ruff returned non-JSON output: {proc.stdout[:200]}")
        return []

    findings: list[ReviewFinding] = []
    for item in raw:
        code = item.get("code") or "unknown"
        findings.append(
            ReviewFinding(
                file=item.get("filename", ""),
                line=(item.get("location") or {}).get("row"),
                severity=_ruff_severity(code),
                category="python_quality",
                rule=f"ruff_{code.lower()}",
                message=item.get("message", ""),
                suggestion=(item.get("fix") or {}).get("message"),
            )
        )
    return findings


# ---------------------------------------------------------------------------
# sqlfluff (SQL)
# ---------------------------------------------------------------------------


def _sqlfluff_severity(code: str) -> str:
    # sqlfluff doesn't emit security findings; treat all as warnings/info
    if code.startswith("PRS"):  # parser errors → can't lint, surface as critical
        return "critical"
    return "warning"


def run_sqlfluff(files: list[Path]) -> list["ReviewFinding"]:
    """Run ``sqlfluff lint --format json`` on SQL files."""
    from lakelogic.ai.code_reviewer import ReviewFinding

    sql_files = [str(f) for f in files if f.suffix == ".sql"]
    if not sql_files:
        return []

    try:
        proc = subprocess.run(
            ["sqlfluff", "lint", "--format", "json", "--dialect", "ansi", *sql_files],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except FileNotFoundError:
        logger.warning("sqlfluff not installed; skipping SQL lint. pip install lakelogic[review]")
        return []
    except subprocess.TimeoutExpired:
        logger.warning("sqlfluff timed out; skipping SQL lint")
        return []

    if not proc.stdout.strip():
        return []

    try:
        raw = json.loads(proc.stdout)
    except json.JSONDecodeError:
        logger.warning(f"sqlfluff returned non-JSON output: {proc.stdout[:200]}")
        return []

    findings: list[ReviewFinding] = []
    for file_entry in raw:
        filepath = file_entry.get("filepath", "")
        for v in file_entry.get("violations", []):
            code = v.get("code", "unknown")
            findings.append(
                ReviewFinding(
                    file=filepath,
                    line=v.get("line_no"),
                    severity=_sqlfluff_severity(code),
                    category="sql_quality",
                    rule=f"sqlfluff_{code.lower()}",
                    message=v.get("description", ""),
                )
            )
    return findings


# ---------------------------------------------------------------------------
# PII pattern scanner
# ---------------------------------------------------------------------------

# Compile once. Patterns are intentionally conservative — false positives are
# louder than false negatives in a code review context.
_PII_PATTERNS: dict[str, re.Pattern[str]] = {
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "ssn_us": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "phone_us": re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}\b"),
    "credit_card": re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
}

# Skip pattern: lines that look like they contain test fixtures, comments
# explicitly marked as examples, or known-safe placeholders.
_PII_SKIP = re.compile(r"(example|fixture|placeholder|test|@example\.com)", re.IGNORECASE)


def scan_pii_patterns(files: list[Path]) -> list["ReviewFinding"]:
    """Scan source files for PII-like literal patterns."""
    from lakelogic.ai.code_reviewer import ReviewFinding

    scannable = [f for f in files if f.suffix in {".py", ".sql", ".yaml", ".yml", ".json", ".tf"}]
    findings: list[ReviewFinding] = []

    for path in scannable:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue

        for lineno, line in enumerate(text.splitlines(), start=1):
            if _PII_SKIP.search(line):
                continue
            for name, pattern in _PII_PATTERNS.items():
                if pattern.search(line):
                    findings.append(
                        ReviewFinding(
                            file=str(path),
                            line=lineno,
                            severity="critical" if name in {"ssn_us", "credit_card"} else "warning",
                            category="security",
                            rule=f"pii_{name}",
                            message=f"Possible {name.replace('_', ' ')} literal detected.",
                            suggestion="Move to a fixture, secrets manager, or environment variable.",
                        )
                    )
                    break  # one finding per line is enough
    return findings


# ---------------------------------------------------------------------------
# Performance smells — cheap regex-based checks for common data-eng perf bugs
# ---------------------------------------------------------------------------


_PYSPARK_IMPORT = re.compile(r"^\s*(import\s+pyspark|from\s+pyspark)", re.MULTILINE)
_COMMENT_PREFIX = re.compile(r"^\s*#")

# Patterns evaluated per-line against the *code* of a Python file.
# Tuple shape: (rule_id, severity, regex, message, suggestion,
#               requires_pyspark, skip_test_files)
_PERF_PATTERNS: list[tuple[str, str, re.Pattern[str], str, str, bool, bool]] = [
    (
        "spark_collect",
        "warning",
        re.compile(r"\.collect\s*\("),
        "PySpark .collect() pulls the entire DataFrame to the driver — risk of OOM on large data.",
        "Use .take(n), .show(), or write to storage and read back instead.",
        True,
        False,
    ),
    (
        "spark_to_pandas",
        "warning",
        re.compile(r"\.toPandas\s*\("),
        "PySpark .toPandas() materialises the whole DataFrame in driver memory.",
        "Use .limit(n).toPandas() or process in Spark and write out.",
        True,
        False,
    ),
    (
        "spark_count_for_bool",
        "info",
        re.compile(r"if\s+\w+\.count\s*\(\s*\)\s*[><=!]"),
        "df.count() triggers a full scan; using it in a boolean check is wasteful.",
        "Use df.head(1) or df.isEmpty() instead.",
        True,
        False,
    ),
    (
        "pandas_read_csv_no_chunksize",
        "info",
        re.compile(r"(?<!pl\.)\bread_csv\s*\((?![^)]*chunksize\s*=)"),
        "pd.read_csv without chunksize loads the whole file into memory.",
        "Pass chunksize=N for large files, or switch to polars/duckdb.",
        False,
        False,
    ),
    (
        "pandas_iterrows",
        "warning",
        re.compile(r"\.(?:iterrows|itertuples)\s*\("),
        "Iterating row-by-row in pandas is up to 100× slower than vectorised ops.",
        "Use vectorised operations, .apply(), or switch to polars.",
        False,
        False,
    ),
    (
        "spark_coalesce_one",
        "warning",
        re.compile(r"\.(?:coalesce|repartition)\s*\(\s*1\s*\)"),
        "coalesce(1)/repartition(1) collapses to a single task — kills parallelism.",
        "Drop the call, or use a small partition count tuned to output size.",
        True,
        False,
    ),
    (
        "spark_show_in_prod",
        "info",
        re.compile(r"\.(?:show|printSchema)\s*\("),
        ".show()/.printSchema() trigger driver-side work; they belong in notebooks/tests, not production pipelines.",
        "Remove before merging, or guard behind a debug flag.",
        True,
        True,
    ),
    (
        "glob_on_cloud_storage",
        "warning",
        re.compile(r"glob\.(?:glob|iglob)\s*\(\s*['\"](?:s3|gs|abfss?|wasbs?)://"),
        "glob.glob() on cloud storage URLs is extremely slow — listing dominates over reading.",
        "Use the engine's native path globbing (spark.read.parquet path patterns, fsspec, boto3 paginator).",
        False,
        False,
    ),
]


_TEST_PATH_PARTS = {"test", "tests", "_smoke", "fixtures"}


def _is_test_file(path: Path) -> bool:
    parts = {p.lower() for p in path.parts}
    return bool(parts & _TEST_PATH_PARTS) or path.name.startswith("test_")


def scan_perf_smells(files: list[Path]) -> list["ReviewFinding"]:
    """Cheap regex-based scan for common data-engineering performance smells.

    Runs on Python files only. Spark-specific checks fire only when the file
    imports ``pyspark`` (cuts false positives in non-Spark code).
    """
    from lakelogic.ai.code_reviewer import ReviewFinding

    py_files = [f for f in files if f.suffix == ".py"]
    findings: list[ReviewFinding] = []

    for path in py_files:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue

        is_pyspark = bool(_PYSPARK_IMPORT.search(text))
        is_test = _is_test_file(path)

        for lineno, line in enumerate(text.splitlines(), start=1):
            if _COMMENT_PREFIX.match(line):
                continue
            for rule, sev, pattern, msg, fix, needs_spark, skip_tests in _PERF_PATTERNS:
                if needs_spark and not is_pyspark:
                    continue
                if skip_tests and is_test:
                    continue
                if pattern.search(line):
                    findings.append(
                        ReviewFinding(
                            file=str(path),
                            line=lineno,
                            severity=sev,
                            category="performance",
                            rule=rule,
                            message=msg,
                            suggestion=fix,
                        )
                    )
    return findings


# ---------------------------------------------------------------------------
# AST: withColumn inside a for-loop — explodes Spark query plan
# ---------------------------------------------------------------------------


def scan_withcolumn_in_loop(files: list[Path]) -> list["ReviewFinding"]:
    """Flag ``.withColumn(...)`` calls inside ``for``/``while`` loops.

    Each call adds a node to Spark's logical plan. In a loop this produces a
    pathologically large plan — slow to optimise, sometimes a planner crash.
    """
    from lakelogic.ai.code_reviewer import ReviewFinding

    findings: list[ReviewFinding] = []
    for path in [f for f in files if f.suffix == ".py"]:
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
            tree = ast.parse(source, filename=str(path))
        except (OSError, SyntaxError):
            continue

        for node in ast.walk(tree):
            if not isinstance(node, (ast.For, ast.While)):
                continue
            for child in ast.walk(node):
                if (
                    isinstance(child, ast.Call)
                    and isinstance(child.func, ast.Attribute)
                    and child.func.attr == "withColumn"
                ):
                    findings.append(
                        ReviewFinding(
                            file=str(path),
                            line=child.lineno,
                            severity="warning",
                            category="performance",
                            rule="withcolumn_in_loop",
                            message=(
                                ".withColumn() inside a loop adds one logical-plan node per "
                                "iteration — slow planning, risk of stack overflow on large loops."
                            ),
                            suggestion=(
                                "Use .select() with a list comprehension of columns, "
                                "or chain .withColumns({...}) once."
                            ),
                        )
                    )
                    break  # one finding per loop is enough
    return findings


# ---------------------------------------------------------------------------
# Unused .cache() / .persist() — AST-based, single-file scope
# ---------------------------------------------------------------------------


_CACHE_METHODS = {"cache", "persist"}


def _ends_with_cache(node: ast.AST) -> Optional[str]:
    """If ``node`` is a Call whose final attribute is .cache()/.persist(),
    return the method name; else None.
    """
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr in _CACHE_METHODS:
            return node.func.attr
    return None


def _count_loads(tree: ast.AST, name: str, after_line: int) -> int:
    """Count ast.Name(Load) references to ``name`` after ``after_line``."""
    count = 0
    for n in ast.walk(tree):
        if (
            isinstance(n, ast.Name)
            and n.id == name
            and isinstance(n.ctx, ast.Load)
            and getattr(n, "lineno", 0) > after_line
        ):
            count += 1
    return count


def scan_unused_cache(files: list[Path]) -> list["ReviewFinding"]:
    """Flag ``var = X.cache()`` (or .persist()) where ``var`` is read 0-1 times.

    Heuristic: a cache that's used once or never adds materialisation cost
    with no reuse benefit — usually a copy-paste leftover or a misunderstanding
    of when caching pays off (caching pays when a DataFrame is reused 2+ times).
    """
    from lakelogic.ai.code_reviewer import ReviewFinding

    findings: list[ReviewFinding] = []
    for path in [f for f in files if f.suffix == ".py"]:
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
            tree = ast.parse(source, filename=str(path))
        except (OSError, SyntaxError):
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            method = _ends_with_cache(node.value)
            if not method:
                continue
            # Only handle the simple `var = ...cache()` form
            if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
                continue
            var_name = node.targets[0].id
            uses = _count_loads(tree, var_name, after_line=node.lineno)

            if uses == 0:
                msg = (
                    f"`{var_name}.{method}()` result is never read. "
                    "Caching only pays off when the DataFrame is reused 2+ times."
                )
                sev = "warning"
            elif uses == 1:
                msg = (
                    f"`{var_name}.{method}()` is only used once after caching. "
                    "Caching adds materialisation overhead with no reuse benefit."
                )
                sev = "info"
            else:
                continue

            findings.append(
                ReviewFinding(
                    file=str(path),
                    line=node.lineno,
                    severity=sev,
                    category="performance",
                    rule=f"unused_{method}",
                    message=msg,
                    suggestion=f"Remove the .{method}() call, or refactor to reuse the cached DataFrame.",
                )
            )
    return findings


# ---------------------------------------------------------------------------
# datacontract-cli — schema breaking-change detection (P4)
# ---------------------------------------------------------------------------


_DATACONTRACT_SUFFIXES = {".datacontract.yaml", ".datacontract.yml"}


def _is_datacontract(path: Path) -> bool:
    name = path.name.lower()
    return any(name.endswith(suf) for suf in _DATACONTRACT_SUFFIXES)


def _git_show(ref: str, path: Path) -> Optional[str]:
    """Return the file content at ``ref`` or None if it didn't exist."""
    try:
        proc = subprocess.run(
            ["git", "show", f"{ref}:{path.as_posix()}"],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout


# datacontract diff text → severity mapping. Conservative defaults; the tool
# emits human-readable lines like "ERROR: ...", "WARNING: ...", "INFO: ...".
_DC_SEVERITY = {
    "error": "critical",
    "fatal": "critical",
    "warning": "warning",
    "warn": "warning",
    "info": "info",
}


def _parse_datacontract_diff(text: str, file_path: str) -> list["ReviewFinding"]:
    from lakelogic.ai.code_reviewer import ReviewFinding

    findings: list[ReviewFinding] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        # Lines look like "ERROR: field 'x' was removed" / "WARNING: type changed"
        head, _, msg = stripped.partition(":")
        sev = _DC_SEVERITY.get(head.strip().lower())
        if not sev or not msg:
            continue
        findings.append(
            ReviewFinding(
                file=file_path,
                severity=sev,
                category="governance",
                rule="datacontract_breaking_change",
                message=msg.strip(),
                suggestion="Bump the contract version and notify downstream consumers.",
            )
        )
    return findings


def run_datacontract_diff(files: list[Path], *, base_ref: Optional[str] = None) -> list["ReviewFinding"]:
    """Run ``datacontract diff <base> <head>`` on each changed contract file.

    Args:
        files: All files in the review batch; only ``*.datacontract.yaml``
            entries are processed.
        base_ref: Git ref containing the previous version of each contract.
            If None, no diff is run (we still need a baseline to compare against).
    """
    contracts = [f for f in files if _is_datacontract(f)]
    if not contracts or not base_ref:
        return []

    findings: list = []
    for contract in contracts:
        base_content = _git_show(base_ref, contract)
        if base_content is None:
            continue  # newly added contract — nothing to diff against

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
            tmp.write(base_content)
            base_path = Path(tmp.name)

        try:
            try:
                proc = subprocess.run(
                    ["datacontract", "diff", str(base_path), str(contract)],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
            except FileNotFoundError:
                logger.warning(
                    "datacontract CLI not installed; skipping breaking-change check. pip install lakelogic[review]"
                )
                return []
            except subprocess.TimeoutExpired:
                logger.warning(f"datacontract diff timed out on {contract}")
                continue
            output = (proc.stdout or "") + "\n" + (proc.stderr or "")
            findings.extend(_parse_datacontract_diff(output, str(contract)))
        finally:
            try:
                base_path.unlink()
            except OSError:
                pass

    return findings
