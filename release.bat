@echo off
REM mkdocs serve --dev-addr 127.0.0.1:9000
REM -- LakeLogic Release Script ------------------------------------------
REM Usage:   release.bat           (auto-detect bump: patch/minor/major)
REM          release.bat minor     (force minor bump)
REM          release.bat major     (force major bump)
REM          release.bat --with-spark  (optionally with minor or major as well)
REM                                (also run the real-Spark tests and Spark conformance)
REM
REM Spark is OFF by default. A change that touches Spark is proven on Spark BEFORE it gets
REM here (RUN_SPARK_TESTS=1 pytest tests/, and the OLC conformance suite with
REM OLC_CONFORMANCE_ENGINES=polars,duckdb,spark); the release re-runs Polars and DuckDB, which
REM take minutes. --with-spark adds Spark for a release that wants the full proof again
REM (about an hour longer).
REM
REM Prerequisites:
REM   pip install commitizen
REM   winget install git-cliff     (or: cargo install git-cliff)
REM
REM What it does:
REM   0.  uv lock --upgrade   - pull latest patched dependency versions
REM   0b. pytest              - compatibility check (abort if tests fail)
REM   0c. ruff check/format   - auto-fix lint & formatting
REM   1.  cz bump             - bumps version in pyproject.toml + creates git tag
REM   2.  git cliff           - regenerates CHANGELOG.md from all tags
REM   3.  git commit amend    - folds changelog into the bump commit
REM   4.  git tag -f          - re-attaches the tag to the amended commit
REM   5.  git push            - pushes everything including tags
REM
REM -----------------------------------------------------------------------
REM IMPORTANT: Commit Message Format
REM -----------------------------------------------------------------------
REM Commits MUST use conventional format for the changelog to work.
REM git-cliff parses commit messages to generate the changelog.
REM Non-conventional commits are grouped under "Other".
REM
REM Format:  type(scope): description
REM
REM   Types:
REM     feat:     New feature          -> "Added" in changelog
REM     fix:      Bug fix              -> "Fixed" in changelog
REM     docs:     Documentation only   -> "Documentation" in changelog
REM     refactor: Code restructuring   -> "Changed" in changelog
REM     perf:     Performance          -> "Performance" in changelog
REM     test:     Adding/fixing tests  -> "Testing" in changelog
REM     ci:       CI/CD changes        -> "CI/CD" in changelog
REM     build:    Build system changes -> "Build" in changelog
REM     chore:    Maintenance          -> (skipped in changelog)
REM
REM   Examples:
REM     feat: add table_properties to Materialization model
REM     feat(ddl): emit TBLPROPERTIES for Spark and Databricks
REM     fix: column comments not applied after SCD2 merge
REM     feat(materialization): add delta compaction with auto mode
REM     docs: update contract YAML examples with compaction config
REM     refactor(processor): extract metadata application to helper
REM     perf: skip compaction when delta table has fewer than 10 files
REM
REM   When using AI-assisted commits (Antigravity/Copilot/Cursor):
REM     The auto-generated messages often lack the conventional prefix.
REM     Before running release.bat, either:
REM       1. Use 'cz commit' for guided conventional commits, or
REM       2. Manually prefix: git commit -m "feat: <your message>"
REM       3. Or amend the last commit: git commit --amend -m "feat: ..."
REM -----------------------------------------------------------------------

REM Always operate on THIS repo, never on whatever directory the caller happens to
REM be standing in. Every step below uses relative paths (tests/, pyproject.toml), so
REM running this from another checkout silently pointed all of them somewhere else.
REM That is not hypothetical: the sibling OLC release.bat was run from THIS repo and
REM cheerfully ran LakeLogic's 1814-test suite as the OLC release gate before failing
REM on a missing path. The failure was the lucky part — `uv lock --upgrade` and the
REM test gate had already run against the wrong repository and reported green.
cd /d "%~dp0"

REM Arguments, in any order: an optional bump (minor/major) and an optional --with-spark.
set "BUMP="
set "WITH_SPARK="
:parse_args
if "%~1"=="" goto args_done
if /i "%~1"=="--with-spark" (
    set "WITH_SPARK=1"
) else (
    set "BUMP=%~1"
)
shift
goto parse_args
:args_done

REM Use THIS repo's virtualenv, not whatever `python` happens to be first on PATH.
REM That is not hypothetical either: on the release box `python` resolved to a
REM SIBLING project's .venv (LakeLogic_SaaS), which has no google-cloud-bigquery —
REM so the compatibility gate failed a BigQuery test that passes here, and blocked
REM a release over an environment it was never meant to be testing. The same wrong
REM interpreter could just as easily pass a gate it should have failed.
set "PY=python"
if exist "%~dp0.venv\Scripts\python.exe" set "PY=%~dp0.venv\Scripts\python.exe"
echo   Python: %PY%

echo.
echo ======================================================
echo   LakeLogic Release
echo ======================================================
echo   Repo: %CD%
echo ======================================================

REM Step 0a: Start from what is on origin/main.
REM The publish workflow pushes a "docs: update changelog" commit to main after every release,
REM so a local main is behind origin after each one. The push at the end used --force to get
REM past that, which overwrote whatever had landed on main since (it wiped 0d2ca10, 2026-10-14).
REM Rebasing here instead means the final push is a plain one that can never overwrite.
echo.
echo [0a/6] Updating from origin/main...
git pull --rebase origin main
if errorlevel 1 (
    echo ERROR: could not rebase onto origin/main. Commit or set aside your changes and resolve, then re-run.
    exit /b 1
)

REM Step 0: Upgrade dependencies (pull latest security patches)
echo.
echo [0/6] Upgrading dependencies (security patches)...
uv lock --upgrade
if errorlevel 1 (
    echo WARNING: uv lock --upgrade failed. Continuing with existing lockfile.
)
git add uv.lock

REM Step 0b: Compatibility check — abort release if upgraded deps break tests
echo.
echo [0b/6] Running compatibility tests...
REM No -x, and one retry of the failures in a FRESH process. On Windows the local Spark
REM JVM can drop its py4j connection late in a long run ("connection forcibly closed"),
REM which fails whichever Spark test is running and every Spark test after it. That is
REM not hypothetical: it blocked a release at test 1215 of ~2450 while the same test
REM passed alone in 10s. A real failure fails twice and still stops the release.
REM
REM Real-Spark tests are skipped unless RUN_SPARK_TESTS=1, which --with-spark sets. They are
REM run BEFORE a release for any change that touches Spark (see Usage at the top).
set "RUN_SPARK_TESTS="
if defined WITH_SPARK (
    set "RUN_SPARK_TESTS=1"
    echo   Real-Spark tests: ON ^(--with-spark^)
) else (
    echo   Real-Spark tests: off ^(run them before releasing a Spark change; --with-spark to include^)
)
REM --cache-clear: the retry below reads THIS run's failures, never a list left by an earlier run.
REM pytest exit codes: 0 passed, 1 tests failed, 2+ interrupted / internal / usage / no tests.
REM Only 1 is retried; anything else stops the release. A run that was stopped or hung and
REM killed must never count as finished (2026-10-14: a stopped conformance step was retried
REM against a stale failure list, "passed", and 1.79.0 shipped without its Spark gate).
"%PY%" -m pytest tests/ -q --tb=short --cache-clear
set "TEST_RC=%ERRORLEVEL%"
if %TEST_RC% GTR 1 (
    echo.
    echo ERROR: the test run did not finish ^(pytest exit %TEST_RC%^). Not releasing.
    exit /b 1
)
if %TEST_RC% EQU 1 (
    echo.
    echo Retrying the failed tests once in a fresh process...
    REM If the run recorded no failures ^(it was cut short^), --lfnf all reruns everything.
    "%PY%" -m pytest tests/ --lf --lfnf all -q --tb=short
    if errorlevel 1 (
        echo.
        echo ERROR: Tests failed twice after dependency upgrade. Review uv.lock changes.
        echo   To revert: git checkout uv.lock
        exit /b 1
    )
)

REM Step 0b2: OLC conformance
REM The same contract, the same input, one expected result, run on every engine named.
REM Core's own tests mostly run on Polars; this is where an engine that reads a file, applies a
REM rule or merges a delete differently is caught. OLC_CONFORMANCE_REQUIRE=1 makes a skipped
REM case a failure, so a missing engine cannot pass for a green run. Polars and DuckDB take
REM about two minutes; --with-spark adds Spark (about an hour).
set "OLC_CONFORMANCE_ENGINES=polars,duckdb"
if defined WITH_SPARK set "OLC_CONFORMANCE_ENGINES=polars,duckdb,spark"
echo.
echo [0b2/6] Running OLC conformance on %OLC_CONFORMANCE_ENGINES%...
set "OLC_REPO=%CD%\..\open-lakehouse-contract"
if not exist "%OLC_REPO%\conformance\test_conformance.py" (
    echo ERROR: OLC conformance suite not found at %OLC_REPO%\conformance.
    echo   Clone open-lakehouse-contract next to this repo. A release does not ship untested.
    exit /b 1
)
set "OLC_CONFORMANCE_REQUIRE=1"
"%PY%" -m pytest "%OLC_REPO%\conformance" -q --tb=short --cache-clear
set "CONF_RC=%ERRORLEVEL%"
if %CONF_RC% GTR 1 (
    echo.
    echo ERROR: OLC conformance did not finish ^(pytest exit %CONF_RC%^). Not releasing.
    exit /b 1
)
if %CONF_RC% EQU 1 (
    echo.
    echo Retrying the failed conformance cases once in a fresh process...
    "%PY%" -m pytest "%OLC_REPO%\conformance" --lf --lfnf all -q --tb=short
    if errorlevel 1 (
        echo.
        echo ERROR: OLC conformance failed twice. An engine disagrees with the contract.
        exit /b 1
    )
)

REM Step 0c: Lint and format (auto-fix safe issues)
echo.
echo [0c/6] Running ruff lint and format...
REM Through this repo's venv for the same reason as %PY%: bare `ruff` resolved to the
REM SaaS venv's 0.16, outside this repo's pin (<0.14), and would reformat to its rules.
"%PY%" -m ruff check . --exclude _ref --fix --quiet
"%PY%" -m ruff format . --exclude _ref --quiet
git add -u

REM Step 1: Bump version (creates tag + bump commit)
echo.
echo [1/6] Bumping version...
if "%BUMP%"=="" (
    "%PY%" -m commitizen bump --yes
) else (
    "%PY%" -m commitizen bump --increment %BUMP% --yes
)
set "BUMP_RC=%ERRORLEVEL%"

REM cz exit code 21 = NoneIncrementExit: commits exist since the last tag but none of
REM them bump the version (docs/style/refactor/chore/test only). That is NOT a failure --
REM there is simply nothing to *release*. Exit cleanly so a docs-only batch doesn't look
REM like a broken build; just push those commits directly.
if "%BUMP_RC%"=="21" (
    echo.
    echo Nothing to release: no feat/fix commits since the last tag.
    echo   docs / style / refactor / chore / test commits do not bump the version.
    echo   Those commits are fine -- just push them:  git push
    echo.
    exit /b 0
)
if not "%BUMP_RC%"=="0" (
    echo.
    echo ERROR: cz bump failed ^(exit %BUMP_RC%^).
    echo.
    echo Common causes:
    echo   - Commits not in conventional format: use 'cz commit' or 'feat:/fix:' prefixes
    echo   - Tip: run 'git log --oneline' to check your commits
    echo.
    exit /b 1
)

REM Capture the new tag name (the latest tag on HEAD)
for /f "tokens=*" %%i in ('git describe --tags --abbrev^=0') do set NEW_TAG=%%i
echo   Tag: %NEW_TAG%

REM Step 2: Generate changelog (tag exists on HEAD, so git-cliff sees it)
echo.
echo [2/6] Generating changelog with git-cliff...
git cliff -o CHANGELOG.md
if errorlevel 1 (
    echo ERROR: git cliff failed. Is git-cliff installed? Run: winget install git-cliff
    exit /b 1
)

REM Step 3: Amend the bump commit to include changelog
REM         This creates a NEW commit hash, so the tag is now orphaned
echo.
echo [3/6] Amending bump commit with changelog...
git add CHANGELOG.md
git commit --amend --no-edit

REM Step 4: Re-attach the tag to the amended commit
REM         Without this, the tag points to the old (pre-amend) commit.
REM         Use an ANNOTATED tag (-a): `git push --follow-tags` (step 5) only
REM         pushes annotated tags, so a lightweight `git tag -f` would never reach
REM         origin and the tag-triggered publish workflow would not fire.
echo.
echo [4/6] Re-tagging %NEW_TAG% on amended commit...
git tag -f -a %NEW_TAG% -m "release %NEW_TAG%"

REM Step 5: Push
echo.
echo [5/6] Pushing to remote...
REM A plain push: never --force. If origin moved during the release, this fails safely; nothing
REM on origin is overwritten. Then: git pull --rebase origin main, re-tag, push again.
git push --follow-tags
if errorlevel 1 (
    echo.
    echo ERROR: push refused ^(origin/main moved during the release^). Nothing was overwritten.
    echo   git pull --rebase origin main
    echo   git tag -f -a %NEW_TAG% -m "release %NEW_TAG%"
    echo   git push --follow-tags
    exit /b 1
)

echo.
echo ======================================================
echo   Release %NEW_TAG% complete!
echo ======================================================
echo.
echo   To fix a bad changelog after release:
echo     1. Edit cliff.toml or reword commits
echo     2. git cliff -o CHANGELOG.md
echo     3. git add CHANGELOG.md ^& git commit --amend --no-edit
echo     4. git tag -f %NEW_TAG%
echo     5. git push --force-with-lease --follow-tags   ^(only if that commit is yours alone^)
echo.
