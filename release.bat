@echo off
REM mkdocs serve --dev-addr 127.0.0.1:9000
REM -- LakeLogic Release Script ------------------------------------------
REM Usage:   release.bat           (auto-detect bump: patch/minor/major)
REM          release.bat minor     (force minor bump)
REM          release.bat major     (force major bump)
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

echo.
echo ======================================================
echo   LakeLogic Release
echo ======================================================

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
python -m pytest tests/ -x -q --tb=short
if errorlevel 1 (
    echo.
    echo ERROR: Tests failed after dependency upgrade. Review uv.lock changes.
    echo   To revert: git checkout uv.lock
    exit /b 1
)

REM Step 0c: Lint and format (auto-fix safe issues)
echo.
echo [0c/6] Running ruff lint and format...
ruff check . --exclude _ref --fix --quiet
ruff format . --exclude _ref --quiet
git add -u

REM Step 1: Bump version (creates tag + bump commit)
echo.
echo [1/6] Bumping version...
if "%1"=="" (
    python -m commitizen bump --yes
) else (
    python -m commitizen bump --increment %1 --yes
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
git push --force --follow-tags

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
echo     5. git push --force --follow-tags
echo.
