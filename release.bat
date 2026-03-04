@echo off
REM -- LakeLogic Release Script ------------------------------------------
REM Usage:   release.bat           (auto-detect bump: patch/minor/major)
REM          release.bat minor     (force minor bump)
REM          release.bat major     (force major bump)
REM
REM What it does:
REM   1. cz bump          - bumps version in pyproject.toml + creates git tag
REM   2. git cliff        - regenerates CHANGELOG.md from all tags
REM   3. git commit amend - folds changelog into the bump commit
REM   4. git push         - pushes everything including tags
REM -----------------------------------------------------------------------

echo.
echo ======================================================
echo   LakeLogic Release
echo ======================================================

REM Step 1: Bump version
echo.
echo [1/4] Bumping version...
if "%1"=="" (
    cz bump --yes
) else (
    cz bump --increment %1 --yes
)
if errorlevel 1 (
    echo.
    echo ERROR: cz bump failed.
    echo.
    echo Common causes:
    echo   - No new commits since last tag: commit your changes first
    echo   - Commits not in conventional format: use 'cz commit' or 'feat:/fix:' prefixes
    echo.
    exit /b 1
)

REM Step 2: Generate changelog
echo.
echo [2/4] Generating changelog with git-cliff...
git cliff -o CHANGELOG.md
if errorlevel 1 (
    echo ERROR: git cliff failed. Is git-cliff installed? Run: winget install git-cliff
    exit /b 1
)

REM Step 3: Amend the bump commit to include changelog
echo.
echo [3/4] Amending bump commit with changelog...
git add CHANGELOG.md
git commit --amend --no-edit

REM Step 4: Push
echo.
echo [4/4] Pushing to remote...
git push --follow-tags

echo.
echo ======================================================
echo   Release complete!
echo ======================================================
echo.
