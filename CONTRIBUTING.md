# Contributing to LakeLogic

Thank you for your interest in contributing to LakeLogic! 🎉

We welcome contributions from the community and are pleased to have you join us.

## 📋 Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Ways to Contribute](#ways-to-contribute)
- [Development Setup](#development-setup)
- [Pull Request Process](#pull-request-process)
- [Code Style Guidelines](#code-style-guidelines)
- [Testing Guidelines](#testing-guidelines)
- [Documentation](#documentation)
- [Community](#community)

---

## 📜 Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to conduct@lakelogic.org.

---

## 🎯 Ways to Contribute

There are many ways to contribute to LakeLogic:

### 🐛 Report Bugs
Found a bug? Please [open an issue](https://github.com/lakelogic/LakeLogic/issues/new?template=bug_report.md) with:
- Clear description of the issue
- Steps to reproduce
- Expected vs actual behavior
- Environment details (OS, Python version, engine)

### 💡 Suggest Features
Have an idea? [Start a discussion](https://github.com/lakelogic/LakeLogic/discussions/new?category=ideas) or [open a feature request](https://github.com/lakelogic/LakeLogic/issues/new?template=feature_request.md)

### 📝 Improve Documentation
- Fix typos or unclear explanations
- Add examples or tutorials
- Improve API documentation
- Translate documentation

### 🔧 Submit Code
- Fix bugs
- Implement features
- Add engine support
- Improve performance

### 💬 Help Others
- Answer questions in [Discussions](https://github.com/lakelogic/LakeLogic/discussions)
- Help triage issues
- Review pull requests

---

## 🛠️ Development Setup

### Prerequisites
- Python 3.9 or higher
- [uv](https://github.com/astral-sh/uv) (recommended) or pip
- Git

### Setup Steps

1. **Fork the repository**
   - Click "Fork" button on [GitHub](https://github.com/lakelogic/LakeLogic)

2. **Clone your fork**
   ```bash
   git clone https://github.com/YOUR-USERNAME/LakeLogic.git
   cd lakelogic
   ```

3. **Add upstream remote**
   ```bash
   git remote add upstream https://github.com/lakelogic/LakeLogic.git
   ```

4. **Install dependencies** (using uv - recommended)
   ```bash
   # Install uv if you don't have it
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # Sync all dependencies
   uv sync --all-extras
   ```

   Or using pip:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -e ".[dev,all]"
   ```

5. **Verify installation**
   ```bash
   uv run pytest
   ```

---

## 🔄 Pull Request Process

### Before You Start
1. Check existing [issues](https://github.com/lakelogic/LakeLogic/issues) and [PRs](https://github.com/lakelogic/LakeLogic/pulls)
2. For major changes, open an issue first to discuss your approach
3. Ensure your fork is up to date with upstream

### Development Workflow

1. **Create a branch**
   ```bash
   git checkout -b feature/my-awesome-feature
   # or
   git checkout -b fix/bug-description
   ```

2. **Make your changes**
   - Write clear, concise code
   - Add tests for new functionality
   - Update documentation as needed
   - Follow our [code style guidelines](#code-style-guidelines)

3. **Test your changes**
   ```bash
   # Run tests
   uv run pytest

   # Run specific test file
   uv run pytest tests/test_processor.py

   # Run with coverage
   uv run pytest --cov=lakelogic --cov-report=html
   ```

4. **Lint your code**
   ```bash
   # Format with black
   uv run black lakelogic tests

   # Check with ruff
   uv run ruff check lakelogic tests

   # Type check with mypy (optional but encouraged)
   uv run mypy lakelogic
   ```

5. **Commit your changes**
   ```bash
   git add .
   git commit -m "feat: add support for X engine"
   ```

   **Commit message format:**
   - `feat:` New feature
   - `fix:` Bug fix
   - `docs:` Documentation changes
   - `test:` Test additions/changes
   - `refactor:` Code refactoring
   - `perf:` Performance improvements
   - `chore:` Maintenance tasks

6. **Push to your fork**
   ```bash
   git push origin feature/my-awesome-feature
   ```

7. **Create Pull Request**
   - Go to [LakeLogic repository](https://github.com/lakelogic/LakeLogic)
   - Click "New Pull Request"
   - Select your fork and branch
   - Fill out the PR template
   - Link related issues

### PR Review Process
- Maintainers will review your PR within 2-3 business days
- Address feedback by pushing new commits
- Once approved, a maintainer will merge your PR
- Your contribution will be included in the next release!

---

## 🎨 Code Style Guidelines

### Python Style
- Follow [PEP 8](https://peps.python.org/pep-0008/)
- Use [Black](https://black.readthedocs.io/) for formatting (line length: 88)
- Use [Ruff](https://docs.astral.sh/ruff/) for linting
- Add type hints to all public functions
- Write docstrings for all public classes and functions

### Docstring Format
```python
def my_function(param1: str, param2: int) -> bool:
    """
    Brief description of what the function does.

    Args:
        param1: Description of param1
        param2: Description of param2

    Returns:
        Description of return value

    Raises:
        ValueError: When param2 is negative

    Example:
        >>> my_function("test", 5)
        True
    """
    pass
```

### Naming Conventions
- **Classes**: `PascalCase` (e.g., `DataProcessor`)
- **Functions/Methods**: `snake_case` (e.g., `load_contract`)
- **Constants**: `UPPER_CASE` (e.g., `DEFAULT_TIMEOUT`)
- **Private members**: `_leading_underscore` (e.g., `_internal_method`)

### Code Organization
- Keep functions focused and under 50 lines when possible
- Extract complex logic into helper functions
- Use early returns to reduce nesting
- Avoid deep nesting (max 3-4 levels)

---

## 🧪 Testing Guidelines

### Test Requirements
- All new features must include tests
- Bug fixes should include regression tests
- Aim for >80% code coverage
- Tests should be fast (under 5 seconds total)

### Test Structure
```python
import pytest
from lakelogic import DataProcessor

def test_feature_name():
    """Test that feature works correctly"""
    # Arrange
    processor = DataProcessor(contract="test_contract.yaml")

    # Act
    result = processor.run_source()

    # Assert
    assert result is not None
```

### Running Tests
```bash
# All tests
uv run pytest

# Specific file
uv run pytest tests/test_processor.py

# Specific test
uv run pytest tests/test_processor.py::test_feature_name

# With coverage
uv run pytest --cov=lakelogic --cov-report=html

# Watch mode (re-run on file changes)
uv run pytest-watch
```

### Test Files
- Place tests in `tests/` directory
- Name test files `test_*.py`
- Name test functions `test_*`
- Use fixtures for reusable test data

---

## 📚 Documentation

### When to Update Docs
- Adding new features → Update relevant docs + add example
- Changing APIs → Update API reference
- Fixing bugs → Update troubleshooting guide if applicable
- Adding new engine → Update capabilities matrix

### Documentation Structure
- `README.md` - Project overview and quick start
- `docs/` - Main documentation (MkDocs)
- `examples/` - Tutorials and examples
- Docstrings - API documentation

### Building Docs Locally
```bash
# Install docs dependencies
uv pip install -e ".[docs]"

# Build docs
mkdocs build

# Serve docs locally (http://127.0.0.1:8000)
mkdocs serve
```

---

## 💬 Community

### Get Help
- [GitHub Discussions](https://github.com/lakelogic/LakeLogic/discussions) - Q&A and general discussions
- [Discord](https://discord.gg/lakelogic) - Real-time chat (coming soon)
- [Documentation](https://LakeLogic.github.io/LakeLogic) - Comprehensive guides

### Stay Updated
- Watch the [repository](https://github.com/lakelogic/LakeLogic) for updates
- Follow [@lakelogic](https://twitter.com/lakelogic) on Twitter
- Subscribe to release notifications

---

## 🏆 Recognition

Contributors will be:
- Listed in our [Contributors](https://github.com/lakelogic/LakeLogic/graphs/contributors) page
- Mentioned in release notes for significant contributions
- Eligible for contributor swag (coming soon!)

---

## 📞 Questions?

- **Technical questions**: [GitHub Discussions](https://github.com/lakelogic/LakeLogic/discussions)
- **Security issues**: security@lakelogic.org
- **Code of Conduct**: conduct@lakelogic.org
- **General inquiries**: hello@lakelogic.org

---

## 📄 License

By contributing to LakeLogic, you agree that your contributions will be licensed under the [MIT License](LICENSE).

---

**Thank you for contributing to LakeLogic! Together we're building the future of data quality.** 🚀
