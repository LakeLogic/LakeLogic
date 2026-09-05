# LakeLogic Logging Configuration Guide

**Date:** February 2026  
**Purpose:** Configure logging behavior, message truncation, and output formats

---

## Default Logging Behavior

LakeLogic uses **Loguru** for structured logging with the following defaults:

```python
# Default configuration (lakelogic/cli/main.py)
logger.add(
    sys.stderr,
    level="INFO",  # or "DEBUG" with --verbose
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    filter=split_long_message,  # Splits long messages across multiple lines
)
```

**Default Settings:**
- **Log Level:** `INFO` (or `DEBUG` with `--verbose` flag)
- **Output:** `stderr` (console)
- **Max Line Length:** `120 characters` (split at word boundaries)
- **Format:** `HH:mm:ss | LEVEL | message`
- **Multi-line:** Continuation lines are indented with 2 spaces

---

## Multi-Line Splitting

### **Why Split Instead of Truncate?**

Multi-line splitting is superior to truncation because:
- ✅ **No data loss** - Full message is preserved
- ✅ **Better readability** - Natural word boundaries
- ✅ **Scannable** - Easy to skim long messages
- ✅ **Professional** - Looks cleaner than "... (truncated)"

### **How It Works**

Messages longer than `max_line_length` are automatically split at word boundaries:

**Before (single long line):**
```
12:18:48 | INFO     | lakelogic.core.processor.run() - Run complete. [domain=customers.parquet] Source: 1,000,000, Total (post-transform): 999,950, Good: 999,900, Quarantined: 50, Pre-Transform Dropped: 50, Ratio: 99.99%
```

**After (split across multiple lines):**
```
12:18:48 | INFO     | lakelogic.core.processor.run() - Run complete. [domain=customers.parquet] Source: 1,000,000, Total
  (post-transform): 999,950, Good: 999,900, Quarantined: 50, Pre-Transform Dropped: 50, Ratio: 99.99%
```

**Features:**
- ✅ Splits at **word boundaries** (never mid-word)
- ✅ **Indents continuation lines** (2 spaces) for visual hierarchy
- ✅ **Preserves full message** (no truncation)
- ✅ **Configurable line length** (default: 120 characters)

---

## Customizing Line Length

### **Option 1: Edit main.py (Permanent)**

Edit `lakelogic/cli/main.py` line 125:

```python
max_line_length = 80  # Narrow terminals (80 chars)
# or
max_line_length = 160  # Wide terminals (160 chars)
```

**Common Values:**
- `80` - Narrow terminals, mobile, strict formatting
- `120` - **Default** (standard terminals, good balance)
- `160` - Wide terminals, modern displays
- `200` - Very wide terminals, minimal splitting
- `999999` - Effectively no splitting (single-line logs)

---

### **Option 2: Environment Variable (Dynamic)**

Add support for environment variable configuration:

```python
# In lakelogic/cli/main.py
import os

max_line_length = int(os.getenv("LAKELOGIC_MAX_LINE_LENGTH", "120"))
```

**Usage:**
```bash
# Windows
set LAKELOGIC_MAX_LINE_LENGTH=160
lakelogic run --contract bronze_customers.yaml --source customers.csv

# Linux/Mac
export LAKELOGIC_MAX_LINE_LENGTH=160
lakelogic run --contract bronze_customers.yaml --source customers.csv
```

---

### **Option 3: CLI Flag (Per-Run)**

Add a `--max-log-length` flag:

```python
# In lakelogic/cli/main.py
@app.command()
def run(
    # ... existing parameters ...
    max_log_length: int = typer.Option(500, "--max-log-length", help="Maximum log message length (characters)."),
):
    # ... existing code ...
    max_message_length = max_log_length
```

**Usage:**
```bash
lakelogic run --contract bronze_customers.yaml --source customers.csv --max-log-length 1000
```

---

## Logging to File (No Truncation)

For production environments, log to file without truncation:

### **Option 1: Add File Handler**

```python
# In lakelogic/cli/main.py
logger.add(
    "logs/lakelogic_{time:YYYY-MM-DD}.log",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    rotation="1 day",
    retention="30 days",
    compression="zip",
    filter=None,  # No truncation for file logs
)
```

**Features:**
- ✅ Daily log rotation
- ✅ 30-day retention
- ✅ Automatic compression
- ✅ Full messages (no truncation)
- ✅ Structured format for parsing

---

### **Option 2: JSON Logs (Structured)**

For log aggregation (Datadog, Splunk, ELK):

```python
import json


def json_formatter(record):
    """Format log records as JSON."""
    return (
        json.dumps(
            {
                "timestamp": record["time"].isoformat(),
                "level": record["level"].name,
                "message": record["message"],
                "file": record["file"].name,
                "function": record["function"],
                "line": record["line"],
            }
        )
        + "\n"
    )


logger.add("logs/lakelogic.jsonl", level="DEBUG", format=json_formatter, serialize=True)
```

**Output:**
```json
{"timestamp": "2026-02-09T12:18:48.123456", "level": "INFO", "message": "Run complete. Source: 1000000, Good: 999900, Quarantined: 50", "file": "processor.py", "function": "run_source", "line": 234}
```

---

## Recommended Configurations

### **Development (Local)**
```python
# Console: Truncated, colored
logger.add(
    sys.stderr,
    level="DEBUG",
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    filter=truncate_message,  # 500 chars
)
```

---

### **Production (Server)**
```python
# Console: Truncated, minimal
logger.add(
    sys.stderr,
    level="INFO",
    format="{time:HH:mm:ss} | {level: <8} | {message}",
    filter=truncate_message,  # 500 chars
)

# File: Full messages, JSON
logger.add(
    "logs/lakelogic_{time:YYYY-MM-DD}.log",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
    rotation="1 day",
    retention="30 days",
    compression="zip",
    filter=None,  # No truncation
)
```

---

### **CI/CD (GitHub Actions)**
```python
# Console only, no colors, no truncation
logger.add(
    sys.stderr,
    level="INFO",
    format="{time:HH:mm:ss} | {level: <8} | {message}",
    colorize=False,
    filter=None,  # No truncation (CI logs are searchable)
)
```

---

## Troubleshooting

### **Problem: Messages still too long**

**Solution:** Reduce `max_message_length`:
```python
max_message_length = 200  # Very compact
```

---

### **Problem: Need full messages for debugging**

**Solution:** Use `--verbose` and log to file:
```bash
lakelogic run --contract bronze_customers.yaml --source customers.csv --verbose > debug.log 2>&1
```

Or add file handler:
```python
logger.add("debug.log", level="DEBUG", filter=None)
```

---

### **Problem: Logs are unreadable in production**

**Solution:** Use JSON logs for structured parsing:
```python
logger.add("logs/lakelogic.jsonl", level="INFO", serialize=True, format=json_formatter)
```

Then parse with `jq`:
```bash
cat logs/lakelogic.jsonl | jq '.message'
```

---

## Best Practices

### **1. Different Configs for Different Environments**

```python
import os

env = os.getenv("ENVIRONMENT", "development")

if env == "production":
    # Production: File logs, JSON, no truncation
    logger.add("logs/lakelogic.jsonl", level="INFO", serialize=True)
elif env == "development":
    # Development: Console, colored, truncated
    logger.add(sys.stderr, level="DEBUG", filter=truncate_message)
else:
    # CI/CD: Console, plain, no truncation
    logger.add(sys.stderr, level="INFO", colorize=False)
```

---

### **2. Separate Logs by Severity**

```python
# INFO and above to console (truncated)
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    filter=truncate_message,
)

# DEBUG and above to file (full)
logger.add(
    "logs/debug.log",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
    filter=None,
)

# ERROR and above to separate file (full)
logger.add(
    "logs/errors.log",
    level="ERROR",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}\n{exception}",
    filter=None,
)
```

---

### **3. Context-Aware Truncation**

Truncate only certain log levels:

```python
def truncate_message(record):
    """Truncate only INFO and DEBUG messages."""
    if record["level"].name in ["INFO", "DEBUG"]:
        message = record["message"]
        if len(message) > 500:
            record["message"] = message[:500] + f"... (truncated {len(message) - 500} chars)"
    return True  # Always log ERROR/WARNING in full
```

---

## Summary

| Configuration | Use Case | Max Length | Output |
|---------------|----------|------------|--------|
| **Default** | Local development | 500 chars | Console (colored) |
| **Verbose** | Debugging | 500 chars | Console (colored, DEBUG level) |
| **Production** | Server deployment | 500 chars (console), unlimited (file) | Console + File (JSON) |
| **CI/CD** | GitHub Actions | Unlimited | Console (plain) |
| **Custom** | Specific needs | User-defined | Configurable |

---

## Example: Full Production Setup

```python
import os
import sys
import json
from loguru import logger

# Remove default handler
logger.remove()

# Environment
env = os.getenv("ENVIRONMENT", "development")
max_message_length = int(os.getenv("LAKELOGIC_MAX_LOG_LENGTH", "500"))


# Truncation filter
def truncate_message(record):
    """Truncate log messages to prevent console overflow."""
    if record["level"].name in ["INFO", "DEBUG"]:
        message = record["message"]
        if len(message) > max_message_length:
            record["message"] = (
                message[:max_message_length] + f"... (truncated {len(message) - max_message_length} chars)"
            )
    return True


# Console handler (always)
logger.add(
    sys.stderr,
    level="INFO" if env == "production" else "DEBUG",
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    filter=truncate_message,
    colorize=(env != "ci"),
)

# File handler (production only)
if env == "production":
    logger.add(
        "logs/lakelogic_{time:YYYY-MM-DD}.log",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
        rotation="1 day",
        retention="30 days",
        compression="zip",
        filter=None,  # No truncation
    )

    # JSON logs for aggregation
    logger.add(
        "logs/lakelogic.jsonl",
        level="INFO",
        serialize=True,
        format=lambda record: (
            json.dumps(
                {
                    "timestamp": record["time"].isoformat(),
                    "level": record["level"].name,
                    "message": record["message"],
                    "file": record["file"].name,
                    "function": record["function"],
                    "line": record["line"],
                }
            )
            + "\n"
        ),
    )
```

---

*Last Updated: February 2026*
