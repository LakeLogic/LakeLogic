# Multi-Format File Ingestion

This directory contains examples for ingesting various file formats with LakeLogic.

## Supported Formats

| Format | Extensions | Engines Supported | Auto Dependencies |
|--------|-----------|-------------------|-------------------|
| **CSV** | `.csv` | All | ✅ Built-in |
| **Parquet** | `.parquet` | All | ✅ Built-in |
| **XML** | `.xml` | All | `lxml` |
| **Excel** | `.xlsx`, `.xls` | All | `openpyxl` |
| **Delta Lake** | `_delta_log` | All | `deltalake` |
| **Iceberg** | (table format) | Spark only | Spark built-in |

## Engine-Specific Behavior

### Polars, Pandas, DuckDB (Local Engines)
- Use native Python libraries for file reading
- XML requires `lxml`
- Excel requires `openpyxl`
- Automatically included when installing with engine extras

### Spark (Distributed Engine)
- Uses JVM-based readers
- XML: Auto-loads `com.databricks:spark-xml`
- Excel: Auto-loads `com.crealytics:spark-excel`
- Packages are downloaded automatically at session creation

## Quick Examples

### CSV Ingestion
```python
from lakelogic import DataProcessor

processor = DataProcessor(contract="csv/contract.yaml")

# Option 1: Tuple unpacking (backward compatible)
df_raw, df_good, df_bad = processor.run_source("data/file.csv")

# Option 2: Named attributes (recommended)
result = processor.run_source("data/file.csv")
validated = result.good
quarantined = result.bad
```

### XML Ingestion
```python
processor = DataProcessor(contract="xml/xml_contract.yaml")

# Access via attributes for clarity
result = processor.run_source("data/file.xml")
print(f"Processed {len(result.raw)} records")
print(f"Validated: {len(result.good)}, Quarantined: {len(result.bad)}")
```

### Excel Ingestion
```python
processor = DataProcessor(contract="excel/excel_contract.yaml")

# Named attributes make code self-documenting
result = processor.run_source("data/file.xlsx")
valid_employees = result.good
invalid_employees = result.bad
```

### Parquet Ingestion
```python
processor = DataProcessor(contract="parquet/contract.yaml")
result = processor.run_source("data/file.parquet")

# Print summary
print(result)  # ValidationResult(good=150, bad=5, raw=155)
```

## Multi-File Support

All formats support reading multiple files:

```yaml
# contract.yaml
source:
  type: landing
  path: data/*.xml  # Reads all XML files in directory
```

Or explicitly:

```python
processor.run_source([
    "data/employees_2023.xml",
    "data/employees_2024.xml"
])
```

## Installation

```bash
# For local engines with all format support
pip install lakelogic[all]

# For specific engines
pip install lakelogic[polars]  # Polars + XML + Excel support
pip install lakelogic[pandas]  # Pandas + XML + Excel support
pip install lakelogic[duckdb]  # DuckDB + XML + Excel support
pip install lakelogic[spark]   # Spark (XML/Excel auto-loaded)
```

## Next Steps

Explore each subdirectory for detailed examples:
- `csv/`: Basic CSV ingestion
- `parquet/`: Columnar format with schema preservation
- `xml/`: XML data validation
- `excel/`: Spreadsheet ingestion
- `delta/`: Advanced Delta Lake integration
