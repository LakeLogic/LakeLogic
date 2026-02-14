# Excel Data Ingestion Example

This example demonstrates how to ingest and validate Excel files (.xlsx, .xls) using LakeLogic.

## Overview

LakeLogic now supports native Excel ingestion across all engines:
- **Polars**: Uses `pl.read_excel()`
- **Pandas**: Uses `pd.read_excel()`
- **DuckDB**: Uses `pd.read_excel()` as a bridge
- **Spark**: Auto-loads `spark-excel` package when needed

## Files

- `data/employees.csv`: CSV version of sample data
- `data/employees.xlsx`: Sample Excel file with employee records (auto-generated)
- `data/create_excel.py`: Helper script to create the Excel file
- `excel_contract.yaml`: Data contract with validation rules
- `excel_example.ipynb`: Interactive notebook demonstrating Excel ingestion

### Creating the Excel File

If the `employees.xlsx` file doesn't exist, create it by running:

```bash
cd data
python create_excel.py
```

Or create it manually in Python:
```python
import pandas as pd
df = pd.read_csv("data/employees.csv")
df.to_excel("data/employees.xlsx", index=False, sheet_name="Employees")
```

## What This Example Shows

1. **Native Excel Reading**: Point LakeLogic directly at `.xlsx` or `.xls` files
2. **Schema Validation**: Ensure Excel data matches expected structure
3. **Quality Rules**: Validate data quality on Excel imports
4. **Automatic Type Inference**: Excel numeric and date types are preserved

## Usage

### Quick Start (Tuple Unpacking)

```python
from lakelogic import DataProcessor

# Load and validate Excel data
processor = DataProcessor(contract="excel_contract.yaml")
df_raw, df_good, df_bad = processor.run_source()

print(f"Valid records: {len(df_good)}")
print(f"Quarantined: {len(df_bad)}")
```

### Using Named Attributes (Recommended)

```python
# Access results via named attributes
result = processor.run_source()

# More readable and self-documenting
validated_employees = result.good
quarantined_employees = result.bad
original_data = result.raw

print(result)  # Shows: ValidationResult(good=3, bad=2, raw=5)
```

### Reading Multiple Sheets

To read specific sheets from an Excel workbook, you can pre-load with pandas:

```python
import pandas as pd
from lakelogic import DataProcessor

# Read specific sheet
df = pd.read_excel("data/multi_sheet.xlsx", sheet_name="Employees")

# Then validate with LakeLogic
processor = DataProcessor(contract="excel_contract.yaml")
df_good, df_bad = processor.run(df)
```

## Dependencies

Excel support requires `openpyxl`:

```bash
# Install with engine-specific extras
pip install lakelogic[polars]  # includes openpyxl
pip install lakelogic[pandas]  # includes openpyxl
pip install lakelogic[duckdb]  # includes openpyxl
pip install lakelogic[all]     # includes everything
```

For Spark, the `spark-excel` package is auto-loaded when needed.

## Common Use Cases

### 1. Finance Data Imports
Excel is commonly used for financial reports. LakeLogic ensures imported data meets quality standards before processing.

### 2. Manual Data Uploads
When users upload Excel files to your data platform, validate them immediately with LakeLogic.

### 3. Legacy System Integration
Many legacy systems export to Excel. LakeLogic provides a bridge to modern data pipelines.

## Next Steps

- Try reading `.xls` (old Excel format) files
- Experiment with multi-sheet workbooks
- Combine with materialization to save validated data as Parquet or Delta
