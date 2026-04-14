# XML Data Ingestion Example

This example demonstrates how to ingest and validate XML data using LakeLogic.

## Overview

LakeLogic now supports native XML ingestion across all engines:
- **Polars**: Uses `pl.read_xml()`
- **Pandas**: Uses `pd.read_xml()`
- **DuckDB**: Uses `pd.read_xml()` as a bridge
- **Spark**: Auto-loads `spark-xml` package when needed

## Files

- `data/employees.xml`: Simple flat XML structure (recommended for beginners)
- `data/employees_nested.xml`: Complex nested/hierarchical XML (enterprise scenario)
- `xml_contract.yaml`: Data contract for flat XML
- `xml_nested_contract.yaml`: Data contract for nested XML with flattening
- `xml_example.ipynb`: Interactive notebook demonstrating XML ingestion
- `README.md`: This file

## Nested XML Support

LakeLogic handles both **flat** and **nested/hierarchical** XML structures:

### Flat XML (Simple)
```xml
<employees>
  <employee>
    <id>1</id>
    <name>Alice</name>
    <email>alice@company.com</email>
  </employee>
</employees>
```

### Nested XML (Enterprise)
```xml
<company>
  <employees>
    <employee id="1">
      <personal_info>
        <name>
          <first>Alice</first>
          <last>Johnson</last>
        </name>
        <contact>
          <email>alice@company.com</email>
        </contact>
      </personal_info>
      <employment>
        <department>
          <name>Engineering</name>
        </department>
      </employment>
    </employee>
  </employees>
</company>
```

**How it works:**
1. XML readers (Polars/Pandas) automatically flatten nested structures
2. Nested fields become prefixed columns: `personal_info_name_first`, `employment_department_name`
3. Use `rename` transformations in your contract to clean up column names
4. Apply validation and business logic as normal

See `xml_nested_contract.yaml` for a complete example of handling hierarchical XML.

## What This Example Shows

1. **Native XML Reading**: Point LakeLogic directly at `.xml` files
2. **Schema Validation**: Ensure XML data matches expected structure
3. **Quality Rules**: Validate email formats, salary ranges, and status values
4. **Transformations**: Clean, derive, and enrich XML data
5. **Quarantine**: Separate good records from invalid ones

## Usage

### Quick Start (Tuple Unpacking)

```python
from lakelogic import DataProcessor

# Load and validate XML data
processor = DataProcessor(contract="xml_contract.yaml")
df_raw, df_good, df_bad = processor.run_source()

print(f"Total records: {len(df_raw)}")
print(f"Valid records: {len(df_good)}")
print(f"Quarantined: {len(df_bad)}")
```

### Using Named Attributes (Recommended)

```python
# Access results via named attributes
result = processor.run_source()

print(f"Raw: {len(result.raw)}")
print(f"Good: {len(result.good)}")
print(f"Bad: {len(result.bad)}")

# Process the results
validated_df = result.good
quarantined_df = result.bad
```

### With Specific Engine

```python
import os
os.environ["LAKELOGIC_ENGINE"] = "polars"  # or pandas, duckdb, spark

processor = DataProcessor(contract="xml_contract.yaml")
df_raw, df_good, df_bad = processor.run_source()
```

## Expected Results

The example XML file contains 5 employee records with intentional data quality issues:
- 1 record with invalid email format (missing @)
- 1 record with negative salary
- 1 record with inactive status

After processing:
- **Raw**: 5 records
- **Good**: 2-3 valid records (depending on active status rule)
- **Quarantined**: 2-3 invalid records with error reasons

## Dependencies

XML support requires `lxml`:

```bash
# Install with engine-specific extras
pip install lakelogic[polars]  # includes lxml
pip install lakelogic[pandas]  # includes lxml
pip install lakelogic[duckdb]  # includes lxml
pip install lakelogic[all]     # includes everything
```

For Spark, the `spark-xml` package is auto-loaded when needed.

## Next Steps

- Modify `xml_contract.yaml` to add custom validation rules
- Try different engines to see performance differences
- Combine with materialization to persist validated data
