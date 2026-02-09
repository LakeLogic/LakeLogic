"""
Oracle Database Example

Simple example of extracting data from Oracle Database.

Prerequisites:
    pip install "lakelogic[oracle]"
    
    Oracle Instant Client installed:
    - Download from: https://www.oracle.com/database/technologies/instant-client.html
    - Set environment variable: LD_LIBRARY_PATH (Linux) or PATH (Windows)

Setup:
    1. Oracle Database running
    2. User with read permissions
    3. Oracle Instant Client installed

Run:
    python oracle_example.py

What it does:
- Connects to Oracle Database
- Extracts data from a table
- Validates schema
- Writes to Delta Lake
"""

from lakelogic.core.data_processor import DataProcessor

def main():
    """
    Extract data from Oracle Database.
    """
    
    print("=" * 80)
    print("Oracle Database Data Extraction")
    print("=" * 80)
    print()
    
    # Create contract (inline for demo)
    contract = {
        "version": "1.0.0",
        "dataset": "employees",
        "description": "Employee data from Oracle Database",
        
        # Source: Oracle Database
        "source": {
            "type": "database",
            "connector": "oracle",
            "connection": {
                "host": "localhost",
                "port": 1521,
                "service_name": "ORCL",  # Or use SID
                "username": "hr",
                "password": "${SECRET:oracle_password}",  # From Key Vault
            },
            "query": "SELECT * FROM EMPLOYEES"
        },
        
        # Schema
        "model": {
            "fields": [
                {"name": "EMPLOYEE_ID", "type": "integer"},
                {"name": "FIRST_NAME", "type": "string"},
                {"name": "LAST_NAME", "type": "string"},
                {"name": "EMAIL", "type": "string"},
                {"name": "PHONE_NUMBER", "type": "string"},
                {"name": "HIRE_DATE", "type": "date"},
                {"name": "JOB_ID", "type": "string"},
                {"name": "SALARY", "type": "decimal"},
                {"name": "COMMISSION_PCT", "type": "decimal"},
                {"name": "MANAGER_ID", "type": "integer"},
                {"name": "DEPARTMENT_ID", "type": "integer"}
            ]
        },
        
        # Quality rules
        "quality": {
            "row_rules": [
                {
                    "name": "employee_id_not_null",
                    "rule": "not_null",
                    "column": "EMPLOYEE_ID",
                    "severity": "error"
                },
                {
                    "name": "email_not_null",
                    "rule": "not_null",
                    "column": "EMAIL",
                    "severity": "error"
                }
            ]
        },
        
        # Output
        "materialization": {
            "bronze": {
                "enabled": True,
                "path": "./data/bronze/employees/",
                "format": "delta",
                "mode": "overwrite"
            }
        }
    }
    
    # Process data
    print("📊 Extracting data from Oracle Database...")
    
    processor = DataProcessor(contract)
    result = processor.run()
    
    print()
    print("✅ Extraction complete!")
    print(f"   Records processed: {result.get('records_processed', 0)}")
    print(f"   Output: {contract['materialization']['bronze']['path']}")


if __name__ == "__main__":
    main()
