#!/usr/bin/env python3
"""Copy flagship examples to docs for MkDocs rendering."""
import shutil
from pathlib import Path

EXAMPLES_ROOT = Path("examples")
DOCS_EXAMPLES = Path("docs/examples")

# Define which notebooks to feature in docs with their supporting files
FLAGSHIP_EXAMPLES = [
    {
        "notebook": "01_quickstart/01_hello_world.ipynb",
        "dest_name": "01_hello_world.ipynb",
        "supporting_files": [],  # Remote data example - no local files needed
    },
    {
        "notebook": "01_quickstart/02_database_governance.ipynb",
        "dest_name": "02_database_governance.ipynb",
        "supporting_files": [
            ("01_quickstart/users_contract.yaml", "users_contract.yaml"),
        ],
    },
    {
        "notebook": "_archive/03_data_sources/files/xml/xml_example.ipynb",
        "dest_name": "xml_example.ipynb",
        "supporting_files": [
            ("_archive/03_data_sources/files/xml/xml_contract.yaml", "xml_contract.yaml"),
            ("_archive/03_data_sources/files/xml/data/employees.xml", "data/employees.xml"),
        ],
    },
    {
        "notebook": "_archive/03_data_sources/files/excel/excel_example.ipynb",
        "dest_name": "excel_example.ipynb",
        "supporting_files": [
            ("_archive/03_data_sources/files/excel/excel_contract.yaml", "excel_contract.yaml"),
            ("_archive/03_data_sources/files/excel/data/employees.xlsx", "data/employees.xlsx"),
        ],
    },
    {
        "notebook": "04_compliance_governance/hipaa_pii_masking/tutorial_hipaa_compliance.ipynb",
        "dest_name": "hipaa_compliance.ipynb",
        "supporting_files": [],  # Uses inline data generation
    },
]

def main():
    # Create docs/examples if it doesn't exist
    DOCS_EXAMPLES.mkdir(parents=True, exist_ok=True)
    
    copied_count = 0
    total_files = 0
    
    for example in FLAGSHIP_EXAMPLES:
        # Copy the notebook
        source = EXAMPLES_ROOT / example["notebook"]
        dest = DOCS_EXAMPLES / example["dest_name"]
        
        if not source.exists():
            print(f"⚠️  Warning: Notebook not found: {source}")
            continue
            
        shutil.copy2(source, dest)
        print(f"✓ Copied notebook: {source.name}")
        copied_count += 1
        total_files += 1
        
        # Copy supporting files (YAML contracts, data files)
        for support_src, support_dest in example["supporting_files"]:
            src_path = EXAMPLES_ROOT / support_src
            dst_path = DOCS_EXAMPLES / support_dest
            
            if not src_path.exists():
                print(f"  ⚠️  Warning: Supporting file not found: {src_path}")
                continue
            
            # Create parent directories if needed
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            
            shutil.copy2(src_path, dst_path)
            print(f"  ✓ Copied supporting file: {support_dest}")
            total_files += 1
    
    print(f"\n✅ Synced {copied_count}/{len(FLAGSHIP_EXAMPLES)} notebooks ({total_files} total files) to docs/examples/")
    
    if copied_count < len(FLAGSHIP_EXAMPLES):
        print("⚠️  Some notebooks were not found. Check the paths above.")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
