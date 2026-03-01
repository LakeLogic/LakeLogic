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
        "supporting_files": [],
    },
    {
        "notebook": "01_quickstart/02_database_governance.ipynb",
        "dest_name": "02_database_governance.ipynb",
        "supporting_files": [
            ("01_quickstart/users_contract.yaml", "users_contract.yaml"),
        ],
    },
    {
        # Renamed: 04_compliance_governance/hipaa_pii_masking
        #       -> 03_compliance_governance/hipaa_gdpr_pii_masking
        "notebook": "03_compliance_governance/hipaa_gdpr_pii_masking/tutorial_hipaa_gdpr_compliance.ipynb",
        "dest_name": "hipaa_gdpr_compliance.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "02_core_patterns/bronze_quality_gate/playbook.ipynb",
        "dest_name": "bronze_quality_gate.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "02_core_patterns/medallion_architecture/quickstart_tutorial.ipynb",
        "dest_name": "medallion_architecture.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "02_core_patterns/dedup_survivorship/playbook.ipynb",
        "dest_name": "dedup_survivorship.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "02_core_patterns/scd2_dimension/playbook.ipynb",
        "dest_name": "scd2_dimension.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "02_core_patterns/soft_delete/playbook.ipynb",
        "dest_name": "soft_delete.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "02_core_patterns/reference_joins/playbook.ipynb",
        "dest_name": "reference_joins.ipynb",
        "supporting_files": [],
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
            print(f"!! Warning: Notebook not found at {source.absolute()}")
            continue
            
        shutil.copy2(source, dest)
        print(f"OK Copied notebook: {source.name}")
        copied_count += 1
        total_files += 1
        
        # Copy supporting files (YAML contracts, data files)
        for support_src, support_dest in example["supporting_files"]:
            src_path = EXAMPLES_ROOT / support_src
            dst_path = DOCS_EXAMPLES / support_dest
            
            if not src_path.exists():
                print(f"  !! Warning: Supporting file not found: {src_path}")
                continue
            
            # Create parent directories if needed
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            
            shutil.copy2(src_path, dst_path)
            print(f"  OK Copied supporting file: {support_dest}")
            total_files += 1
    
    print(f"\nDONE Synced {copied_count}/{len(FLAGSHIP_EXAMPLES)} notebooks ({total_files} total files) to docs/examples/")
    
    if copied_count < len(FLAGSHIP_EXAMPLES):
        print("!! Some notebooks were not found. Check the paths above.")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
