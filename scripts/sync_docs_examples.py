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
        "notebook": "04_compliance_governance/hipaa_pii_masking/tutorial_hipaa_gdpr_compliance.ipynb",
        "dest_name": "hipaa_gdpr_compliance.ipynb",
        "supporting_files": [],  # Uses inline data generation
    },
    {
        "notebook": "03_advanced_workflows/ai_contract_enrichment/ai_enrich_demo.ipynb",
        "dest_name": "ai_enrich_demo.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "03_advanced_workflows/ai_contract_enrichment/ai_edge_cases_demo.ipynb",
        "dest_name": "ai_edge_cases_demo.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "03_advanced_workflows/synthetic_data_generation/synthetic_data_generation.ipynb",
        "dest_name": "synthetic_data_generation.ipynb",
        "supporting_files": [
            ("03_advanced_workflows/synthetic_data_generation/contract_orders.yaml", "contract_orders.yaml"),
        ],
    },
    {
        "notebook": "02_core_patterns/medallion_architecture/quickstart_tutorial.ipynb",
        "dest_name": "medallion_architecture.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "03_advanced_workflows/late_arriving_reprocess/demo_self_healing.ipynb",
        "dest_name": "late_arriving_reprocess.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "03_advanced_workflows/payments_lifecycle/payments_lifecycle.ipynb",
        "dest_name": "payments_lifecycle.ipynb",
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
