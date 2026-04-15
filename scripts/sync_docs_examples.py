#!/usr/bin/env python3
"""Copy flagship examples to docs for MkDocs rendering."""

import shutil
from pathlib import Path

EXAMPLES_ROOT = Path("examples")
DOCS_EXAMPLES = Path("docs/examples")

# Define which notebooks to feature in docs with their supporting files
FLAGSHIP_EXAMPLES = [
    {
        "notebook": "colab/00_quickstart.ipynb",
        "dest_name": "00_quickstart.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "colab/01_data_quality_trust.ipynb",
        "dest_name": "01_data_quality_trust.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "colab/02_compliance_governance.ipynb",
        "dest_name": "02_compliance_governance.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "colab/03_engine_scale.ipynb",
        "dest_name": "03_engine_scale.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "colab/04_developer_experience.ipynb",
        "dest_name": "04_developer_experience.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "colab/05_data_generation_ai.ipynb",
        "dest_name": "05_data_generation_ai.ipynb",
        "supporting_files": [],
    },
    {
        "notebook": "colab/06_integrations.ipynb",
        "dest_name": "06_integrations.ipynb",
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

    print(
        f"\nDONE Synced {copied_count}/{len(FLAGSHIP_EXAMPLES)} notebooks ({total_files} total files) to docs/examples/"
    )

    if copied_count < len(FLAGSHIP_EXAMPLES):
        print("!! Some notebooks were not found. Check the paths above.")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
