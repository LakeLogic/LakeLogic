import json
import os
import re


def clean_notebook(path):
    print(f"Cleaning {path}...")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Clean the first markdown cell (usually the title)
    for cell in data.get("cells", []):
        if cell.get("cell_type") == "markdown":
            source = cell.get("source", [])
            if source and source[0].startswith("# "):
                # Remove emojis and leading/trailing icons
                title = source[0]
                # Regex to remove emojis: this is a simple one, might not catch all
                # But we can target specific ones if we know them
                clean_title = re.sub(r"[🌍🚀⚡🌟🛡️🗄️🎯🎉📄🛡️]", "", title).strip()
                # Clean multiple spaces
                clean_title = re.sub(r" +", " ", clean_title)
                # Ensure it still starts with #
                if not clean_title.startswith("# "):
                    clean_title = "# " + clean_title.lstrip("# ")

                # If we stripped everything but the #, restore a bit of sanity
                if clean_title == "# ":
                    continue

                # Ensure newline
                if not clean_title.endswith("\n"):
                    clean_title += "\n"

                source[0] = clean_title
                print(f"  New Title: {clean_title.strip()}")
                break  # Only clean the first H1 found

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1, ensure_ascii=False)


# List of notebooks to clean
notebooks = [
    r"examples\01_quickstart\01_hello_world.ipynb",
    r"examples\01_quickstart\02_database_governance.ipynb",
]

for nb in notebooks:
    full_path = os.path.join(os.getcwd(), nb)
    if os.path.exists(full_path):
        clean_notebook(full_path)
    else:
        print(f"Not found: {full_path}")
