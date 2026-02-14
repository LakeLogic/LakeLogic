import shutil
import os

paths = [
    'd:/Github/_SaaS/lakelogic/examples/01_quickstart/hello_world_remote.ipynb',
    'd:/Github/_SaaS/lakelogic/examples/01_quickstart/basic_validation',
    'd:/Github/_SaaS/lakelogic/examples/01_quickstart/database_extraction'
]

for path in paths:
    try:
        if os.path.isfile(path):
            os.remove(path)
            print(f"Removed file: {path}")
        elif os.path.isdir(path):
            shutil.rmtree(path)
            print(f"Removed directory: {path}")
        else:
            print(f"Path not found: {path}")
    except Exception as e:
        print(f"Error removing {path}: {e}")
