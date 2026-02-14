import os
import shutil

# Files and directories to delete
to_delete = [
    r"d:\Github\_SaaS\lakelogic\examples\01_quickstart\hello_world_remote.ipynb",
    r"d:\Github\_SaaS\lakelogic\examples\01_quickstart\basic_validation",
    r"d:\Github\_SaaS\lakelogic\examples\01_quickstart\database_extraction"
]

for path in to_delete:
    print(f"Attempting to delete: {path}")
    if not os.path.exists(path):
        print(f"  Result: Path does not exist.")
        continue
        
    try:
        if os.path.isfile(path):
            os.remove(path)
            print(f"  Result: Successfully removed file.")
        else:
            shutil.rmtree(path)
            print(f"  Result: Successfully removed directory.")
    except Exception as e:
        print(f"  Result: Failed with error: {e}")

print("\nFinal check of directory contents:")
try:
    print(os.listdir(r"d:\Github\_SaaS\lakelogic\examples\01_quickstart"))
except Exception as e:
    print(f"  Error listing directory: {e}")
