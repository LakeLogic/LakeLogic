import sys
import subprocess

REGISTRY = "s3://your-bucket/examples/insurance_elt/contracts/insurance/_registry.yaml"
REF_REGISTRY = "s3://your-bucket/examples/insurance_elt/contracts/shared/reference/_registry.yaml"
GOLD_REGISTRY = "s3://your-bucket/examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml"

cmd = [
    "lakeguard-driver",
    "--registry", REGISTRY,
    "--reference-registry", REF_REGISTRY,
    "--gold-registry", GOLD_REGISTRY,
    "--layers", "reference,bronze,silver,gold",
    "--window", "last_success",
]

print(" ".join(cmd))
result = subprocess.run(cmd, check=False)
if result.returncode != 0:
    sys.exit(result.returncode)
