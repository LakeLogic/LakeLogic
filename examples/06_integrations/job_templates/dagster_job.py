import subprocess
from dagster import job, op

REGISTRY = "examples/insurance_elt/contracts/insurance/_registry.yaml"
REF_REGISTRY = "examples/insurance_elt/contracts/shared/reference/_registry.yaml"
GOLD_REGISTRY = "examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml"


def run_layer(layer: str) -> None:
    cmd = [
        "lakelogic-driver",
        "--registry", REGISTRY,
        "--reference-registry", REF_REGISTRY,
        "--gold-registry", GOLD_REGISTRY,
        "--layers", layer,
        "--window", "last_success",
        "--summary-table", "lakelogic.pipeline_runs",
        "--summary-backend", "duckdb",
        "--summary-database", "examples/insurance_elt/output/run_logs/lakelogic_pipeline_runs.duckdb",
        "--metrics-path", "examples/insurance_elt/output/run_logs/pipeline_metrics.json",
    ]
    subprocess.run(cmd, check=True)


@op
def run_reference():
    run_layer("reference")


@op
def run_bronze():
    run_layer("bronze")


@op
def run_silver():
    run_layer("silver")


@op
def run_gold():
    run_layer("gold")


@job
def lakelogic_insurance_elt():
    ref = run_reference()
    bronze = run_bronze().add_dependency(ref)
    silver = run_silver().add_dependency(bronze)
    run_gold().add_dependency(silver)
