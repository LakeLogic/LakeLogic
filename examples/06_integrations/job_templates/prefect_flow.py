from prefect import flow, task
import subprocess

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


@task
def run_reference():
    run_layer("reference")


@task
def run_bronze():
    run_layer("bronze")


@task
def run_silver():
    run_layer("silver")


@task
def run_gold():
    run_layer("gold")


@flow(name="lakelogic-insurance-elt")
def insurance_elt():
    ref = run_reference()
    bronze = run_bronze(wait_for=[ref])
    silver = run_silver(wait_for=[bronze])
    run_gold(wait_for=[silver])


if __name__ == "__main__":
    insurance_elt()
