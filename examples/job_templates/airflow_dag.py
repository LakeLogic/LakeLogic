from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

REGISTRY = "examples/insurance_elt/contracts/insurance/_registry.yaml"
REF_REGISTRY = "examples/insurance_elt/contracts/shared/reference/_registry.yaml"
GOLD_REGISTRY = "examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml"

with DAG(
    dag_id="lakeguard_insurance_elt",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    run_reference = BashOperator(
        task_id="run_reference",
        bash_command=(
            "lakeguard-driver "
            f"--registry {REGISTRY} "
            f"--reference-registry {REF_REGISTRY} "
            f"--gold-registry {GOLD_REGISTRY} "
            "--layers reference "
            "--window last_success"
        ),
    )

    run_bronze = BashOperator(
        task_id="run_bronze",
        bash_command=(
            "lakeguard-driver "
            f"--registry {REGISTRY} "
            f"--reference-registry {REF_REGISTRY} "
            f"--gold-registry {GOLD_REGISTRY} "
            "--layers bronze "
            "--window last_success"
        ),
    )

    run_silver = BashOperator(
        task_id="run_silver",
        bash_command=(
            "lakeguard-driver "
            f"--registry {REGISTRY} "
            f"--reference-registry {REF_REGISTRY} "
            f"--gold-registry {GOLD_REGISTRY} "
            "--layers silver "
            "--window last_success"
        ),
    )

    run_gold = BashOperator(
        task_id="run_gold",
        bash_command=(
            "lakeguard-driver "
            f"--registry {REGISTRY} "
            f"--reference-registry {REF_REGISTRY} "
            f"--gold-registry {GOLD_REGISTRY} "
            "--layers gold "
            "--window last_success"
        ),
    )

    run_reference >> run_bronze >> run_silver >> run_gold
