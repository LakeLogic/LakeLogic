import argparse
from pathlib import Path

from lakelogic.tools.template_apply import (
    DEFAULT_LIST_MERGE_KEYS,
    apply_contract_template,
    collect_contract_paths,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply a base template to many LakeLogic contracts.")
    parser.add_argument("--base-template", required=True, help="Path to base template YAML.")
    parser.add_argument("--registry", help="Registry YAML to resolve contract paths.")
    parser.add_argument("--stage", help="Stage for registry contracts (bronze/silver/gold).")
    parser.add_argument("--contracts-dir", help="Directory containing contract YAMLs.")
    parser.add_argument("--contracts", nargs="*", help="Explicit contract YAML paths.")
    parser.add_argument("--output-dir", help="Write updated contracts here instead of in-place.")
    parser.add_argument("--entity-include", help="Regex to include entities by name.")
    parser.add_argument("--entity-exclude", help="Regex to exclude entities by name.")
    parser.add_argument(
        "--list-merge-keys",
        default="transformations,quality.row_rules,quality.dataset_rules",
        help="Comma-separated list paths to merge instead of replace.",
    )
    parser.add_argument(
        "--list-mode",
        default="append",
        choices=["append", "prepend", "replace"],
        help="How to merge list keys: append=template+contract, prepend=contract+template, replace=contract only.",
    )
    parser.add_argument("--infer-columns", action="store_true", help="Infer columns from local source path if model missing.")
    parser.add_argument("--soft-delete", action="store_true", help="Inject soft-delete handling for operation/deleted_at/is_deleted columns.")
    parser.add_argument("--soft-delete-operation-column", default="operation", help="Operation column name.")
    parser.add_argument("--soft-delete-deleted-at-column", default="deleted_at", help="Deleted-at column name.")
    parser.add_argument("--soft-delete-flag-column", default="is_deleted", help="Existing delete flag column name.")
    parser.add_argument("--soft-delete-output-column", default="is_deleted", help="Output delete flag column name.")
    parser.add_argument("--soft-delete-keep-hard-deletes", action="store_true", help="Keep hard deletes instead of filtering them out.")
    parser.add_argument(
        "--soft-delete-operation-values",
        default="i,u,d,insert,update,delete,upsert,merge",
        help="Comma-separated operation values considered valid (lowercase).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print planned updates without writing files.")
    args = parser.parse_args()

    registry_path = Path(args.registry).resolve() if args.registry else None
    contracts_dir = Path(args.contracts_dir).resolve() if args.contracts_dir else None
    contracts = [Path(p).resolve() for p in (args.contracts or [])]
    output_dir = Path(args.output_dir).resolve() if args.output_dir else None

    list_merge_keys = [key.strip() for key in str(args.list_merge_keys).split(",") if key.strip()]
    op_values = [v.strip() for v in str(args.soft_delete_operation_values).split(",") if v.strip()]

    targets = collect_contract_paths(
        registry=registry_path,
        contracts_dir=contracts_dir,
        contracts=contracts,
        stage=args.stage,
    )

    if not targets:
        print("No contracts found. Provide --registry, --contracts-dir, or --contracts.")
        return

    results = apply_contract_template(
        base_template=Path(args.base_template),
        registry=registry_path,
        stage=args.stage,
        contracts_dir=contracts_dir,
        contracts=contracts,
        output_dir=output_dir,
        entity_include=args.entity_include,
        entity_exclude=args.entity_exclude,
        list_merge_keys=list_merge_keys or DEFAULT_LIST_MERGE_KEYS,
        list_mode=args.list_mode,
        infer_columns=args.infer_columns,
        soft_delete=args.soft_delete,
        soft_delete_operation_column=args.soft_delete_operation_column,
        soft_delete_deleted_at_column=args.soft_delete_deleted_at_column,
        soft_delete_flag_column=args.soft_delete_flag_column,
        soft_delete_output_column=args.soft_delete_output_column,
        soft_delete_keep_hard_deletes=args.soft_delete_keep_hard_deletes,
        soft_delete_operation_values=op_values,
        dry_run=args.dry_run,
    )

    for result in results:
        if args.dry_run:
            print(f"[DRY RUN] {result.contract_path} -> {result.output_path}")
        else:
            print(f"Updated {result.output_path}")


if __name__ == "__main__":
    main()
