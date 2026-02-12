import argparse
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml


def _yaml_loader():
    class Loader(yaml.SafeLoader):
        pass

    for key, mappings in list(Loader.yaml_implicit_resolvers.items()):
        Loader.yaml_implicit_resolvers[key] = [
            (tag, regex) for tag, regex in mappings if tag != "tag:yaml.org,2002:bool"
        ]

    bool_regex = re.compile(r"^(?:true|false)$", re.IGNORECASE)
    Loader.add_implicit_resolver("tag:yaml.org,2002:bool", bool_regex, list("tTfF"))
    return Loader


def _load_yaml(path: Path) -> Dict[str, Any]:
    loader = _yaml_loader()
    return yaml.load(path.read_text(encoding="utf-8"), Loader=loader) or {}


def _dump_yaml(data: Dict[str, Any], path: Path) -> None:
    text = yaml.safe_dump(data, sort_keys=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _merge_list(base_list: List[Any], overlay_list: List[Any], mode: str) -> List[Any]:
    if not base_list:
        return list(overlay_list)
    if not overlay_list:
        return list(base_list)
    merge_mode = (mode or "append").lower()
    if merge_mode == "replace":
        return list(overlay_list)
    if merge_mode == "prepend":
        return list(overlay_list) + list(base_list)
    return list(base_list) + list(overlay_list)


def _deep_merge(
    base: Any,
    overlay: Any,
    *,
    path: str,
    list_merge_keys: set[str],
    list_mode: str,
) -> Any:
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged: Dict[str, Any] = dict(base)
        for key, value in overlay.items():
            next_path = f"{path}.{key}" if path else key
            if key in merged:
                merged[key] = _deep_merge(
                    merged[key],
                    value,
                    path=next_path,
                    list_merge_keys=list_merge_keys,
                    list_mode=list_mode,
                )
            else:
                merged[key] = value
        return merged

    if isinstance(base, list) and isinstance(overlay, list):
        if path in list_merge_keys:
            return _merge_list(base, overlay, list_mode)
        return list(overlay)

    return overlay if overlay is not None else base


def _collect_registry_paths(registry_path: Path, stage: Optional[str]) -> List[Path]:
    data = _load_yaml(registry_path)
    entries = data.get("entries", [])
    paths: List[Path] = []
    for entry in entries:
        if entry.get("enabled") is False:
            continue

        contract_paths: List[str] = []
        contracts_block = entry.get("contracts")
        if stage and isinstance(contracts_block, dict):
            if contracts_block.get(stage):
                contract_paths.append(contracts_block.get(stage))
        elif stage is None and isinstance(contracts_block, dict):
            contract_paths.extend([val for val in contracts_block.values() if val])

        if entry.get("contract_path"):
            contract_paths.append(entry.get("contract_path"))

        for contract_path in contract_paths:
            resolved = (registry_path.parent / contract_path).resolve()
            paths.append(resolved)
    return paths


def _iter_contracts(
    *,
    registry: Optional[Path],
    contracts_dir: Optional[Path],
    contracts: Optional[List[Path]],
    stage: Optional[str],
) -> List[Path]:
    collected: List[Path] = []
    if registry:
        collected.extend(_collect_registry_paths(registry, stage))
    if contracts_dir:
        collected.extend(sorted(contracts_dir.rglob("*.yml")))
        collected.extend(sorted(contracts_dir.rglob("*.yaml")))
    if contracts:
        collected.extend([path.resolve() for path in contracts])
    # Deduplicate
    seen = set()
    unique = []
    for path in collected:
        if path in seen:
            continue
        seen.add(path)
        unique.append(path)
    return unique


def _apply_template(
    contract_data: Dict[str, Any],
    template: Dict[str, Any],
    *,
    list_merge_keys: set[str],
    list_mode: str,
) -> Dict[str, Any]:
    return _deep_merge(
        template,
        contract_data,
        path="",
        list_merge_keys=list_merge_keys,
        list_mode=list_mode,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply a base template to many LakeLogic contracts.")
    parser.add_argument("--base-template", required=True, help="Path to base template YAML.")
    parser.add_argument("--registry", help="Registry YAML to resolve contract paths.")
    parser.add_argument("--stage", help="Stage for registry contracts (bronze/silver/gold).")
    parser.add_argument("--contracts-dir", help="Directory containing contract YAMLs.")
    parser.add_argument("--contracts", nargs="*", help="Explicit contract YAML paths.")
    parser.add_argument("--output-dir", help="Write updated contracts here instead of in-place.")
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
    parser.add_argument("--dry-run", action="store_true", help="Print planned updates without writing files.")
    args = parser.parse_args()

    base_template_path = Path(args.base_template).resolve()
    base_template = _load_yaml(base_template_path)

    registry_path = Path(args.registry).resolve() if args.registry else None
    contracts_dir = Path(args.contracts_dir).resolve() if args.contracts_dir else None
    contracts = [Path(p).resolve() for p in (args.contracts or [])]
    output_dir = Path(args.output_dir).resolve() if args.output_dir else None

    list_merge_keys = {key.strip() for key in str(args.list_merge_keys).split(",") if key.strip()}

    targets = _iter_contracts(
        registry=registry_path,
        contracts_dir=contracts_dir,
        contracts=contracts,
        stage=args.stage,
    )

    if not targets:
        print("No contracts found. Provide --registry, --contracts-dir, or --contracts.")
        return

    for contract_path in targets:
        contract_data = _load_yaml(contract_path)
        merged = _apply_template(
            contract_data,
            base_template,
            list_merge_keys=list_merge_keys,
            list_mode=args.list_mode,
        )

        if output_dir:
            if registry_path:
                try:
                    rel_path = contract_path.relative_to(registry_path.parent)
                except ValueError:
                    rel_path = contract_path.name
            elif contracts_dir:
                try:
                    rel_path = contract_path.relative_to(contracts_dir)
                except ValueError:
                    rel_path = contract_path.name
            else:
                rel_path = contract_path.name
            out_path = output_dir / rel_path
        else:
            out_path = contract_path

        if args.dry_run:
            print(f"[DRY RUN] {contract_path} -> {out_path}")
            continue

        _dump_yaml(merged, out_path)
        print(f"Updated {out_path}")


if __name__ == "__main__":
    main()
