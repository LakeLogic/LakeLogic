from __future__ import annotations

import csv
import re
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import yaml

DEFAULT_LIST_MERGE_KEYS: Tuple[str, ...] = (
    "transformations",
    "quality.row_rules",
    "quality.dataset_rules",
)
DEFAULT_SOFT_DELETE_OPERATION_VALUES: Tuple[str, ...] = (
    "i",
    "u",
    "d",
    "insert",
    "update",
    "delete",
    "upsert",
    "merge",
)


@dataclass(frozen=True)
class TemplateApplyResult:
    contract_path: Path
    output_path: Path
    written: bool


def _yaml_loader() -> yaml.SafeLoader:
    class Loader(yaml.SafeLoader):
        pass

    for key, mappings in list(Loader.yaml_implicit_resolvers.items()):
        Loader.yaml_implicit_resolvers[key] = [
            (tag, regex) for tag, regex in mappings if tag != "tag:yaml.org,2002:bool"
        ]

    bool_regex = re.compile(r"^(?:true|false)$", re.IGNORECASE)
    Loader.add_implicit_resolver("tag:yaml.org,2002:bool", bool_regex, list("tTfF"))
    return Loader


def load_yaml(path: Path) -> Dict[str, Any]:
    loader = _yaml_loader()
    return yaml.load(path.read_text(encoding="utf-8"), Loader=loader) or {}


class _QuotedString(str):
    pass


def _quote_boolish(value: Any) -> Any:
    if isinstance(value, dict):
        normalized: Dict[Any, Any] = {}
        for key, val in value.items():
            if isinstance(key, bool):
                key = "true" if key else "false"
            normalized[_quote_boolish(key)] = _quote_boolish(val)
        return normalized
    if isinstance(value, list):
        return [_quote_boolish(item) for item in value]
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return _QuotedString(value)
    return value


def dump_yaml(data: Dict[str, Any], path: Path) -> None:
    data = _quote_boolish(data)

    class _SafeDumper(yaml.SafeDumper):
        pass

    def _repr_quoted(dumper: yaml.Dumper, value: _QuotedString):
        return dumper.represent_scalar("tag:yaml.org,2002:str", value, style="'")

    _SafeDumper.add_representer(_QuotedString, _repr_quoted)
    text = yaml.dump(data, sort_keys=False, Dumper=_SafeDumper)
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
        merged = list(overlay_list) + list(base_list)
    else:
        merged = list(base_list) + list(overlay_list)
    return _dedupe_list(merged)


def _dedupe_list(items: List[Any]) -> List[Any]:
    if not items:
        return []
    seen: set[str] = set()
    deduped: List[Any] = []
    for item in items:
        key = _fingerprint_item(item)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _fingerprint_item(item: Any) -> str:
    try:
        return json.dumps(item, sort_keys=True, default=str)
    except Exception:
        try:
            return yaml.safe_dump(item, sort_keys=True)
        except Exception:
            return repr(item)


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


def _normalize_entity_name(value: str) -> str:
    text = (value or "").strip()
    for prefix in ("bronze_", "silver_", "gold_", "platinum_", "reference_"):
        if text.startswith(prefix):
            return text[len(prefix):]
    return text


def _infer_columns_from_source(contract: Dict[str, Any], contract_path: Path) -> List[str]:
    source = contract.get("source") or {}
    raw_path = source.get("path")
    if not raw_path or str(raw_path).startswith(("s3://", "gs://", "abfss://", "adl://", "https://")):
        return []
    base = contract_path.parent
    path = Path(raw_path)
    if not path.is_absolute():
        path = (base / path).resolve()
    if path.is_dir():
        pattern = source.get("pattern") or "*.csv"
        candidates = sorted(path.glob(pattern))
        if candidates:
            path = candidates[0]
    if not path.exists():
        return []
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            return [h.strip() for h in header or [] if h and h.strip()]
    if suffix == ".parquet":
        try:
            import pyarrow.parquet as pq  # type: ignore
        except Exception:
            return []
        try:
            return pq.read_schema(path).names
        except Exception:
            return []
    return []


def _collect_columns(contract: Dict[str, Any], contract_path: Path, infer_columns: bool) -> List[str]:
    model = contract.get("model") or {}
    fields = model.get("fields") or []
    cols = [f.get("name") for f in fields if isinstance(f, dict) and f.get("name")]
    if cols:
        return cols
    if infer_columns:
        return _infer_columns_from_source(contract, contract_path)
    return []


def _find_column(columns: List[str], target: str) -> Optional[str]:
    lookup = {c.lower(): c for c in columns}
    return lookup.get(target.lower())


def _soft_delete_already_applied(contract: Dict[str, Any]) -> bool:
    metadata = contract.get("metadata") or {}
    if metadata.get("soft_delete_applied"):
        return True
    for step in contract.get("transformations") or []:
        if not isinstance(step, dict):
            continue
        if "map_values" in step and isinstance(step["map_values"], dict):
            if step["map_values"].get("output") in {"__op_delete", "__deleted_at_flag", "__is_deleted_flag"}:
                return True
        if "derive" in step and isinstance(step["derive"], dict):
            if step["derive"].get("field") in {"__deleted_at_flag"}:
                return True
        if "coalesce" in step and isinstance(step["coalesce"], dict):
            if step["coalesce"].get("output") == "is_deleted":
                return True
    return False


def _apply_soft_delete(
    contract: Dict[str, Any],
    *,
    columns: List[str],
    operation_col: str,
    deleted_at_col: str,
    flag_col: str,
    output_col: str,
    exclude_hard_deletes: bool,
    operation_values: List[str],
) -> None:
    if _soft_delete_already_applied(contract):
        return

    op_actual = _find_column(columns, operation_col)
    deleted_at_actual = _find_column(columns, deleted_at_col)
    flag_actual = _find_column(columns, flag_col)

    if not any([op_actual, deleted_at_actual, flag_actual]):
        return

    transformations = contract.get("transformations")
    if not isinstance(transformations, list):
        transformations = []

    pre_steps: List[Dict[str, Any]] = []

    if op_actual:
        pre_steps.append({"lower": {"fields": [op_actual]}, "phase": "pre"})
        pre_steps.append({
            "map_values": {
                "field": op_actual,
                "mapping": {val: True for val in operation_values},
                "default": False,
                "output": "__op_delete",
            },
            "phase": "pre",
        })
        if exclude_hard_deletes:
            pre_steps.append({
                "filter": {
                    "sql": "__op_delete IS NOT TRUE"
                },
                "phase": "pre",
            })

    if deleted_at_actual:
        pre_steps.append({
            "sql": f"SELECT *, CASE WHEN {deleted_at_actual} IS NOT NULL THEN TRUE ELSE FALSE END AS __deleted_at_flag FROM source",
            "phase": "pre",
        })

    if flag_actual:
        mapping = {
            "true": True,
            "false": False,
            "t": True,
            "f": False,
            "yes": True,
            "no": False,
            "y": True,
            "n": False,
            "1": True,
            "0": False,
            "deleted": True,
            "delete": True,
        }
        pre_steps.append({
            "map_values": {
                "field": flag_actual,
                "mapping": mapping,
                "default": False,
                "output": "__is_deleted_flag",
            },
            "phase": "pre",
        })

    sources: List[str] = []
    if flag_actual:
        sources.append("__is_deleted_flag")
    if deleted_at_actual:
        sources.append("__deleted_at_flag")
    if op_actual:
        sources.append("__op_delete")

    if sources:
        pre_steps.append({
            "coalesce": {
                "field": output_col,
                "sources": sources,
                "default": False,
                "output": output_col,
            },
            "phase": "pre",
        })
        pre_steps.append({
            "drop": {
                "columns": [col for col in ["__op_delete", "__deleted_at_flag", "__is_deleted_flag"]]
            },
            "phase": "pre",
        })

    transformations = pre_steps + transformations
    contract["transformations"] = transformations

    quality = contract.get("quality")
    if not isinstance(quality, dict):
        quality = {}
    row_rules = quality.get("row_rules")
    if not isinstance(row_rules, list):
        row_rules = []

    if op_actual:
        row_rules.append({
            "accepted_values": {
                "field": op_actual,
                "values": operation_values,
            }
        })
    quality["row_rules"] = row_rules
    contract["quality"] = quality

    model = contract.get("model")
    if isinstance(model, dict):
        fields = model.get("fields")
        if isinstance(fields, list):
            existing = {f.get("name", "").lower() for f in fields if isinstance(f, dict)}
            if output_col.lower() not in existing:
                fields.append({
                    "name": output_col,
                    "type": "boolean",
                })
        model["fields"] = fields
        contract["model"] = model

    metadata = contract.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    metadata["soft_delete_applied"] = True
    contract["metadata"] = metadata


def _collect_registry_paths(registry_path: Path, stage: Optional[str]) -> List[Path]:
    data = load_yaml(registry_path)
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


def collect_contract_paths(
    *,
    registry: Optional[Path],
    contracts_dir: Optional[Path],
    contracts: Optional[Iterable[Path]],
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


def apply_contract_template(
    base_template: Union[Path, Dict[str, Any]],
    *,
    registry: Optional[Path] = None,
    stage: Optional[str] = None,
    contracts_dir: Optional[Path] = None,
    contracts: Optional[Iterable[Path]] = None,
    output_dir: Optional[Path] = None,
    entity_include: Optional[str] = None,
    entity_exclude: Optional[str] = None,
    list_merge_keys: Optional[Iterable[str]] = None,
    list_mode: str = "append",
    infer_columns: bool = False,
    soft_delete: bool = False,
    soft_delete_operation_column: str = "operation",
    soft_delete_deleted_at_column: str = "deleted_at",
    soft_delete_flag_column: str = "is_deleted",
    soft_delete_output_column: str = "is_deleted",
    soft_delete_keep_hard_deletes: bool = False,
    soft_delete_operation_values: Optional[Iterable[str]] = None,
    dry_run: bool = False,
) -> List[TemplateApplyResult]:
    if isinstance(base_template, (str, Path)):
        base_template_path = Path(base_template).resolve()
        template = load_yaml(base_template_path)
    else:
        template = dict(base_template)

    registry_path = Path(registry).resolve() if registry else None
    contracts_dir_path = Path(contracts_dir).resolve() if contracts_dir else None
    contract_paths = [Path(path).resolve() for path in (contracts or [])]
    output_dir_path = Path(output_dir).resolve() if output_dir else None

    list_keys: Iterable[str]
    if list_merge_keys is None:
        list_keys = DEFAULT_LIST_MERGE_KEYS
    elif isinstance(list_merge_keys, str):
        list_keys = [key.strip() for key in list_merge_keys.split(",") if key.strip()]
    else:
        list_keys = list_merge_keys
    list_merge_keys_set = {key.strip() for key in list_keys if key and str(key).strip()}

    include_re = re.compile(entity_include) if entity_include else None
    exclude_re = re.compile(entity_exclude) if entity_exclude else None

    if soft_delete_operation_values is None:
        op_values = [val for val in DEFAULT_SOFT_DELETE_OPERATION_VALUES]
    elif isinstance(soft_delete_operation_values, str):
        op_values = [v.strip() for v in soft_delete_operation_values.split(",") if v.strip()]
    else:
        op_values = [v for v in soft_delete_operation_values if str(v).strip()]

    targets = collect_contract_paths(
        registry=registry_path,
        contracts_dir=contracts_dir_path,
        contracts=contract_paths,
        stage=stage,
    )

    results: List[TemplateApplyResult] = []
    for contract_path in targets:
        contract_data = load_yaml(contract_path)
        dataset_name = contract_data.get("dataset") or contract_path.stem
        entity_name = _normalize_entity_name(str(dataset_name))
        if include_re and not (include_re.search(entity_name) or include_re.search(str(dataset_name))):
            continue
        if exclude_re and (exclude_re.search(entity_name) or exclude_re.search(str(dataset_name))):
            continue

        merged = _apply_template(
            contract_data,
            template,
            list_merge_keys=list_merge_keys_set,
            list_mode=list_mode,
        )

        if soft_delete:
            columns = _collect_columns(merged, contract_path, infer_columns)
            _apply_soft_delete(
                merged,
                columns=columns,
                operation_col=soft_delete_operation_column,
                deleted_at_col=soft_delete_deleted_at_column,
                flag_col=soft_delete_flag_column,
                output_col=soft_delete_output_column,
                exclude_hard_deletes=not soft_delete_keep_hard_deletes,
                operation_values=op_values,
            )

        if output_dir_path:
            if registry_path:
                try:
                    rel_path = contract_path.relative_to(registry_path.parent)
                except ValueError:
                    rel_path = contract_path.name
            elif contracts_dir_path:
                try:
                    rel_path = contract_path.relative_to(contracts_dir_path)
                except ValueError:
                    rel_path = contract_path.name
            else:
                rel_path = contract_path.name
            out_path = output_dir_path / rel_path
        else:
            out_path = contract_path

        if not dry_run:
            dump_yaml(merged, out_path)
        results.append(TemplateApplyResult(contract_path=contract_path, output_path=out_path, written=not dry_run))

    return results
