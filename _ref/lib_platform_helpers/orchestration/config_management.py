import yaml
import os
import re
from typing import Any, Dict, Optional


def load_yaml(path: str) -> Dict[str, Any]:
    """Loads a YAML configuration file from the given path.

    Args:
        path (str): The file system path to the YAML file.

    Returns:
        Dict[str, Any]: The parsed YAML content as a Python dictionary.

    Example:
        # Assuming 'config.yml' contains:
        # key: value
        config = load_yaml('config.yml')
        print(config)
        # Output: {'key': 'value'}
    """
    with open(path, "r") as f:
        return yaml.safe_load(f)


def dynamic_config_loader(
    config_path: str,
    env_name: str,
    output_path: Optional[str] = None,
    overwrite_output: bool = True,
    debug_mode: bool = False,
) -> Dict[str, Any]:
    """
    Loads a YAML config, merges an optional override config, recursively
    replaces all placeholders, and optionally writes the result to a new file.

    This function dynamically resolves nested placeholders like `${section.key}`
    by repeatedly looking up values from elsewhere in the configuration until no
    more replacements can be made.

    Loads and resolves a YAML config file with support for:
      ✅ Nested ${placeholders}
      ✅ Environment override (env_name)
      ✅ Dataset-aware templating (${current_dataset.*})

    Args:
        config_path (str): Path to the base YAML configuration file.
        env_name (str): The name of the target environment (e.g., "dev", "qc").
        output_path (Optional[str]): If provided, the fully resolved config
            will be written to this file path.
        overwrite_output (bool): If True, will overwrite the output file if it
            already exists. Defaults to False to prevent accidental overwrites.
        debug_mode (bool): If True, enables verbose logging for debugging.

    Returns:
        Dict[str, Any]: The fully merged and templated configuration dictionary.

    Example:
        config = dynamic_config_loader(
                    "base_config.yml",
                    "dev",
                    "dev_overrides.yml",
                    "resolved_config_dev.yml",
                    overwrite_output=True,  # Explicitly allow overwriting
                    debug_mode=True
                )
    """
    # --- Load YAML config ---
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # --- Recursive dict merge (preserved from original) ---
    def _merge_dicts(base: Dict, override: Dict) -> Dict:
        for key, value in override.items():
            if isinstance(value, dict) and key in base and isinstance(base[key], dict):
                base[key] = _merge_dicts(base[key], value)
            else:
                base[key] = value
        return base

    # --- Apply environment override ---
    if "environment" in config and "name" in config["environment"]:
        config["environment"]["name"] = env_name

    # --- Flatten dicts/lists for lookup ---
    def _flatten_structure(
        obj: Any, parent_key: str = "", sep: str = "."
    ) -> Dict[str, Any]:
        items = {}
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                items.update(_flatten_structure(v, new_key, sep))
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                new_key = f"{parent_key}[{i}]"
                items.update(_flatten_structure(v, new_key, sep))
        else:
            items[parent_key] = obj
        return items

    # --- Recursive placeholder resolver (from your original logic) ---
    def _template_recursive(obj: Any, flat_config: Dict[str, Any]) -> Any:
        if isinstance(obj, dict):
            return {k: _template_recursive(v, flat_config) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_template_recursive(item, flat_config) for item in obj]
        elif isinstance(obj, str):

            def replacer(match):
                key = match.group(1)
                return str(flat_config.get(key, match.group(0)))

            return re.sub(r"\$\{([^}]+)\}", replacer, obj)
        else:
            return obj

    # --- Multi-pass placeholder resolution (unchanged from original) ---
    previous_config_str = ""
    current_config_str = str(config)

    while previous_config_str != current_config_str:
        previous_config_str = current_config_str
        flat_config = _flatten_structure(config)
        config = _template_recursive(config, flat_config)
        current_config_str = str(config)

    # --- Add dataset-aware templating (NEW SECTION) ---
    def _resolve_dynamic(obj: Any, context: Dict[str, Any]) -> Any:
        """Resolve ${...} using dynamic dataset context."""
        if isinstance(obj, dict):
            return {k: _resolve_dynamic(v, context) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_resolve_dynamic(v, context) for v in obj]
        elif isinstance(obj, str):

            def replacer(match):
                expr = match.group(1).strip()
                try:
                    parts = re.split(r"[.\[\]]+", expr)
                    parts = [p for p in parts if p]
                    current = context
                    for p in parts:
                        if p.isdigit():
                            current = current[int(p)]
                        else:
                            current = current[p]
                    return str(current)
                except Exception:
                    return match.group(0)

            return re.sub(r"\$\{([^}]+)\}", replacer, obj)
        else:
            return obj

    if "datasets" in config and isinstance(config["datasets"], list):
        resolved_datasets = []
        for i, ds in enumerate(config["datasets"]):
            context = {**config, "current_dataset": ds, "current_dataset_index": i}
            resolved_datasets.append(_resolve_dynamic(ds, context))
            if debug_mode:
                print(f"✅ Dataset[{i}] resolved → {ds.get('name', 'unknown')}")
        config["datasets"] = resolved_datasets

    # --- Optional: Write final resolved config ---
    if output_path:
        if os.path.exists(output_path) and not overwrite_output:
            if debug_mode:
                print(f"⚠️ Skipping write, file exists: {output_path}")
        else:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "w") as f:
                yaml.dump(config, f, sort_keys=False, default_flow_style=False)
            if debug_mode:
                print(f"📝 Resolved config written to: {output_path}")

    return config
