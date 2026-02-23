"""
lakelogic.adapters.dbt
----------------------
Reads dbt ``schema.yml`` / ``sources.yml`` files and converts them into a
``DataContract`` so every LakeLogic tool works without any schema migration.

Supported dbt test → LakeLogic rule mappings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
=========================  ===============================================
dbt test                   LakeLogic equivalent
=========================  ===============================================
not_null                   ``field.required = True``
unique                     ``quality.dataset_rules`` uniqueness check
accepted_values            ``quality.row_rules`` accepted_values
relationships              ``quality.row_rules`` SQL IS NOT NULL (soft)
expression_is_true         ``quality.row_rules`` raw SQL pass-through
dbt_utils.expression_is_true  same
=========================  ===============================================

PII convention
~~~~~~~~~~~~~~
Any column carrying ``meta: {pii: true}`` or tagged with ``pii`` is
promoted to ``field.pii = True`` in the resulting contract.

Usage
-----
Python API::

    from lakelogic.adapters.dbt import load_contract_from_dbt
    contract = load_contract_from_dbt("models/schema.yml", model="customers")

    from lakelogic import DataProcessor
    proc = DataProcessor.from_dbt("models/schema.yml", model="customers")

CLI::

    lakelogic import-dbt --schema models/schema.yml --model customers \\
                         --output contracts/customers.yaml

    # Import all models in a schema file
    lakelogic import-dbt --schema models/schema.yml --output contracts/

    # Import a dbt source
    lakelogic import-dbt --schema models/sources.yml --source-name raw \\
                         --source-table orders --output contracts/
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

from lakelogic.core.models import (
    DataContract,
    FieldDefinition,
    Info,
    Model,
    Quality,
    QualityRule,
)


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def load_contract_from_dbt(
    schema_path: Union[str, Path],
    *,
    model: Optional[str] = None,
    source_name: Optional[str] = None,
    source_table: Optional[str] = None,
) -> DataContract:
    """
    Load a ``DataContract`` from a dbt ``schema.yml`` or ``sources.yml``.

    Parameters
    ----------
    schema_path
        Path to the dbt YAML file (``schema.yml``, ``sources.yml``, or any
        dbt-format YAML containing ``models:`` or ``sources:`` keys).
    model
        Name of the dbt model to import.  Required when the file contains
        multiple models and you want a single contract.  If the file contains
        exactly one model, this may be omitted.
    source_name
        dbt source name (the top-level ``sources[].name`` value).
    source_table
        dbt source table name (``sources[].tables[].name``).

    Returns
    -------
    DataContract
    """
    adapter = DbtAdapter(schema_path)
    if source_name or source_table:
        return adapter.source_to_contract(source_name, source_table)
    return adapter.model_to_contract(model)


# ---------------------------------------------------------------------------
# Main adapter class
# ---------------------------------------------------------------------------

class DbtAdapter:
    """
    Reads a dbt schema file and emits ``DataContract`` objects.

    Parameters
    ----------
    schema_path
        Path to the dbt YAML file.
    """

    def __init__(self, schema_path: Union[str, Path]) -> None:
        self.schema_path = Path(schema_path)
        if not self.schema_path.exists():
            raise FileNotFoundError(f"dbt schema file not found: {self.schema_path}")
        with open(self.schema_path, encoding="utf-8") as fh:
            self._raw: Dict[str, Any] = yaml.safe_load(fh) or {}

    # ------------------------------------------------------------------
    # Model import
    # ------------------------------------------------------------------

    def list_models(self) -> List[str]:
        """Return the names of all models defined in the schema file."""
        return [m.get("name", "") for m in self._raw.get("models", [])]

    def model_to_contract(self, model_name: Optional[str] = None) -> DataContract:
        """
        Convert a dbt model definition to a ``DataContract``.

        Parameters
        ----------
        model_name
            Name of the model.  If ``None`` and the file contains exactly
            one model, that model is used automatically.
        """
        models = self._raw.get("models", [])
        if not models:
            raise ValueError(
                f"No 'models:' block found in {self.schema_path}. "
                "Use source_to_contract() for sources.yml files."
            )

        if model_name is None:
            if len(models) == 1:
                node = models[0]
            else:
                names = [m.get("name") for m in models]
                raise ValueError(
                    f"Multiple models found in {self.schema_path}: {names}. "
                    "Specify model=<name> to select one."
                )
        else:
            node = next((m for m in models if m.get("name") == model_name), None)
            if node is None:
                available = [m.get("name") for m in models]
                raise ValueError(
                    f"Model '{model_name}' not found in {self.schema_path}. "
                    f"Available: {available}"
                )

        return self._node_to_contract(node, layer="silver")

    # ------------------------------------------------------------------
    # Source import
    # ------------------------------------------------------------------

    def list_sources(self) -> List[Dict[str, Any]]:
        """Return all (source_name, table_name) pairs defined in the file."""
        out = []
        for src in self._raw.get("sources", []):
            sname = src.get("name", "")
            for tbl in src.get("tables", []):
                out.append({"source": sname, "table": tbl.get("name", "")})
        return out

    def source_to_contract(
        self,
        source_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> DataContract:
        """
        Convert a dbt source table definition to a ``DataContract``.
        """
        sources = self._raw.get("sources", [])
        if not sources:
            raise ValueError(f"No 'sources:' block found in {self.schema_path}.")

        src_node = None
        if source_name:
            src_node = next((s for s in sources if s.get("name") == source_name), None)
            if src_node is None:
                raise ValueError(f"Source '{source_name}' not found in {self.schema_path}.")
        elif len(sources) == 1:
            src_node = sources[0]
        else:
            names = [s.get("name") for s in sources]
            raise ValueError(f"Multiple sources found: {names}. Specify source_name=.")

        tables = src_node.get("tables", [])
        tbl_node = None
        if table_name:
            tbl_node = next((t for t in tables if t.get("name") == table_name), None)
            if tbl_node is None:
                available = [t.get("name") for t in tables]
                raise ValueError(
                    f"Table '{table_name}' not in source '{src_node.get('name')}'. "
                    f"Available: {available}"
                )
        elif len(tables) == 1:
            tbl_node = tables[0]
        else:
            names = [t.get("name") for t in tables]
            raise ValueError(f"Multiple tables found: {names}. Specify source_table=.")

        # Merge source-level columns into table node (dbt allows columns on source too)
        merged = dict(tbl_node)
        merged.setdefault("description", src_node.get("description", ""))
        return self._node_to_contract(merged, layer="bronze")

    # ------------------------------------------------------------------
    # Internal conversion
    # ------------------------------------------------------------------

    def _node_to_contract(self, node: Dict[str, Any], layer: str) -> DataContract:
        """Convert a raw dbt model/table node dict into a DataContract."""
        name        = node.get("name", "unnamed")
        description = node.get("description", "")
        columns     = node.get("columns", [])
        node_tests  = node.get("tests", [])   # model-level tests (rare)

        fields: List[FieldDefinition] = []
        row_rules: List[QualityRule]  = []
        dataset_rules: List[QualityRule] = []
        primary_key: List[str] = []

        tbl = name  # table alias used in SELECT … FROM <table>

        for col in columns:
            col_name  = col.get("name", "col")
            col_desc  = col.get("description", "")
            col_tests = col.get("tests", [])
            col_meta  = col.get("meta", {}) or {}
            col_tags  = col.get("tags", []) or []
            col_type  = col.get("data_type") or col_meta.get("type") or "string"

            required = False
            pii = _is_pii(col_name, col_meta, col_tags)

            for test in col_tests:
                parsed_rules, is_required, is_unique = _parse_column_test(
                    col_name, test, tbl
                )
                if is_required:
                    required = True
                if is_unique:
                    # Dataset rule SQL must be a full SELECT that returns a
                    # scalar value.  The engine evaluates: result must_be_less_than 1
                    # (i.e. 0 duplicate pairs → passes).
                    dataset_rules.append(
                        QualityRule(
                            name=f"{col_name}_unique",
                            sql=(
                                f"SELECT COUNT(*) - COUNT(DISTINCT {col_name}) "
                                f"FROM {tbl}"
                            ),
                            must_be_less_than=1,
                            description=f"{col_name} must be unique",
                        )
                    )
                    primary_key.append(col_name)
                row_rules.extend(parsed_rules)

            fields.append(
                FieldDefinition(
                    name=col_name,
                    type=_normalise_type(col_type),
                    description=col_desc or None,
                    required=required,
                    pii=pii,
                )
            )

        # Model-level tests (e.g. unique_combination_of_columns)
        for test in node_tests:
            parsed, _, _ = _parse_column_test("__model__", test, name)
            dataset_rules.extend(parsed)

        quality = Quality(row_rules=row_rules, dataset_rules=dataset_rules) if (row_rules or dataset_rules) else None

        return DataContract(
            version="1.0.0",
            dataset=name,
            primary_key=primary_key,
            info=Info(
                title=name,
                version="1.0.0",
                description=description or f"Imported from dbt schema: {self.schema_path.name}",
                target_layer=layer,
            ),
            model=Model(fields=fields) if fields else None,
            quality=quality,
        )

    # ------------------------------------------------------------------
    # Export to LakeLogic YAML
    # ------------------------------------------------------------------

    def to_yaml(
        self,
        model_name: Optional[str] = None,
        output: Optional[Union[str, Path]] = None,
    ) -> str:
        """
        Convert a dbt model to a LakeLogic contract and return as YAML string.
        Optionally write to *output* path.
        """
        contract = self.model_to_contract(model_name)
        data = contract.model_dump(exclude_none=True, by_alias=True)
        text = yaml.dump(data, sort_keys=False, allow_unicode=True, default_flow_style=False)
        if output:
            out = Path(output)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(text, encoding="utf-8")
        return text

    def export_all(self, output_dir: Union[str, Path]) -> List[Path]:
        """
        Export every model in the schema file to individual contract YAMLs
        in *output_dir*.

        Returns
        -------
        List[Path]
            Paths of written files.
        """
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        written: List[Path] = []
        for model_name in self.list_models():
            out_path = out_dir / f"{model_name}.yaml"
            self.to_yaml(model_name, output=out_path)
            written.append(out_path)
        return written


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _is_pii(col_name: str, meta: Dict[str, Any], tags: List[str]) -> bool:
    """Detect PII from dbt meta block or tags."""
    if meta.get("pii") or meta.get("is_pii"):
        return True
    if "pii" in [str(t).lower() for t in tags]:
        return True
    # Common PII field name patterns
    _PII_PATTERNS = re.compile(
        r"\b(email|phone|mobile|address|postcode|zip|ssn|passport|dob|birth|"
        r"national_id|tax_id|name|surname|forename|ip_address|device_id|"
        r"credit_card|bank_account|iban|bic|sort_code)\b",
        re.IGNORECASE,
    )
    return bool(_PII_PATTERNS.search(col_name))


def _normalise_type(dbt_type: str) -> str:
    """Map dbt / SQL type names to LakeLogic field types."""
    t = dbt_type.lower().strip()
    _MAP = {
        "varchar": "string", "nvarchar": "string", "char": "string",
        "text": "string", "string": "string",
        "int": "integer", "integer": "integer", "int4": "integer",
        "int8": "integer", "bigint": "integer", "smallint": "integer",
        "tinyint": "integer", "number": "double",
        "float": "double", "float4": "double", "float8": "double",
        "double": "double", "numeric": "double", "decimal": "double",
        "real": "double",
        "bool": "boolean", "boolean": "boolean",
        "date": "date",
        "timestamp": "timestamp", "datetime": "timestamp",
        "timestamp_ntz": "timestamp", "timestamp_tz": "timestamp",
        "timestamptz": "timestamp",
        "json": "string", "jsonb": "string", "variant": "string",
        "array": "string", "object": "string",
    }
    # Strip precision/scale e.g. numeric(18,2) → numeric
    base = re.split(r"[\(\s]", t)[0]
    return _MAP.get(base, "string")


def _parse_column_test(
    col_name: str,
    test: Any,
    table_name: str = "source",
) -> tuple[List[QualityRule], bool, bool]:
    """
    Parse a single dbt column test into LakeLogic QualityRule(s).

    Row-level rules use bare SQL expressions (wrapped by the engine in
    ``SELECT … WHERE NOT (<expr>)`` style logic).

    Dataset-level rules must be full ``SELECT <scalar> FROM <table>``
    statements that return a single numeric value which the engine then
    compares against ``must_be_less_than`` / ``must_be_between`` etc.

    Returns
    -------
    (rules, is_required, is_unique)
    """
    rules: List[QualityRule] = []
    is_required = False
    is_unique   = False

    # Simple string test: "not_null", "unique"
    if isinstance(test, str):
        if test == "not_null":
            is_required = True
        elif test == "unique":
            is_unique = True
        return rules, is_required, is_unique

    if not isinstance(test, dict):
        return rules, is_required, is_unique

    # Dict test: {"not_null": {...}} or {"accepted_values": {"values": [...]}}
    test_name = next(iter(test))
    cfg = test.get(test_name) or {}

    if test_name == "not_null":
        is_required = True

    elif test_name == "unique":
        is_unique = True

    elif test_name == "accepted_values":
        values = cfg.get("values", []) if isinstance(cfg, dict) else []
        if values and col_name != "__model__":
            # Row rules use bare expressions — the engine wraps these in its
            # own SELECT … WHERE NOT (<expr>) evaluation.
            quoted = ", ".join(
                f"'{v}'" if not str(v).lstrip("-").isdigit() else str(v)
                for v in values
            )
            rules.append(
                QualityRule(
                    name=f"{col_name}_accepted_values",
                    sql=f"{col_name} IN ({quoted})",
                    description=f"{col_name} must be one of: {values}",
                )
            )

    elif test_name in ("relationships", "dbt_utils.relationships"):
        # Soft referential integrity — row rule (bare expression)
        if col_name != "__model__":
            rules.append(
                QualityRule(
                    name=f"{col_name}_fk_not_null",
                    sql=f"{col_name} IS NOT NULL",
                    description=f"{col_name} must have a valid FK reference",
                )
            )

    elif test_name in ("expression_is_true", "dbt_utils.expression_is_true"):
        # Row rule — bare SQL expression evaluated per-row
        expression = cfg.get("expression") if isinstance(cfg, dict) else str(cfg)
        if expression:
            safe_name = re.sub(r"\W+", "_", str(expression))[:40]
            rules.append(
                QualityRule(
                    name=f"expr_{col_name}_{safe_name}".strip("_"),
                    sql=expression,
                    description=f"Expression check on {col_name}: {expression}",
                )
            )

    elif test_name in ("dbt_utils.not_null_proportion",):
        # No direct SQL equivalent — skip gracefully
        pass

    return rules, is_required, is_unique
