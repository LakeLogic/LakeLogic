"""
DuckDB Engine Adapter for LakeLogic.

Provides a first-class DuckDB execution path — all SQL transforms,
schema enforcement, and quality rules run directly in DuckDB's
in-process engine without Polars intermediation.

DuckDB is already used as a fallback inside PolarsAdapter; this adapter
makes it a standalone engine suitable for lightweight pipelines, Colab
demos, and SQL-heavy contracts.
"""

import time
from pathlib import Path
from typing import Any, List, Tuple

from loguru import logger

from lakelogic.engines.base import EngineAdapter


class DuckDBAdapter(EngineAdapter):
    """
    DuckDB execution engine for LakeLogic.

    Supports row-level validation, aggregate metrics, and SQL-first
    transformations using DuckDB's in-process SQL engine.
    """

    engine_name: str = "duckdb"

    def __init__(self, contract, connection=None, trace=None):
        """
        Initialize the DuckDB adapter.

        Args:
            contract: DataContract instance.
            connection: Optional existing DuckDB connection. If None,
                       creates an in-memory connection.
            trace: Optional list of trace steps to pre-populate.
        """
        super().__init__(contract, trace)
        self.engine_name = "duckdb"

        import duckdb

        self._owns_connection = connection is None
        self.con = connection or duckdb.connect(database=":memory:")

    # ── Helpers ───────────────────────────────────────────────────────────

    def _register_df(self, name: str, df: Any) -> None:
        """Register a dataframe-like object as a DuckDB view."""
        try:
            import polars as pl

            if isinstance(df, pl.LazyFrame):
                df = df.collect()
            if isinstance(df, pl.DataFrame):
                # Use zero-copy Arrow registration to bypass pandas serialization
                self.con.register(name, df.to_arrow())
                return
        except ImportError:
            pass

        if hasattr(df, "toPandas"):
            # Spark DataFrame
            self.con.register(name, df.toPandas())
        else:
            # Assume pandas or compatible
            self.con.register(name, df)

    def _to_output_df(self, rel) -> Any:
        """
        Convert a DuckDB relation to a Polars DataFrame for compatibility
        with the rest of the LakeLogic pipeline (materialization, lineage, etc.).
        """
        try:
            # Use DuckDB's native zero-copy Polars/Arrow integration
            return rel.pl()
        except ImportError:
            return rel.df()

    def _get_columns(self, df: Any) -> List[str]:
        """Get column names from a dataframe."""
        if hasattr(df, "columns"):
            cols = df.columns
            return list(cols) if not isinstance(cols, list) else cols
        return []

    # ── Link registration ─────────────────────────────────────────────────

    def _register_links(self) -> None:
        """Register linked reference datasets as DuckDB views for JOINs."""
        for link in self.contract.links:
            try:
                # Skip table-type links (Spark-only)
                table_path = link.path[6:] if link.path and link.path.startswith("table:") else None
                if link.table or (link.type and link.type.lower() == "table") or table_path:
                    table_name = link.table or table_path or link.path
                    logger.warning(
                        f"Link '{link.name}' references table '{table_name}'. Table links are supported in Spark only."
                    )
                    continue

                if not link.path:
                    continue

                # Load remote paths using deltalake and register via PyArrow
                if link.path.startswith(("s3://", "gs://", "abfss://", "adl://", "https://")):
                    try:
                        from deltalake import DeltaTable as _DT
                        from lakelogic.core.processor import DataProcessor as _DP

                        _dummy_proc = _DP.__new__(_DP)
                        _sopts = _dummy_proc._get_cloud_storage_options(link.path)
                        _dt = _DT(link.path, storage_options=_sopts)

                        pa_table = _dt.to_pyarrow_table()
                        if link.columns:
                            available = set(pa_table.column_names)
                            keep = [c for c in link.columns if c in available]
                            if keep:
                                pa_table = pa_table.select(keep)
                        self.con.register(link.name, pa_table)
                        logger.info(f"Registered remote link '{link.name}' from {link.path}")
                    except Exception as e:
                        logger.warning(f"Could not load remote link '{link.name}' from {link.path}: {e}")
                    continue

                path = Path(link.path)
                if not path.is_absolute() and hasattr(self.contract, "_base_path"):
                    path = Path(self.contract._base_path) / path
                if not path.exists():
                    logger.warning(f"Link file/directory not found: {path}")
                    continue

                col_clause = ", ".join(f'"{c}"' for c in link.columns) if link.columns else "*"
                link_type = (link.type or "parquet").lower()

                if link_type == "delta":
                    # Use DuckDB's delta extension or fall back to deltalake + pandas
                    try:
                        self.con.execute("INSTALL delta; LOAD delta;")
                        self.con.execute(
                            f"CREATE OR REPLACE VIEW {link.name} AS "
                            f"SELECT {col_clause} FROM delta_scan('{path.as_posix()}')"
                        )
                    except Exception:
                        # Fall back: read via deltalake Python library
                        try:
                            from deltalake import DeltaTable

                            dt = DeltaTable(str(path))
                            pdf = dt.to_pandas()
                            if link.columns:
                                available = set(pdf.columns)
                                keep = [c for c in link.columns if c in available]
                                if keep:
                                    pdf = pdf[keep]
                            self.con.register(link.name, pdf)
                        except Exception as e_inner:
                            logger.warning(f"Could not load Delta link '{link.name}': {e_inner}")
                            continue
                elif link_type == "parquet":
                    if path.is_dir():
                        glob_path = path.as_posix() + "/*.parquet"
                        self.con.execute(
                            f"CREATE OR REPLACE VIEW {link.name} AS "
                            f"SELECT {col_clause} FROM read_parquet('{glob_path}')"
                        )
                    else:
                        self.con.execute(
                            f"CREATE OR REPLACE VIEW {link.name} AS "
                            f"SELECT {col_clause} FROM read_parquet('{path.as_posix()}')"
                        )
                elif link_type == "csv":
                    self.con.execute(
                        f"CREATE OR REPLACE VIEW {link.name} AS "
                        f"SELECT {col_clause} FROM read_csv_auto('{path.as_posix()}')"
                    )
                else:
                    logger.warning(f"Unsupported link type '{link_type}' for link '{link.name}'")
                    continue

                logger.info(f"Registered link '{link.name}' from {path} (type={link_type})")
            except Exception as e:
                logger.warning(f"Could not register link '{link.name}': {e}")

    # ── Schema enforcement ────────────────────────────────────────────────

    def _apply_schema(self, table_name: str = "source") -> Tuple[str, List[str]]:
        """
        Apply schema casts, missing columns, and unknown field handling.

        Operates via SQL ALTER/ADD COLUMN statements on the registered view.

        Returns:
            Tuple of (output_table_name, schema_errors).
        """
        if not self.contract.model or not self.contract.model.fields:
            return table_name, []

        existing_cols = set(
            row[0] for row in self.con.sql(f"SELECT column_name FROM (DESCRIBE {table_name})").fetchall()
        )
        expected_fields = [f.name for f in self.contract.model.fields]
        expected = set(expected_fields)

        missing = expected - existing_cols
        unknown = existing_cols - expected

        # Exclude transient, framework, and lineage columns from unknown
        transient_cols = {"rn", "__index_level_0__", "_row_number"}
        system_cols = {c for c in unknown if c.startswith("_lakelogic_")}
        unknown = unknown - transient_cols - system_cols - self._lineage_columns()

        # Add missing columns as NULL
        add_cols = []
        for col in missing:
            add_cols.append(f'NULL AS "{col}"')

        if add_cols:
            existing_select = ", ".join(f'"{c}"' for c in existing_cols)
            null_cols = ", ".join(add_cols)
            self.con.sql(
                f"CREATE OR REPLACE VIEW _schema_applied AS SELECT {existing_select}, {null_cols} FROM {table_name}"
            )
            table_name = "_schema_applied"

        # Type casting
        server = self.contract.server
        from lakelogic.core.models import SchemaPolicy as _SP

        _sp_defaults = _SP()
        evolution = _sp_defaults.evolution
        policy = _sp_defaults.unknown_fields
        cast_to_string = False

        if server:
            cast_to_string = bool(server.cast_to_string)
            if server.schema_policy:
                evolution = (server.schema_policy.evolution or _sp_defaults.evolution).lower()
                policy = (server.schema_policy.unknown_fields or _sp_defaults.unknown_fields).lower()

        if cast_to_string:
            cols = [row[0] for row in self.con.sql(f"SELECT column_name FROM (DESCRIBE {table_name})").fetchall()]
            cast_exprs = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in cols)
            self.con.sql(f"CREATE OR REPLACE VIEW _typed AS SELECT {cast_exprs} FROM {table_name}")
            table_name = "_typed"
        else:
            # Cast fields to contract types
            _TYPE_MAP = {
                "string": "VARCHAR",
                "varchar": "VARCHAR",
                "text": "VARCHAR",
                "int": "BIGINT",
                "integer": "BIGINT",
                "long": "BIGINT",
                "bigint": "BIGINT",
                "float": "DOUBLE",
                "double": "DOUBLE",
                "decimal": "DOUBLE",
                "bool": "BOOLEAN",
                "boolean": "BOOLEAN",
                "date": "DATE",
                "timestamp": "TIMESTAMP",
                "datetime": "TIMESTAMP",
            }
            cols = [row[0] for row in self.con.sql(f"SELECT column_name FROM (DESCRIBE {table_name})").fetchall()]
            casts = []
            self._type_err_cols = []
            for col in cols:
                matched_field = next((f for f in self.contract.model.fields if f.name == col), None)
                if matched_field:
                    field_type = (matched_field.type or "").lower().split("(")[0].strip()
                    duckdb_type = _TYPE_MAP.get(field_type)
                    if duckdb_type:
                        err_col = f"__type_err_{col}"
                        self._type_err_cols.append(err_col)
                        err_msg = f"Type Mismatch: {col} cannot be cast to {field_type}".replace("'", "''")
                        casts.append(f'TRY_CAST("{col}" AS {duckdb_type}) AS "{col}"')
                        casts.append(
                            f'CASE WHEN "{col}" IS NOT NULL AND TRY_CAST("{col}" AS {duckdb_type}) IS NULL '
                            f"THEN '{err_msg}' ELSE NULL END AS \"{err_col}\""
                        )
                    else:
                        casts.append(f'"{col}"')
                else:
                    casts.append(f'"{col}"')
            self.con.sql(f"CREATE OR REPLACE VIEW _typed AS SELECT {', '.join(casts)} FROM {table_name}")
            table_name = "_typed"

        # ── Detect post-phase SQL transforms that reshape columns ────────────
        # When a contract has a post-phase SQL transform (e.g. gold aggregation
        # with GROUP BY), the model fields describe the *output* of the SQL, not
        # the source.  Strict missing/unknown enforcement at this stage would
        # produce false positives because the source columns haven't been
        # transformed yet (post-transforms run AFTER schema enforcement).
        _has_post_sql = False
        if self.contract.transformations:
            for _t in self.contract.transformations:
                _phase = (getattr(_t, "phase", None) or "post").lower()
                if _phase == "post" and getattr(_t, "sql", None):
                    _has_post_sql = True
                    break

        schema_errors: List[str] = []
        if evolution == "strict" and missing and not _has_post_sql:
            schema_errors.append(f"Missing fields: {', '.join(sorted(missing))}")

        if policy == "drop" and unknown:
            if not _has_post_sql:
                keep_cols = [c for c in self._get_current_columns(table_name) if c not in unknown]
                select = ", ".join(f'"{c}"' for c in keep_cols)
                self.con.sql(f"CREATE OR REPLACE VIEW _pruned AS SELECT {select} FROM {table_name}")
                table_name = "_pruned"
        elif policy == "quarantine" and unknown and not _has_post_sql:
            schema_errors.append(f"Unknown fields present: {', '.join(sorted(unknown))}")

        self.schema_drift = {
            "missing_fields": sorted(missing),
            "unknown_fields": sorted(unknown),
            "policy": policy,
            "evolution": evolution or "",
        }

        return table_name, schema_errors

    def _get_current_columns(self, table_name: str) -> List[str]:
        """Get column names from a DuckDB table/view."""
        return [row[0] for row in self.con.sql(f"SELECT column_name FROM (DESCRIBE {table_name})").fetchall()]

    # ── SQL transformations ───────────────────────────────────────────────

    def _apply_sql_transformation(self, table_name: str, sql: str) -> str:
        """
        Execute a SQL transformation, creating a new view.

        Args:
            table_name: Current source table/view name.
            sql: SQL query to execute.

        Returns:
            Name of the new view containing transformed data.
        """
        import re

        # Normalize Spark SQL quirks
        sql = self._normalize_spark_sql(sql)

        # Replace 'source' references with actual table name
        if table_name != "source":
            sql = re.sub(r"\bsource\b", table_name, sql)

        view_name = f"_transform_{id(sql) & 0xFFFFFF:06x}"
        try:
            self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")
            return view_name
        except Exception as e:
            logger.warning(f"DuckDB SQL transform failed: {e}")
            raise

    @staticmethod
    def _normalize_spark_sql(sql: str) -> str:
        """
        Normalize Spark SQL dialect quirks into DuckDB-compatible SQL.
        """
        import re
        import datetime as _dt

        now = _dt.datetime.now(_dt.timezone.utc)
        today_str = now.strftime("%Y-%m-%d")
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        sql = re.sub(r"\bNOW\s*\(\s*\)", f"TIMESTAMP '{now_str}'", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bCURRENT_TIMESTAMP\b", f"TIMESTAMP '{now_str}'", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bCURRENT_DATE\b", f"DATE '{today_str}'", sql, flags=re.IGNORECASE)

        # Spark type aliases
        _TYPE_MAP = {
            "STRING": "VARCHAR",
            "TEXT": "VARCHAR",
            "LONG": "BIGINT",
            "SHORT": "SMALLINT",
            "BYTE": "TINYINT",
        }
        for spark_type, ansi_type in _TYPE_MAP.items():
            sql = re.sub(rf"\bAS\s+{spark_type}\b", f"AS {ansi_type}", sql, flags=re.IGNORECASE)

        # timestamp_micros(x) -> make_timestamp(CAST(x AS BIGINT))
        sql = re.sub(
            r"timestamp_micros\s*\((.*?)\)",
            r"make_timestamp(CAST(\1 AS BIGINT))",
            sql,
            flags=re.IGNORECASE,
        )

        # try_to_date(x, 'yyyyMMdd') -> strptime(x, '%Y%m%d')::DATE
        def _date_repl(m):
            arg1, fmt = m.group(1), m.group(2)
            fmt = fmt.replace("yyyy", "%Y").replace("MM", "%m").replace("dd", "%d")
            return f"strptime({arg1}, '{fmt}')::DATE"

        sql = re.sub(
            r"try_to_date\s*\(\s*(.*?)\s*,\s*'(.*?)'\s*\)",
            _date_repl,
            sql,
            flags=re.IGNORECASE,
        )

        return sql

    # ── Pre/Post transformations ──────────────────────────────────────────

    def _apply_pre_transformations(self, table_name: str) -> str:
        """Apply filters, renames, and derived columns before schema enforcement."""
        current = table_name
        for trans in self.contract.transformations:
            trans_phase = (trans.phase or "post").lower()

            if trans.sql and trans_phase == "pre":
                logger.debug(f"Pre-Transform [SQL]: {trans.sql}")
                try:
                    current = self._apply_sql_transformation(current, trans.sql)
                except Exception as e:
                    logger.warning(f"Pre-Transform [SQL] failed: {e}")
                continue

            if trans.derive and trans_phase == "pre":
                logger.debug(f"Pre-Transform [Derive]: {trans.derive.field}")
                derive_sql = self._transpile_derive_sql(trans.derive)
                derive_sql = self._normalize_spark_sql(derive_sql)
                field_name = trans.derive.field
                cols = self._get_current_columns(current)
                view_name = f"_pre_derive_{field_name}"

                if field_name in cols:
                    expr = f'SELECT * EXCLUDE ("{field_name}"), ({derive_sql}) AS "{field_name}" FROM {current}'
                else:
                    expr = f'SELECT *, ({derive_sql}) AS "{field_name}" FROM {current}'

                try:
                    self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS {expr}")
                    current = view_name
                except Exception as e:
                    logger.warning(f"Pre-Transform [Derive] '{field_name}' failed: {e}")
                continue

            if trans.filter and trans_phase == "pre":
                logger.debug(f"Pre-Transform [Filter]: {trans.filter}")
                filter_sql = self._normalize_spark_sql(
                    trans.filter.sql if hasattr(trans.filter, "sql") else str(trans.filter)
                )
                view_name = f"_pre_filter_{id(filter_sql) & 0xFFFFFF:06x}"
                try:
                    self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {current} WHERE {filter_sql}")
                    current = view_name
                except Exception as e:
                    logger.warning(f"Pre-Transform [Filter] failed: {e}")
                continue

            if trans.rename and trans_phase == "pre":
                mappings = trans.rename.mappings if hasattr(trans.rename, "mappings") else {}
                if mappings:
                    logger.debug(f"Pre-Transform [Rename]: {mappings}")
                    cols = self._get_current_columns(current)
                    renames = []
                    for col in cols:
                        if col in mappings:
                            renames.append(f'"{col}" AS "{mappings[col]}"')
                        else:
                            renames.append(f'"{col}"')
                    view_name = f"_pre_rename_{id(mappings) & 0xFFFFFF:06x}"
                    self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(renames)} FROM {current}")
                    current = view_name
                continue

        return current

    def _apply_post_transformations(self, table_name: str) -> str:
        """Apply post-schema transformations (derives, SQL, rollups)."""
        current = table_name
        for trans in self.contract.transformations:
            trans_phase = (trans.phase or "post").lower()
            if trans_phase != "post":
                continue

            if trans.sql:
                logger.debug(f"Post-Transform [SQL]: {trans.sql}")
                try:
                    current = self._apply_sql_transformation(current, trans.sql)
                except Exception as e:
                    logger.warning(f"Post-Transform [SQL] failed: {e}")
                continue

            if trans.derive:
                field_name = trans.derive.field
                logger.debug(f"Post-Transform [Derive]: {field_name}")
                derive_sql = self._transpile_derive_sql(trans.derive)
                derive_sql = self._normalize_spark_sql(derive_sql)
                cols = self._get_current_columns(current)
                view_name = f"_post_derive_{field_name}"

                if field_name in cols:
                    expr = f'SELECT * EXCLUDE ("{field_name}"), ({derive_sql}) AS "{field_name}" FROM {current}'
                else:
                    expr = f'SELECT *, ({derive_sql}) AS "{field_name}" FROM {current}'

                try:
                    self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS {expr}")
                    current = view_name
                except Exception as e:
                    logger.warning(f"Post-Transform [Derive] '{field_name}' failed: {e}")
                continue

            if trans.filter:
                filter_sql = self._normalize_spark_sql(trans.filter)
                view_name = f"_post_filter_{id(filter_sql) & 0xFFFFFF:06x}"
                try:
                    self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {current} WHERE {filter_sql}")
                    current = view_name
                except Exception as e:
                    logger.warning(f"Post-Transform [Filter] failed: {e}")
                continue

            # Rollup
            if trans.rollup:
                rollup_sql = self._build_rollup_sql(trans.rollup, source_table=current)
                if rollup_sql:
                    view_name = f"_post_rollup_{id(rollup_sql) & 0xFFFFFF:06x}"
                    try:
                        self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS {rollup_sql}")
                        current = view_name
                    except Exception as e:
                        logger.warning(f"Post-Transform [Rollup] failed: {e}")
                continue

            # Pivot
            if trans.pivot:
                pivot_sql = self._build_pivot_sql(trans.pivot, source_table=current)
                if pivot_sql:
                    view_name = f"_post_pivot_{id(pivot_sql) & 0xFFFFFF:06x}"
                    try:
                        self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS {pivot_sql}")
                        current = view_name
                    except Exception as e:
                        logger.warning(f"Post-Transform [Pivot] failed: {e}")
                continue

            # Unpivot
            if trans.unpivot:
                unpivot_sql = self._build_unpivot_sql(trans.unpivot, source_table=current)
                if unpivot_sql:
                    view_name = f"_post_unpivot_{id(unpivot_sql) & 0xFFFFFF:06x}"
                    try:
                        self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS {unpivot_sql}")
                        current = view_name
                    except Exception as e:
                        logger.warning(f"Post-Transform [Unpivot] failed: {e}")
                continue

        return current

    # ── Main execution ────────────────────────────────────────────────────

    def execute(self, df: Any) -> Tuple[Any, Any]:
        """
        Execute the contract on a dataframe using DuckDB.

        Args:
            df: Input dataframe (Polars, Pandas, or compatible).

        Returns:
            Tuple of (good_df, bad_df) as Polars DataFrames.
        """
        start_time = time.perf_counter()
        self.dataset_rule_results = []
        self.schema_drift = {}
        self.trace = []

        # 0. Register input data
        self._register_df("source", df)
        if self.contract.dataset:
            self._register_df(self.contract.dataset, df)

        # Register linked reference datasets for JOINs in SQL transforms
        if self.contract.links:
            self._register_links()

        raw_count = self.con.sql("SELECT COUNT(*) FROM source").fetchone()[0]
        self._add_trace(
            "Load Source",
            input_rows=None,
            output_rows=raw_count,
            duration_ms=(time.perf_counter() - start_time) * 1000,
        )

        # 1. Pre-transformations
        current_table = "source"
        if self.contract.transformations:
            step_start = time.perf_counter()
            current_table = self._apply_pre_transformations(current_table)
            pre_count = self.con.sql(f"SELECT COUNT(*) FROM {current_table}").fetchone()[0]
            self._add_trace(
                "Pre-Transformations",
                input_rows=raw_count,
                output_rows=pre_count,
                duration_ms=(time.perf_counter() - step_start) * 1000,
            )

        # 2. Schema enforcement
        step_start = time.perf_counter()
        schema_input = self.con.sql(f"SELECT COUNT(*) FROM {current_table}").fetchone()[0]
        current_table, schema_errors = self._apply_schema(current_table)
        schema_output = self.con.sql(f"SELECT COUNT(*) FROM {current_table}").fetchone()[0]
        self._add_trace(
            "Schema Enforcement",
            input_rows=schema_input,
            output_rows=schema_output,
            duration_ms=(time.perf_counter() - step_start) * 1000,
            details={"errors": schema_errors},
        )

        # 3. Post-transformations
        if self.contract.transformations:
            step_start = time.perf_counter()
            post_input = self.con.sql(f"SELECT COUNT(*) FROM {current_table}").fetchone()[0]
            current_table = self._apply_post_transformations(current_table)
            post_output = self.con.sql(f"SELECT COUNT(*) FROM {current_table}").fetchone()[0]
            self._add_trace(
                "Post-Transformations",
                input_rows=post_input,
                output_rows=post_output,
                duration_ms=(time.perf_counter() - step_start) * 1000,
            )
            # Prune type-error columns that no longer exist after
            # transformations (e.g. GROUP BY replaces entire column set)
            if getattr(self, "_type_err_cols", None):
                surviving = set(self._get_current_columns(current_table))
                self._type_err_cols = [c for c in self._type_err_cols if c in surviving]

        # 4. Row-level quality rules
        row_rules = self.get_row_rules()

        if row_rules or schema_errors:
            step_start = time.perf_counter()

            # Build rule evaluation expressions
            rule_cols = []
            for i, rule in enumerate(row_rules):
                rule_cols.append(f"CAST(({rule.sql}) AS BOOLEAN) AS _rule_{i}")

            if rule_cols:
                eval_sql = f"SELECT *, {', '.join(rule_cols)} FROM {current_table}"
                self.con.sql(f"CREATE OR REPLACE VIEW _evaluated AS {eval_sql}")
            else:
                self.con.sql(f"CREATE OR REPLACE VIEW _evaluated AS SELECT * FROM {current_table}")

            # Build error tracking
            error_parts = []
            category_parts = []
            for err in schema_errors:
                error_parts.append(f"'{err.replace(chr(39), chr(39) * 2)}'")
                category_parts.append("'schema'")

            for i, rule in enumerate(row_rules):
                err_msg = f"Rule failed: {rule.name} ({rule.sql})".replace("'", "''")
                error_parts.append(f"CASE WHEN _rule_{i} IS NULL OR NOT _rule_{i} THEN '{err_msg}' ELSE NULL END")
                cat_msg = getattr(rule, "category", "data_quality").replace("'", "''")
                category_parts.append(f"CASE WHEN _rule_{i} IS NULL OR NOT _rule_{i} THEN '{cat_msg}' ELSE NULL END")

            for err_col in getattr(self, "_type_err_cols", []):
                error_parts.append(f'"{err_col}"')
                category_parts.append(f"CASE WHEN \"{err_col}\" IS NOT NULL THEN 'schema' ELSE NULL END")

            if error_parts:
                error_array = f"list_value({', '.join(error_parts)})"
                category_array = f"list_value({', '.join(category_parts)})"
                # Filter out NULLs from the array
                self.con.sql(
                    f"CREATE OR REPLACE TEMP TABLE _with_errors AS "
                    f"SELECT *, "
                    f"list_filter({error_array}, x -> x IS NOT NULL) AS {self.ERROR_COLUMN}, "
                    f"list_filter({category_array}, x -> x IS NOT NULL) AS {self.CATEGORY_COLUMN} "
                    f"FROM _evaluated"
                )
            else:
                self.con.sql(
                    f"CREATE OR REPLACE TEMP TABLE _with_errors AS "
                    f"SELECT *, "
                    f"CAST(list_value() AS VARCHAR[]) AS {self.ERROR_COLUMN}, "
                    f"CAST(list_value() AS VARCHAR[]) AS {self.CATEGORY_COLUMN} "
                    f"FROM _evaluated"
                )

            eval_count = self.con.sql("SELECT COUNT(*) FROM _with_errors").fetchone()[0]
            self._add_trace(
                "Row Rules Evaluation",
                input_rows=eval_count,
                output_rows=eval_count,
                duration_ms=(time.perf_counter() - step_start) * 1000,
                details={"rules_count": len(row_rules)},
            )
        else:
            self.con.sql(
                f"CREATE OR REPLACE TEMP TABLE _with_errors AS "
                f"SELECT *, "
                f"CAST(list_value() AS VARCHAR[]) AS {self.ERROR_COLUMN}, "
                f"CAST(list_value() AS VARCHAR[]) AS {self.CATEGORY_COLUMN} "
                f"FROM {current_table}"
            )

        # 5. Split good/bad
        internal_cols = [f"_rule_{i}" for i in range(len(row_rules))] + getattr(self, "_type_err_cols", [])
        drop_list = internal_cols + [self.ERROR_COLUMN, self.CATEGORY_COLUMN]
        drop_clause = ", ".join(f'"{c}"' for c in drop_list) if drop_list else ""

        # Good rows: no errors
        exclude_clause = f" EXCLUDE ({drop_clause})" if drop_clause else ""
        self.con.sql(
            f"CREATE OR REPLACE VIEW _good AS "
            f"SELECT *{exclude_clause} FROM _with_errors "
            f"WHERE len({self.ERROR_COLUMN}) = 0"
        )

        # Bad rows: have errors
        internal_drop = ", ".join(f'"{c}"' for c in internal_cols) if internal_cols else ""
        bad_exclude = f" EXCLUDE ({internal_drop})" if internal_drop else ""
        self.con.sql(
            f"CREATE OR REPLACE VIEW _bad AS "
            f"SELECT *{bad_exclude}, "
            f"'active' AS quarantine_state, "
            f"false AS quarantine_reprocessed "
            f"FROM _with_errors "
            f"WHERE len({self.ERROR_COLUMN}) > 0"
        )

        # 6. Dataset rules on good data
        dataset_name = self.contract.dataset or "source"
        self.con.sql(f"CREATE OR REPLACE VIEW {dataset_name} AS SELECT * FROM _good")
        if dataset_name != "source":
            self.con.sql("CREATE OR REPLACE VIEW source AS SELECT * FROM _good")

        self._run_dataset_rules(dataset_name)

        # 7. Collect results
        include_errors = True
        if self.contract.quarantine:
            include_errors = self.contract.quarantine.include_error_reason

        good_df = self._to_output_df(self.con.sql("SELECT * FROM _good"))

        if not include_errors:
            bad_exclude_err = f' EXCLUDE ("{self.ERROR_COLUMN}", "{self.CATEGORY_COLUMN}")'
            bad_df = self._to_output_df(self.con.sql(f"SELECT *{bad_exclude_err} FROM _bad"))
        else:
            bad_df = self._to_output_df(self.con.sql("SELECT * FROM _bad"))

        return good_df, bad_df

    def _run_dataset_rules(self, table_name: str) -> None:
        """Execute dataset-level quality rules."""
        rules = self.get_dataset_rules()
        if not rules:
            return

        for rule in rules:
            try:
                sql = rule.sql.replace("{dataset}", table_name)
                res = self.con.sql(sql).fetchone()
                val = res[0] if res else None

                passed = True
                expected = ""
                if val is None:
                    passed = False
                elif rule.must_be_between:
                    passed = rule.must_be_between[0] <= val <= rule.must_be_between[1]
                    expected = f"(expected {rule.must_be_between[0]} to {rule.must_be_between[1]})"
                elif rule.must_be_less_than is not None:
                    passed = val < rule.must_be_less_than
                    expected = f"(expected < {rule.must_be_less_than})"
                elif rule.must_be_greater_than is not None:
                    passed = val > rule.must_be_greater_than
                    expected = f"(expected > {rule.must_be_greater_than})"

                status = "PASS" if passed else "FAIL"
                logger.info(f"Quality Check: {rule.name} | Result: {val} {expected} | Status: {status}")
                self.dataset_rule_results.append(
                    {
                        "name": rule.name,
                        "value": f"{val} {expected}".strip(),
                        "passed": passed,
                        "description": rule.description,
                    }
                )
            except Exception as e:
                logger.error(f"Error executing dataset rule '{rule.name}': {e}")

    def close(self):
        """Close the DuckDB connection if we own it."""
        if self._owns_connection and self.con:
            self.con.close()
            self.con = None
