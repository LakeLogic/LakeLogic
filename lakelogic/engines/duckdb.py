"""
DuckDB Engine Adapter for LakeLogic.

Provides a first-class DuckDB execution path — all SQL transforms,
schema enforcement, and quality rules run directly in DuckDB's
in-process engine without Polars intermediation.

DuckDB is already used as a fallback inside PolarsAdapter; this adapter
makes it a standalone engine suitable for lightweight pipelines, Colab
demos, and SQL-heavy contracts.
"""

import os
import time
from pathlib import Path
from typing import Any, List, Tuple

from loguru import logger

from lakelogic.engines.base import EngineAdapter


def _read_ducklake_table_arrow(fq_table: str, columns=None):
    """Read a DuckLake table ('catalog.schema.table') into a PyArrow table.

    DuckLake IS DuckDB, so a `table:` link on the duckdb engine can be satisfied by
    reading it straight from the attached DuckLake catalog — local (file-backed) or
    MotherDuck ('md:'). Catalog metadata/data paths come from the env the DuckLake
    materializer also uses (DUCKLAKE_METADATA / DUCKLAKE_DATA_PATH); MotherDuck needs
    the motherduck_token env.
    """
    import duckdb

    meta = os.environ.get("DUCKLAKE_METADATA", "")
    catalog = fq_table.split(".")[0]
    if str(meta).startswith("md:"):
        con = duckdb.connect("md:")
        con.execute("INSTALL ducklake; LOAD ducklake;")
        con.execute(f'CREATE DATABASE IF NOT EXISTS "{catalog}" (TYPE DUCKLAKE)')
    else:
        data = os.environ.get("DUCKLAKE_DATA_PATH", "")
        con = duckdb.connect()
        con.execute("INSTALL ducklake; LOAD ducklake;")
        con.execute(f"ATTACH 'ducklake:{meta}' AS \"{catalog}\" (DATA_PATH '{data}')")
    try:
        cols = "*"
        if columns:
            cols = ", ".join(f'"{c}"' for c in columns)
        return con.execute(f"SELECT {cols} FROM {fq_table}").arrow()
    finally:
        con.close()


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
                table_path = link.path[6:] if link.path and link.path.startswith("table:") else None
                _table_ref = link.table or table_path
                # DuckLake table link — read it from the attached DuckLake catalog
                # (DuckLake is DuckDB) instead of the Spark-only skip below.
                if _table_ref and os.environ.get("DUCKLAKE_METADATA"):
                    try:
                        _pa = _read_ducklake_table_arrow(_table_ref, getattr(link, "columns", None))
                        # Materialise into a real table (not a registered arrow view): a
                        # registered arrow scan is single-use, but a fact may reference the
                        # link twice in one query (a join AND an IN-subquery), which would
                        # otherwise read 0 rows the second time.
                        self.con.register("_ll_link_src", _pa)
                        self.con.execute(f'CREATE OR REPLACE TABLE "{link.name}" AS SELECT * FROM _ll_link_src')
                        self.con.unregister("_ll_link_src")
                        logger.info(f"Registered DuckLake link '{link.name}' from {_table_ref}")
                        continue
                    except Exception as e:
                        logger.warning(f"DuckLake link '{link.name}' ({_table_ref}) failed: {e}")
                # Skip table-type links (Spark-only)
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

                # Link paths are STORAGE references — resolved by the
                # registry from {silver_path}/{bronze_path}/etc, not anchored
                # on the contract YAML's location. See materialization.py /
                # quarantine.py / run_log.py for the same separation.
                path = Path(link.path)
                if not path.exists():
                    logger.warning(f"Link file/directory not found: {path}")
                    continue

                # Defer projection to the post-pass when filter/query is present,
                # so the predicate may reference not-yet-projected columns.
                _defer_proj = bool(getattr(link, "filter", None) or getattr(link, "query", None))
                col_clause = ", ".join(f'"{c}"' for c in link.columns) if (link.columns and not _defer_proj) else "*"
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

        # Post-pass: load-time row subsetting for links declaring a portable
        # `filter` (WHERE) or an engine-specific `query` escape hatch. The inner
        # SELECT is materialized to a TEMP table first (eager), which breaks the
        # view self-reference so the final view can be replaced in place. Runs
        # after all registration branches so it applies uniformly. ``{link}``
        # refers to the just-registered dataset.
        for link in self.contract.links:
            _flt = getattr(link, "filter", None)
            _qry = getattr(link, "query", None)
            if not (_flt or _qry):
                continue
            try:
                inner = _qry.replace("{link}", link.name) if _qry else f"SELECT * FROM {link.name} WHERE {_flt}"
                tmp = f"_{link.name}__olc_sub"
                self.con.execute(f'CREATE OR REPLACE TEMP TABLE "{tmp}" AS {inner}')
                # Apply the deferred projection now (after the predicate).
                proj = "*"
                if link.columns:
                    avail = {r[0] for r in self.con.execute(f'DESCRIBE "{tmp}"').fetchall()}
                    keep = [c for c in link.columns if c in avail]
                    if keep:
                        proj = ", ".join(f'"{c}"' for c in keep)
                self.con.execute(f'CREATE OR REPLACE VIEW {link.name} AS SELECT {proj} FROM "{tmp}"')
                logger.debug(f"Link '{link.name}' subset via {'query' if _qry else 'filter'}")
            except Exception as e:
                logger.warning(f"Could not apply link subset for '{link.name}': {e}")

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

        # SCD2 mechanics columns are injected by the materializer AFTER this
        # check runs, so don't flag them as missing here.
        _scd2_injected = self._scd2_injected_columns()
        if _scd2_injected:
            missing = missing - _scd2_injected

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

        # to_date(x, 'yyyy-MM-dd') -> strptime(x, '%Y-%m-%d')::DATE  (Spark 2-arg form)
        sql = re.sub(
            r"\bto_date\s*\(\s*(.*?)\s*,\s*'(.*?)'\s*\)",
            _date_repl,
            sql,
            flags=re.IGNORECASE,
        )
        # to_date(x) -> CAST(x AS DATE)  (Spark 1-arg form; DuckDB has no to_date)
        sql = re.sub(
            r"\bto_date\s*\(\s*([^(),]+?)\s*\)",
            r"CAST(\1 AS DATE)",
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

            # Column-level shorthands (lower / upper / trim / cast) run BEFORE schema
            # enforcement — matching the Polars engine — so a `cast` that conflicts
            # with the declared model type is reconciled by schema enforcement the same
            # way on both engines. Applied regardless of phase (value rewrites).
            replace_expr = self._build_column_replace_sql(trans)
            if replace_expr:
                view_name = f"_pre_colshorthand_{id(replace_expr) & 0xFFFFFF:06x}"
                self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * REPLACE ({replace_expr}) FROM {current}")
                current = view_name
                continue

            # Coalesce: add (or overwrite) one column from the first non-null source.
            if trans.coalesce:
                sources = trans.coalesce.sources or [trans.coalesce.field]
                args = [f'"{c}"' for c in sources]
                if trans.coalesce.default is not None:
                    default = trans.coalesce.default
                    args.append(f"'{default}'" if isinstance(default, str) else str(default))
                output = trans.coalesce.output or trans.coalesce.field
                cols = self._get_current_columns(current)
                exclude = f' EXCLUDE ("{output}")' if output in cols else ""
                view_name = f"_pre_coalesce_{id(trans.coalesce) & 0xFFFFFF:06x}"
                self.con.sql(
                    f"CREATE OR REPLACE VIEW {view_name} AS "
                    f'SELECT *{exclude}, COALESCE({", ".join(args)}) AS "{output}" FROM {current}'
                )
                current = view_name
                continue

            # Select / drop — structural column projection (phase-agnostic).
            if getattr(trans, "select", None) and trans.select.columns:
                keep = ", ".join(f'"{c}"' for c in trans.select.columns)
                view_name = f"_pre_select_{id(trans.select) & 0xFFFFFF:06x}"
                self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {keep} FROM {current}")
                current = view_name
                continue
            if getattr(trans, "drop", None) and trans.drop.columns:
                dropped = ", ".join(f'"{c}"' for c in trans.drop.columns)
                view_name = f"_pre_drop_{id(trans.drop) & 0xFFFFFF:06x}"
                self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * EXCLUDE ({dropped}) FROM {current}")
                current = view_name
                continue

            # Split a string column into a LIST (string_split), matching Polars str.split.
            if getattr(trans, "split", None):
                output = trans.split.output or trans.split.field
                cols = self._get_current_columns(current)
                exclude = f' EXCLUDE ("{output}")' if output in cols else ""
                delim = str(trans.split.delimiter).replace("'", "''")
                view_name = f"_pre_split_{id(trans.split) & 0xFFFFFF:06x}"
                # to_json gives a compact JSON array string that matches the Polars
                # engine's list->string serialisation (both "[\"a\",\"b\"]").
                self.con.sql(
                    f"CREATE OR REPLACE VIEW {view_name} AS "
                    f'SELECT *{exclude}, to_json(string_split("{trans.split.field}", \'{delim}\')) AS "{output}" FROM {current}'  # noqa: E501
                )
                current = view_name
                continue

            # Explode a list column into one row per element (UNNEST).
            if getattr(trans, "explode", None):
                ex = trans.explode
                output = ex.output or ex.field
                cols = self._get_current_columns(current)
                if output == ex.field and ex.field in cols:
                    sql = f'SELECT * EXCLUDE ("{ex.field}"), UNNEST("{ex.field}") AS "{output}" FROM {current}'
                else:
                    sql = f'SELECT *, UNNEST("{ex.field}") AS "{output}" FROM {current}'
                view_name = f"_pre_explode_{id(ex) & 0xFFFFFF:06x}"
                self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")
                current = view_name
                continue

            # Map a column's values via a lookup, else default (or the original value).
            if getattr(trans, "map_values", None):
                mv = trans.map_values
                output = mv.output or mv.field
                whens = []
                for k, v in mv.mapping.items():
                    kq = str(k).replace("'", "''")
                    vq = f"'{str(v)}'" if isinstance(v, str) else str(v)
                    whens.append(f"WHEN \"{mv.field}\" = '{kq}' THEN {vq}")
                if mv.default is not None:
                    els = f"'{mv.default}'" if isinstance(mv.default, str) else str(mv.default)
                else:
                    els = f'"{mv.field}"'
                case_sql = f"CASE {' '.join(whens)} ELSE {els} END"
                cols = self._get_current_columns(current)
                exclude = f' EXCLUDE ("{output}")' if output in cols else ""
                view_name = f"_pre_mapvalues_{id(mv) & 0xFFFFFF:06x}"
                self.con.sql(
                    f'CREATE OR REPLACE VIEW {view_name} AS SELECT *{exclude}, {case_sql} AS "{output}" FROM {current}'
                )
                current = view_name
                continue

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

            # Deduplicate is applied regardless of `phase` to mirror the Polars
            # engine (whose filter/deduplicate branch is not phase-gated). Without
            # this, the duckdb engine silently skipped `deduplicate`, letting
            # duplicate keys through where Polars/Spark removed them.
            dedupe_cfg = self._resolve_deduplicate(trans)
            if dedupe_cfg and dedupe_cfg.on:
                logger.debug(f"Pre-Transform [Deduplicate]: {dedupe_cfg.on}")
                on_cols = ", ".join(f'"{c}"' for c in dedupe_cfg.on)
                if dedupe_cfg.sort_by:
                    direction = "DESC" if (dedupe_cfg.order or "desc").lower() == "desc" else "ASC"
                    order_by = ", ".join(f'"{c}" {direction}' for c in dedupe_cfg.sort_by)
                else:
                    # No sort key: keep an arbitrary single row per group, matching
                    # Polars' unique(subset=..., maintain_order=True).
                    order_by = "(SELECT 1)"
                view_name = f"_pre_dedup_{id(dedupe_cfg) & 0xFFFFFF:06x}"
                try:
                    self.con.sql(
                        f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {current} "
                        f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {on_cols} ORDER BY {order_by}) = 1"
                    )
                    current = view_name
                except Exception as e:
                    logger.warning(f"Pre-Transform [Deduplicate] failed: {e}")
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
                raw_filter = trans.filter.sql if hasattr(trans.filter, "sql") else str(trans.filter)
                filter_sql = self._normalize_spark_sql(raw_filter)
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

            # Date diff — tolerant of ISO (YYYY-MM-DD) and compact (YYYYMMDD, e.g. GA4)
            # date strings; date granularity to match the Polars engine.
            if trans.date_diff and getattr(trans.date_diff, "from_col", None) and getattr(
                trans.date_diff, "to_col", None
            ):
                dd = trans.date_diff
                unit = (getattr(dd, "unit", None) or "days").lower().rstrip("s")
                unit = unit if unit in {"day", "week", "month", "year", "hour", "minute", "second"} else "day"

                def _dparse(col: str) -> str:
                    return f"COALESCE(TRY_CAST(\"{col}\" AS DATE), TRY_STRPTIME(\"{col}\", '%Y%m%d')::DATE)"

                expr = f"DATEDIFF('{unit}', {_dparse(dd.from_col)}, {_dparse(dd.to_col)})"
                field = dd.field
                cols = self._get_current_columns(current)
                exclude = f' EXCLUDE ("{field}")' if field in cols else ""
                view_name = f"_post_datediff_{id(dd) & 0xFFFFFF:06x}"
                self.con.sql(
                    f"CREATE OR REPLACE VIEW {view_name} AS "
                    f'SELECT *{exclude}, ({expr}) AS "{field}" FROM {current}'
                )
                current = view_name
                continue

            # Lookup — enrich with a single column from a registered reference link.
            if trans.lookup:
                lu = trans.lookup
                cols = self._get_current_columns(current)
                # Exclude a pre-existing (schema-added) output column so it doesn't
                # collide with the joined value.
                src_star = f'src.* EXCLUDE ("{lu.field}")' if lu.field in cols else "src.*"
                view_name = f"_post_lookup_{id(lu) & 0xFFFFFF:06x}"
                self.con.sql(
                    f"CREATE OR REPLACE VIEW {view_name} AS "
                    f'SELECT {src_star}, ref."{lu.value}" AS "{lu.field}" '
                    f'FROM {current} src LEFT JOIN "{lu.reference}" ref '
                    f'ON src."{lu.on}" = ref."{lu.key}"'
                )
                current = view_name
                continue

            # Join — enrich with multiple columns from a registered reference link.
            if trans.join:
                jn = trans.join
                join_type = (jn.type or "left").upper()
                if join_type == "FULL":
                    join_type = "FULL OUTER"
                cols = self._get_current_columns(current)
                defaults = getattr(jn, "defaults", None) or {}
                aliases = [(f"{jn.prefix}{f}" if jn.prefix else f) for f in jn.fields]
                exclude = [a for a in aliases if a in cols]
                src_star = (
                    'src.* EXCLUDE (' + ", ".join(f'"{c}"' for c in exclude) + ")" if exclude else "src.*"
                )
                parts = [src_star]
                for f in jn.fields:
                    alias = f"{jn.prefix}{f}" if jn.prefix else f
                    if f in defaults and defaults[f] is not None:
                        parts.append(f'COALESCE(ref."{f}", {self._format_literal(defaults[f])}) AS "{alias}"')
                    else:
                        parts.append(f'ref."{f}" AS "{alias}"')
                view_name = f"_post_join_{id(jn) & 0xFFFFFF:06x}"
                self.con.sql(
                    f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(parts)} "
                    f'FROM {current} src {join_type} JOIN "{jn.reference}" ref '
                    f'ON src."{jn.on}" = ref."{jn.key}"'
                )
                current = view_name
                continue

            # Bucket — CASE-based binning via the shared cross-engine builder.
            if trans.bucket:
                bsql = self._build_bucket_sql(trans.bucket, source_table=current)
                if bsql:
                    field = trans.bucket.field
                    cols = self._get_current_columns(current)
                    if field in cols:
                        bsql = bsql.replace("SELECT *,", f'SELECT * EXCLUDE ("{field}"),')
                    view_name = f"_post_bucket_{id(trans.bucket) & 0xFFFFFF:06x}"
                    self.con.sql(f"CREATE OR REPLACE VIEW {view_name} AS {bsql}")
                    current = view_name
                continue

            # Date-range explode — one row per date in [start, end] (clamped to today,
            # null end -> today), matching the Polars engine.
            if trans.date_range_explode:
                cfg = trans.date_range_explode
                cols = self._get_current_columns(current)
                start_e = f'CAST("{cfg.start_col}" AS DATE)'
                if getattr(cfg, "end_col", None):
                    end_e = f'LEAST(COALESCE(CAST("{cfg.end_col}" AS DATE), current_date), current_date)'
                else:
                    end_e = "current_date"
                import re as _re

                _intv = str(getattr(cfg, "interval", None) or "1d").strip().lower()
                _m = _re.match(r"(\d+)\s*([a-z]+)", _intv)
                _num = _m.group(1) if _m else "1"
                _unit = {"d": "DAY", "day": "DAY", "days": "DAY", "w": "WEEK", "week": "WEEK",
                         "weeks": "WEEK", "mo": "MONTH", "month": "MONTH", "months": "MONTH"}.get(
                    _m.group(2) if _m else "d", "DAY"
                )
                interval = f"INTERVAL {_num} {_unit}"
                exclude = f' EXCLUDE ("{cfg.output}")' if cfg.output in cols else ""
                view_name = f"_post_daterange_{id(cfg) & 0xFFFFFF:06x}"
                self.con.sql(
                    f"CREATE OR REPLACE VIEW {view_name} AS "
                    f'SELECT *{exclude}, CAST(UNNEST(generate_series({start_e}, {end_e}, {interval})) AS DATE) '
                    f'AS "{cfg.output}" FROM {current}'
                )
                current = view_name
                continue

            # JSON extract — pull a JSON path into a (optionally cast) column.
            if trans.json_extract:
                cfg = trans.json_extract
                path = str(cfg.path).replace("'", "''")
                expr = f"json_extract_string(\"{cfg.source}\", '{path}')"
                if cfg.cast:
                    expr = f"CAST({expr} AS {self._DUCKDB_CAST_TYPES.get(str(cfg.cast).lower(), 'VARCHAR')})"
                cols = self._get_current_columns(current)
                exclude = f' EXCLUDE ("{cfg.field}")' if cfg.field in cols else ""
                view_name = f"_post_jsonextract_{id(cfg) & 0xFFFFFF:06x}"
                self.con.sql(
                    f'CREATE OR REPLACE VIEW {view_name} AS SELECT *{exclude}, {expr} AS "{cfg.field}" FROM {current}'
                )
                current = view_name
                continue

        return current

    # OLC scalar type name → DuckDB CAST type.
    _DUCKDB_CAST_TYPES = {
        "string": "VARCHAR", "str": "VARCHAR", "text": "VARCHAR",
        "int": "INTEGER", "integer": "INTEGER", "long": "BIGINT", "bigint": "BIGINT",
        "float": "FLOAT", "double": "DOUBLE", "decimal": "DOUBLE",
        "bool": "BOOLEAN", "boolean": "BOOLEAN",
        "date": "DATE", "timestamp": "TIMESTAMP", "datetime": "TIMESTAMP",
    }

    def _build_column_replace_sql(self, trans: Any) -> str:
        """Build the REPLACE(...) body for lower/upper/trim/cast; '' if none apply."""
        parts = []
        if getattr(trans, "lower", None):
            parts += [f'LOWER("{c}") AS "{c}"' for c in trans.lower.fields]
        if getattr(trans, "upper", None):
            parts += [f'UPPER("{c}") AS "{c}"' for c in trans.upper.fields]
        if getattr(trans, "trim", None):
            side = (getattr(trans.trim, "side", None) or "both").lower()
            fn = {"left": "LTRIM", "right": "RTRIM"}.get(side, "TRIM")
            parts += [f'{fn}("{c}") AS "{c}"' for c in trans.trim.fields]
        if getattr(trans, "cast", None):
            for col, dtype in trans.cast.columns.items():
                sql_type = self._DUCKDB_CAST_TYPES.get(str(dtype).lower(), "VARCHAR")
                parts.append(f'CAST("{col}" AS {sql_type}) AS "{col}"')
        return ", ".join(parts)

    # ── Main execution ────────────────────────────────────────────────────

    # Transform ops the DuckDB engine actually implements (pre + post passes).
    # `deduplicate_by_latest` is handled via `_resolve_deduplicate`.
    _DUCKDB_SUPPORTED_TRANSFORMS = frozenset(
        {"sql", "derive", "filter", "rename", "rollup", "pivot", "unpivot",
         "deduplicate", "deduplicate_by_latest",
         "lower", "upper", "trim", "cast", "coalesce",
         "select", "drop", "map_values", "json_extract", "date_diff", "split",
         "bucket", "lookup", "join", "explode", "date_range_explode"}
    )

    def _assert_supported_transforms(self) -> None:
        """Raise on any transform op this engine would silently no-op.

        The DuckDB engine implements a subset of the transform vocabulary. An
        unimplemented op (e.g. ``lower``, ``coalesce``, ``date_diff``,
        ``json_extract``) used to be skipped silently — producing wrong output
        with no error. Fail loud instead so the gap is impossible to miss.
        """
        from lakelogic.core.models import Transformation

        op_fields = [f for f in Transformation.model_fields if f != "phase"]
        for trans in (self.contract.transformations or []):
            for op in op_fields:
                if getattr(trans, op, None) and op not in self._DUCKDB_SUPPORTED_TRANSFORMS:
                    title = getattr(self.contract.info, "title", None) if self.contract.info else None
                    raise ValueError(
                        f"The DuckDB engine does not implement the '{op}' transformation "
                        f"and would silently skip it. Use the Polars/Spark engine, or express "
                        f"it as a `sql` / `derive` transform instead"
                        + (f" (contract: {title})" if title else "")
                        + "."
                    )

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

        # Fail loud rather than silently no-op on transforms this engine does not
        # implement (see _assert_supported_transforms).
        self._assert_supported_transforms()

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

        # 6. Dataset rules on good data — evaluate the POST-transform good snapshot.
        # The input was registered as a relation under "source"/dataset_name, which
        # shadows a CREATE VIEW of the same name; unregister it first so the redirect
        # to _good actually takes effect (otherwise rules evaluate the PRE-transform
        # input, disagreeing with the other engines when a transform changes the key).
        dataset_name = self.contract.dataset or "source"
        for _nm in {dataset_name, "source"}:
            try:
                self.con.unregister(_nm)
            except Exception:  # pragma: no cover - not a registered relation
                pass
            self.con.sql(f"CREATE OR REPLACE VIEW {_nm} AS SELECT * FROM _good")

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
                msg = str(e)
                # A dataset rule may reference a column that materialization injects
                # later — e.g. a surrogate key (`*_sk`) or SCD2 audit columns
                # (effective_from/to, is_current, version). At validation time that
                # column legitimately doesn't exist yet, and such keys are unique by
                # construction, so this isn't an error — just note it and move on.
                if "not found" in msg.lower() or "referenced column" in msg.lower():
                    logger.info(
                        f"Dataset rule '{rule.name}' not evaluated at validation: references a "
                        f"column not present yet (injected at materialization); enforced there."
                    )
                else:
                    logger.warning(f"Dataset rule '{rule.name}' could not be evaluated: {e}")

    def close(self):
        """Close the DuckDB connection if we own it."""
        if self._owns_connection and self.con:
            self.con.close()
            self.con = None
