import duckdb
import time
from typing import Tuple, Any, List
from lakelogic.engines.base import EngineAdapter
from loguru import logger
from pathlib import Path
from uuid import uuid4

class DuckDBAdapter(EngineAdapter):
    """
    DuckDB execution engine for LakeLogic.
    Handles data processing using native DuckDB SQL.
    """

    def execute(self, df: Any) -> Tuple[Any, Any]:
        start_time = time.perf_counter()
        self.dataset_rule_results = []
        self.schema_drift = {}
        self.trace = []

        # Register the input and dependencies
        # Use a shared connection for all operations in this execution
        self.con = duckdb.connect(database=':memory:')
        con = self.con
        
        # Use a internal unique name for the actual data to avoid "source" name collisions
        internal_input_name = "__lakelogic_raw_source__"
        self.internal_input_name = internal_input_name
        
        # Handle string paths directly via DuckDB native readers
        if isinstance(df, (str, Path)):
            path_str = str(df)
            ext = path_str.lower()
            if ext.endswith(".csv"):
                con.execute(f"CREATE OR REPLACE VIEW {internal_input_name} AS SELECT * FROM read_csv_auto('{path_str}')")
            elif ext.endswith(".parquet"):
                con.execute(f"CREATE OR REPLACE VIEW {internal_input_name} AS SELECT * FROM read_parquet('{path_str}')")
            elif ext.endswith(".xml"):
                import pandas as pd
                xml_df = pd.read_xml(path_str)
                con.register(internal_input_name, xml_df)
            elif ext.endswith((".xlsx", ".xls")):
                import pandas as pd
                excel_df = pd.read_excel(path_str)
                con.register(internal_input_name, excel_df)
            elif "/_delta_log" in ext or "delta" in ext:
                con.execute("INSTALL delta; LOAD delta;")
                con.execute(f"CREATE OR REPLACE VIEW {internal_input_name} AS SELECT * FROM delta_scan('{path_str}')")
            elif "iceberg" in ext:
                con.execute("INSTALL iceberg; LOAD iceberg;")
                con.execute(f"CREATE OR REPLACE VIEW {internal_input_name} AS SELECT * FROM iceberg_scan('{path_str}')")
            else:
                con.execute(f"CREATE OR REPLACE VIEW {internal_input_name} AS SELECT * FROM read_csv_auto('{path_str}')")
        else:
            con.register(internal_input_name, df)
            
        self._register_links(con)
        
        raw_count = self._get_row_count(con.table(internal_input_name))
        self._add_trace("Load Source", input_rows=None, output_rows=raw_count, duration_ms=(time.perf_counter() - start_time)*1000)


        # 0. Apply pre-processing (renames, filters, deduplication)
        step_start = time.perf_counter()
        processed_tbl = self._apply_pre_transformations(con, internal_input_name)
        pre_output_count = self._get_row_count(con.table(processed_tbl))
        self._add_trace("Pre-Transformations", input_rows=raw_count, output_rows=pre_output_count, duration_ms=(time.perf_counter() - step_start)*1000)

        
        # 0.5 Apply schema enforcement
        step_start = time.perf_counter()
        schema_tbl, schema_errors = self._apply_schema(con, processed_tbl)
        schema_output_count = self._get_row_count(con.table(schema_tbl))
        self._add_trace("Schema Enforcement", input_rows=pre_output_count, output_rows=schema_output_count, duration_ms=(time.perf_counter() - step_start)*1000, details={"errors": schema_errors})

        
        # 1. Evaluate Row-Level Rules
        row_rules = self.get_row_rules()
        rule_exprs = []
        for i, rule in enumerate(row_rules):
            rule_exprs.append(f"CAST(({rule.sql}) AS BOOLEAN) as _rule_{i}")
        
        eval_sql = f"SELECT *, {', '.join(rule_exprs) if rule_exprs else '1 as _dummy'} FROM {schema_tbl}"
        step_start = time.perf_counter()
        con.execute(f"CREATE OR REPLACE VIEW eval_results AS {eval_sql}")
        self._add_trace("Row Rules Evaluation", input_rows=schema_output_count, output_rows=schema_output_count, duration_ms=(time.perf_counter() - step_start)*1000, details={"sql": eval_sql, "rules_count": len(row_rules)})

        
        # 2. Accumulate errors into arrays
        error_clauses = []
        category_clauses = []

        if schema_errors:
            error_clauses.extend([f"'{err}'" for err in schema_errors])
            category_clauses.extend([f"'schema'" for _ in schema_errors])
        
        for i, rule in enumerate(row_rules):
            # Escape single quotes for SQL string literals
            escaped_msg = f"Rule failed: {rule.name} ({rule.sql})".replace("'", "''")
            error_clauses.append(f"CASE WHEN _rule_{i} IS NOT TRUE THEN '{escaped_msg}' ELSE NULL END")
            category_clauses.append(f"CASE WHEN _rule_{i} IS NOT TRUE THEN '{rule.category}' ELSE NULL END")
            
        if not error_clauses:
            agg_sql = f"SELECT *, CAST([] AS VARCHAR[]) as {self.ERROR_COLUMN}, CAST([] AS VARCHAR[]) as {self.CATEGORY_COLUMN} FROM eval_results"
        else:
            agg_sql = f"""
            SELECT 
                *,
                FILTER(list_value({', '.join(error_clauses)}), x -> x IS NOT NULL) as {self.ERROR_COLUMN},
                FILTER(list_value({', '.join(category_clauses)}), x -> x IS NOT NULL) as {self.CATEGORY_COLUMN}
            FROM eval_results
            """
        con.execute(f"CREATE OR REPLACE VIEW final_results AS {agg_sql}")
        
        include_errors = True
        if self.contract.quarantine:
            include_errors = self.contract.quarantine.include_error_reason

        # 3. Split Good and Bad (Return as Relations)
        # Build explicit exclude list since DuckDB doesn't support wildcards in EXCLUDE
        internal_cols = []
        if row_rules:
            internal_cols.extend([f"_rule_{i}" for i in range(len(row_rules))])
        else:
            internal_cols.append("_dummy")
            
        bad_exclude_cols = list(internal_cols)
        if not include_errors:
            bad_exclude_cols.extend([self.ERROR_COLUMN, self.CATEGORY_COLUMN])
        bad_exclude = ", ".join(bad_exclude_cols)
        bad_rel = con.sql(f"""
            SELECT * EXCLUDE ({bad_exclude}), 
                   'active' as quarantine_state, 
                   FALSE as quarantine_reprocessed 
            FROM final_results 
            WHERE len({self.ERROR_COLUMN}) > 0
        """)

        # Materialize bad rows to avoid view recursion/type changes later
        bad_view = f"bad_view_{uuid4().hex[:8]}"
        bad_table = f"__bad_buffer_{uuid4().hex[:8]}"
        bad_rel.create_view(bad_view)
        con.execute(f"CREATE OR REPLACE TEMP TABLE {bad_table} AS SELECT * FROM {bad_view}")
        bad_rel = con.table(bad_table)
        
        good_exclude = ", ".join(internal_cols + [self.ERROR_COLUMN, self.CATEGORY_COLUMN])
        good_rel = con.sql(f"SELECT * EXCLUDE ({good_exclude}) FROM final_results WHERE len({self.ERROR_COLUMN}) = 0")
        
        # 4. Apply Dataset-Level Checks
        self._run_dataset_rules(good_rel, con)

        # 5. Apply heavy Transformations
        step_start = time.perf_counter()
        post_input_count = self._get_row_count(good_rel)
        good_out = self._apply_post_transformations(good_rel, con)
        post_output_count = self._get_row_count(good_out)
        self._add_trace("Post-Transformations", input_rows=post_input_count, output_rows=post_output_count, duration_ms=(time.perf_counter() - step_start)*1000)

        # ── Materialise to Polars DataFrames before returning ─────────────
        # The temporary views/tables (post_view_*, __bad_buffer_*, etc.)
        # only live on this adapter's private in-memory connection.  If we
        # return lazy DuckDB relations, downstream code (lineage injection,
        # materialization) can't resolve those view names and fails with
        # "Table does not exist".  Collecting into Polars here:
        #   1. Executes the query while the connection is still alive
        #   2. Lets lineage injection use the well-tested Polars code path
        #   3. Gives materialization a concrete DataFrame to convert to Arrow
        import polars as pl

        def _rel_to_polars(rel):
            """Convert a DuckDB relation to a Polars DataFrame safely."""
            try:
                return pl.from_arrow(rel.arrow())
            except (ValueError, Exception):
                # Empty relation → .arrow() has no batches/schema.
                # Fall back to pandas which handles empty results.
                pdf = rel.df()
                return pl.from_pandas(pdf)

        good_df_result = _rel_to_polars(good_out)
        bad_df_result  = _rel_to_polars(bad_rel)

        # Connection can now be closed — all data is materialised
        con.close()
        return good_df_result, bad_df_result

    @classmethod
    def setup_extensions(cls):
        """
        Pre-install required DuckDB extensions for OSS data lakehouse support.
        """
        import duckdb
        con = duckdb.connect()
        extensions = ["iceberg", "delta", "httpfs", "aws", "azure"]
        logger.info(f"Pre-installing DuckDB extensions: {', '.join(extensions)}")
        for ext in extensions:
            try:
                con.execute(f"INSTALL {ext}")
                logger.debug(f"Extension {ext} installed.")
            except Exception as e:
                logger.warning(f"Failed to install extension {ext}: {e}")
        con.close()

    def _register_links(self, con: duckdb.DuckDBPyConnection) -> None:
        """
        Register linked reference datasets into DuckDB.

        Args:
            con: DuckDB connection.
        """
        for link in self.contract.links:
            try:
                table_path = link.path[6:] if link.path and link.path.startswith("table:") else None
                if link.table or (link.type and link.type.lower() == "table") or table_path:
                    table_name = link.table or table_path or link.path
                    logger.warning(f"Link '{link.name}' references table '{table_name}'. Table links are supported in Spark only for OSS.")
                    continue

                if not link.path:
                    continue

                if link.path.startswith(("s3://", "gs://", "abfss://", "adl://", "https://")):
                    logger.warning(f"Link '{link.name}' uses remote path '{link.path}'. Local-only loading supported in OSS demo.")
                    continue

                path = Path(link.path)
                if not path.is_absolute() and hasattr(self.contract, "_base_path"):
                    path = Path(self.contract._base_path) / path
                if not path.exists():
                    logger.warning(f"Link file not found: {path}")
                    continue

                if path.suffix.lower() == ".parquet":
                    con.execute(f"CREATE OR REPLACE VIEW {link.name} AS SELECT * FROM read_parquet('{path.as_posix()}')")
                elif path.suffix.lower() == ".csv":
                    con.execute(f"CREATE OR REPLACE VIEW {link.name} AS SELECT * FROM read_csv_auto('{path.as_posix()}')")
                else:
                    logger.warning(f"Unsupported link format for {link.name}: {path.suffix}")
            except Exception as e:
                logger.warning(f"Could not register link {link.name}: {e}")

    def _get_columns(self, con: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
        """
        Fetch column names for a DuckDB table/view.

        Args:
            con: DuckDB connection.
            table_name: Table or view name.

        Returns:
            List of column names.
        """
        return [row[0] for row in con.execute(f"DESCRIBE {table_name}").fetchall()]

    def _quote_ident(self, name: str) -> str:
        """Quote an identifier for DuckDB SQL."""
        text = str(name)
        if text.startswith('"') and text.endswith('"'):
            return text
        escaped = text.replace('"', '""')
        return f"\"{escaped}\""

    def _quote_qualified(self, name: str) -> str:
        """Quote dotted identifiers (schema.table) for DuckDB SQL."""
        text = str(name)
        if '"' in text:
            return text
        return ".".join(self._quote_ident(part) for part in text.split("."))

    def _qualify(self, alias: str, name: str) -> str:
        """Build a qualified identifier (alias + column)."""
        return f"{alias}.{self._quote_ident(name)}"

    def _to_duckdb_type(self, type_name: str) -> str:
        """
        Map contract type names to DuckDB SQL types.

        Args:
            type_name: Logical type name from contract.

        Returns:
            DuckDB type string.
        """
        type_name = (type_name or "").lower().strip()
        mapping = {
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
        return mapping.get(type_name)

    def _apply_schema(self, con: duckdb.DuckDBPyConnection, tbl_name: str) -> Tuple[str, list]:
        """
        Apply schema casts, missing columns, and unknown field handling.

        Args:
            con: DuckDB connection.
            tbl_name: Source table/view name.

        Returns:
            Tuple of (schema_view_name, schema_errors).
        """
        if not self.contract.model or not self.contract.model.fields:
            if self.contract.server and self.contract.server.mode == "ingest" and self.contract.server.cast_to_string:
                cols = [row[0] for row in con.execute(f"DESCRIBE {tbl_name}").fetchall()]
                if cols:
                    cast_exprs = [f"CAST({self._quote_ident(c)} AS VARCHAR) AS {self._quote_ident(c)}" for c in cols]
                    con.execute(f"CREATE OR REPLACE VIEW schema_applied AS SELECT {', '.join(cast_exprs)} FROM {tbl_name}")
                    return "schema_applied", []
            return tbl_name, []

        cols = [row[0] for row in con.execute(f"DESCRIBE {tbl_name}").fetchall()]
        existing = set(cols)
        expected_fields = [f.name for f in self.contract.model.fields]
        expected = set(expected_fields)

        missing = expected - existing
        unknown = existing - expected
        unknown = unknown - self._lineage_columns()

        server = self.contract.server
        evolution = None
        policy = self.contract.schema_policy.unknown_fields if self.contract.schema_policy else "allow"
        cast_to_string = False
        allow_schema_drift = True

        if server and server.mode == "ingest":
            evolution = (server.schema_evolution or "strict").lower()
            cast_to_string = bool(server.cast_to_string)
            allow_schema_drift = bool(server.allow_schema_drift)
            if evolution in ["append", "merge", "overwrite"]:
                policy = "allow"
            else:
                policy = "quarantine"

        select_exprs = []
        for field in self.contract.model.fields:
            duck_type = self._to_duckdb_type(field.type)
            if cast_to_string:
                duck_type = "VARCHAR"
            if field.name in existing:
                if duck_type:
                    qfield = self._quote_ident(field.name)
                    if cast_to_string:
                        select_exprs.append(f"CAST({qfield} AS {duck_type}) AS {qfield}")
                    else:
                        select_exprs.append(f"TRY_CAST({qfield} AS {duck_type}) AS {qfield}")
                else:
                    select_exprs.append(self._quote_ident(field.name))
            else:
                if duck_type:
                    qfield = self._quote_ident(field.name)
                    select_exprs.append(f"CAST(NULL AS {duck_type}) AS {qfield}")
                else:
                    select_exprs.append(f"NULL AS {self._quote_ident(field.name)}")

        if policy in ["allow", "quarantine"] and unknown:
            if cast_to_string:
                select_exprs.extend([f"CAST({self._quote_ident(c)} AS VARCHAR) AS {self._quote_ident(c)}" for c in sorted(unknown)])
            else:
                select_exprs.extend([self._quote_ident(c) for c in sorted(unknown)])

        schema_view = "schema_applied"
        con.execute(f"CREATE OR REPLACE VIEW {schema_view} AS SELECT {', '.join(select_exprs)} FROM {tbl_name}")

        schema_errors = []
        if evolution == "strict" and missing:
            schema_errors.append(f"Missing fields: {', '.join(sorted(missing))}")
        if policy == "quarantine" and unknown:
            schema_errors.append(f"Unknown fields present: {', '.join(sorted(unknown))}")

        self.schema_drift = {
            "missing_fields": sorted(missing),
            "unknown_fields": sorted(unknown),
            "policy": policy,
            "evolution": evolution or "",
            "allow_schema_drift": allow_schema_drift,
        }

        return schema_view, schema_errors

    def _run_dataset_rules(self, rel: Any, con: duckdb.DuckDBPyConnection):
        """
        Execute dataset-level quality rules.

        Args:
            rel: DuckDB relation of good records.
            con: DuckDB connection.
        """
        rules = self.get_dataset_rules()
        if not rules:
            return
        
        tbl_name = self.contract.dataset or "source"
        rel.create_view(tbl_name)

        for rule in rules:
            try:
                val = con.execute(rule.sql).fetchone()[0]
                passed = True
                if val is None:
                    passed = False
                elif rule.must_be_between:
                    passed = rule.must_be_between[0] <= val <= rule.must_be_between[1]
                elif rule.must_be_less_than is not None:
                    passed = val < rule.must_be_less_than
                elif rule.must_be_greater_than is not None:
                    passed = val > rule.must_be_greater_than
                
                status = "PASS" if passed else "FAIL"
                logger.info(f"Quality Check (DuckDB): {rule.name} | Result: {val} | Status: {status}")
                self.dataset_rule_results.append({
                    "name": rule.name,
                    "value": val,
                    "passed": passed,
                    "description": rule.description
                })
            except Exception as e:
                logger.error(f"Error executing dataset rule '{rule.name}': {e}")

    def _apply_pre_transformations(self, con: duckdb.DuckDBPyConnection, tbl_name: str) -> str:
        """
        Apply pre-processing transformations (rename, filter, deduplicate, and cleanup helpers) via SQL views.

        Args:
            con: DuckDB connection.
            tbl_name: Source table/view name.

        Returns:
            Name of the final view to use.
        """
        current_tbl = tbl_name
        view_idx = 0
        dataset_name = self.contract.dataset or "source"
        
        for trans in self.contract.transformations:
            trans_phase = (trans.phase or "post").lower()
            if trans.sql and (trans.phase or "post").lower() == "pre":
                logger.debug(f"Pre-Transform [SQL]: {trans.sql}")
                view_name = f"pre_trans_{view_idx}"
                
                # Setup aliases so users can reference either 'source' or the dataset name.
                # Use a temp table (materialized) to avoid recursive view binding.
                buffer_name = f"__pre_buffer_{uuid4().hex[:8]}"
                con.execute(f"CREATE OR REPLACE TEMP TABLE {buffer_name} AS SELECT * FROM {current_tbl}")
                con.execute(f"CREATE OR REPLACE VIEW {dataset_name} AS SELECT * FROM {buffer_name}")
                if dataset_name != "source":
                    con.execute(f"CREATE OR REPLACE VIEW source AS SELECT * FROM {buffer_name}")
                
                # Run the actual transformation
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {trans.sql}")
                current_tbl = view_name
                view_idx += 1
                continue

            if trans.pivot and trans_phase == "pre":
                pivot_sql = self._build_pivot_sql(trans.pivot, source_table=dataset_name)
                if not pivot_sql:
                    continue
                view_name = f"pre_trans_{view_idx}"

                buffer_name = f"__pre_buffer_{uuid4().hex[:8]}"
                con.execute(f"CREATE OR REPLACE TEMP TABLE {buffer_name} AS SELECT * FROM {current_tbl}")
                con.execute(f"CREATE OR REPLACE VIEW {dataset_name} AS SELECT * FROM {buffer_name}")
                if dataset_name != "source":
                    con.execute(f"CREATE OR REPLACE VIEW source AS SELECT * FROM {buffer_name}")

                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {pivot_sql}")
                current_tbl = view_name
                view_idx += 1
                continue

            if trans.unpivot and trans_phase == "pre":
                unpivot_sql = self._build_unpivot_sql(trans.unpivot, source_table=dataset_name)
                if not unpivot_sql:
                    continue
                view_name = f"pre_trans_{view_idx}"

                buffer_name = f"__pre_buffer_{uuid4().hex[:8]}"
                con.execute(f"CREATE OR REPLACE TEMP TABLE {buffer_name} AS SELECT * FROM {current_tbl}")
                con.execute(f"CREATE OR REPLACE VIEW {dataset_name} AS SELECT * FROM {buffer_name}")
                if dataset_name != "source":
                    con.execute(f"CREATE OR REPLACE VIEW source AS SELECT * FROM {buffer_name}")

                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {unpivot_sql}")
                current_tbl = view_name
                view_idx += 1
                continue

            view_name = f"pre_trans_{view_idx}"
            if trans.rename:
                cols = self._get_columns(con, current_tbl)
                rename_pairs = trans.rename.iter_pairs()
                if not rename_pairs:
                    continue
                rename_map = {}
                for from_col, to_col in rename_pairs:
                    if from_col not in cols:
                        logger.warning(f"Pre-Transform [Rename] skipped; column not found: {from_col}")
                        continue
                    logger.debug(f"Pre-Transform [Rename]: {from_col} -> {to_col}")
                    rename_map[from_col] = to_col
                if rename_map:
                    target_cols = set(rename_map.values())
                    exprs = []
                    for col in cols:
                        if col in rename_map:
                            exprs.append(f"{self._quote_ident(col)} AS {self._quote_ident(rename_map[col])}")
                        elif col in target_cols:
                            # Skip existing target columns to avoid duplicates.
                            continue
                        else:
                            exprs.append(self._quote_ident(col))
                    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                    current_tbl = view_name
                    view_idx += 1
            elif trans.select:
                logger.debug(f"Pre-Transform [Select]: {trans.select.columns}")
                select_cols = [self._quote_ident(col) for col in trans.select.columns if col]
                if select_cols:
                    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(select_cols)} FROM {current_tbl}")
                    current_tbl = view_name
                    view_idx += 1
            elif trans.drop:
                logger.debug(f"Pre-Transform [Drop]: {trans.drop.columns}")
                cols = self._get_columns(con, current_tbl)
                keep = [self._quote_ident(col) for col in cols if col not in set(trans.drop.columns)]
                if keep:
                    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(keep)} FROM {current_tbl}")
                    current_tbl = view_name
                    view_idx += 1
                else:
                    logger.warning("Pre-Transform [Drop] produced empty column set; skipping.")
            elif trans.cast:
                logger.debug(f"Pre-Transform [Cast]: {list(trans.cast.columns.keys())}")
                cols = self._get_columns(con, current_tbl)
                exprs = []
                for col in cols:
                    if col in trans.cast.columns:
                        duck_type = self._to_duckdb_type(trans.cast.columns[col]) or trans.cast.columns[col]
                        qcol = self._quote_ident(col)
                        exprs.append(f"CAST({qcol} AS {duck_type}) AS {qcol}")
                    else:
                        exprs.append(self._quote_ident(col))
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.trim:
                logger.debug(f"Pre-Transform [Trim]: {trans.trim.fields}")
                cols = self._get_columns(con, current_tbl)
                exprs = []
                for col in cols:
                    if col in trans.trim.fields:
                        qcol = self._quote_ident(col)
                        if trans.trim.side == "left":
                            exprs.append(f"LTRIM({qcol}) AS {qcol}")
                        elif trans.trim.side == "right":
                            exprs.append(f"RTRIM({qcol}) AS {qcol}")
                        else:
                            exprs.append(f"TRIM({qcol}) AS {qcol}")
                    else:
                        exprs.append(self._quote_ident(col))
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.lower:
                logger.debug(f"Pre-Transform [Lower]: {trans.lower.fields}")
                cols = self._get_columns(con, current_tbl)
                exprs = []
                for col in cols:
                    if col in trans.lower.fields:
                        qcol = self._quote_ident(col)
                        exprs.append(f"LOWER({qcol}) AS {qcol}")
                    else:
                        exprs.append(self._quote_ident(col))
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.upper:
                logger.debug(f"Pre-Transform [Upper]: {trans.upper.fields}")
                cols = self._get_columns(con, current_tbl)
                exprs = []
                for col in cols:
                    if col in trans.upper.fields:
                        qcol = self._quote_ident(col)
                        exprs.append(f"UPPER({qcol}) AS {qcol}")
                    else:
                        exprs.append(self._quote_ident(col))
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.coalesce:
                sources = trans.coalesce.sources or []
                if not sources:
                    sources = [trans.coalesce.field]
                expr_parts = ", ".join(
                    [self._quote_ident(part) for part in sources]
                    + ([self._format_literal(trans.coalesce.default)] if trans.coalesce.default is not None else [])
                )
                output = trans.coalesce.output or trans.coalesce.field
                expr = f"COALESCE({expr_parts}) AS {self._quote_ident(output)}"
                cols = self._get_columns(con, current_tbl)
                exprs = []
                replaced = False
                for col in cols:
                    if col == output:
                        exprs.append(expr)
                        replaced = True
                    else:
                        exprs.append(self._quote_ident(col))
                if not replaced:
                    exprs.append(expr)
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.split:
                output = trans.split.output or trans.split.field
                cols = self._get_columns(con, current_tbl)
                exprs = []
                split_expr = f"str_split({self._quote_ident(trans.split.field)}, {self._format_literal(trans.split.delimiter)}) AS {self._quote_ident(output)}"
                replaced = False
                for col in cols:
                    if col == output:
                        exprs.append(split_expr)
                        replaced = True
                    else:
                        exprs.append(self._quote_ident(col))
                if not replaced:
                    exprs.append(split_expr)
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.explode:
                output = trans.explode.output or trans.explode.field
                cols = self._get_columns(con, current_tbl)
                select_cols = [col for col in cols if col != output]
                if output == trans.explode.field:
                    select_cols = [col for col in cols if col != trans.explode.field]
                exprs = [self._quote_ident(col) for col in select_cols] + [f"unnest({self._quote_ident(trans.explode.field)}) AS {self._quote_ident(output)}"]
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.map_values:
                field = trans.map_values.field
                qfield = self._quote_ident(field)
                mapping = trans.map_values.mapping or {}
                if mapping:
                    cases = []
                    for key, value in mapping.items():
                        cases.append(f"WHEN {qfield} = {self._format_literal(key)} THEN {self._format_literal(value)}")
                    default_expr = self._format_literal(trans.map_values.default) if trans.map_values.default is not None else qfield
                    case_expr = f"CASE {' '.join(cases)} ELSE {default_expr} END"
                    output = trans.map_values.output or field
                    cols = self._get_columns(con, current_tbl)
                    exprs = []
                    replaced = False
                    for col in cols:
                        if col == output:
                            exprs.append(f"{case_expr} AS {self._quote_ident(output)}")
                            replaced = True
                        else:
                            exprs.append(self._quote_ident(col))
                    if not replaced:
                        exprs.append(f"{case_expr} AS {self._quote_ident(output)}")
                    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                    current_tbl = view_name
                    view_idx += 1
            elif trans.filter and trans_phase == "pre":
                logger.debug(f"Pre-Transform [Filter]: {trans.filter.sql}")
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {current_tbl} WHERE {trans.filter.sql}")
                current_tbl = view_name
                view_idx += 1
            elif trans.deduplicate and trans_phase == "pre":
                logger.debug(f"Pre-Transform [Deduplicate]: {trans.deduplicate.on}")
                on_cols = ", ".join(self._quote_ident(col) for col in trans.deduplicate.on)
                order_clause = ""
                if trans.deduplicate.sort_by:
                    cols = ", ".join(self._quote_ident(col) for col in trans.deduplicate.sort_by)
                    order_clause = f"ORDER BY {cols} {trans.deduplicate.order}"
                
                # Standard window function approach
                con.execute(f"""
                    CREATE OR REPLACE VIEW {view_name} AS 
                    SELECT * EXCLUDE(_rn) FROM (
                        SELECT *, ROW_NUMBER() OVER(PARTITION BY {on_cols} {order_clause}) as _rn
                        FROM {current_tbl}
                    ) WHERE _rn = 1
                """)
                current_tbl = view_name
                view_idx += 1
                
        return current_tbl

    def _apply_post_transformations(self, rel: Any, con: duckdb.DuckDBPyConnection) -> Any:
        """
        Apply derive, lookup, and join transformations.

        Args:
            rel: DuckDB relation of good records.
            con: DuckDB connection.

        Returns:
            Pandas dataframe of transformed records.
        """
        if not self.contract.transformations:
            return rel
            
        current_rel = rel
        dataset_name = self.contract.dataset or "source"
        
        for trans in self.contract.transformations:
            trans_phase = (trans.phase or "post").lower()
            if trans.sql and trans_phase != "pre":
                logger.debug(f"Post-Transform [SQL]: {trans.sql}")
                
                # Create a physical view from relation so we can reference it by name in SQL
                view_name = f"post_view_{uuid4().hex[:8]}"
                current_rel.create_view(view_name)
                buffer_name = f"__post_buffer_{uuid4().hex[:8]}"
                con.execute(f"CREATE OR REPLACE TEMP TABLE {buffer_name} AS SELECT * FROM {view_name}")
                
                # Provide aliases for the SQL snippet (avoid view recursion)
                con.execute(f"CREATE OR REPLACE VIEW {dataset_name} AS SELECT * FROM {buffer_name}")
                if dataset_name != "source":
                    con.execute(f"CREATE OR REPLACE VIEW source AS SELECT * FROM {buffer_name}")
                
                sql = trans.sql.replace("{dataset}", dataset_name).replace("{source}", "source")
                current_rel = con.sql(sql)
                continue
            if trans.rollup and trans_phase != "pre":
                rollup_sql = self._build_rollup_sql(trans.rollup, source_table=dataset_name)
                logger.debug(f"Post-Transform [Rollup]: {rollup_sql}")

                view_name = f"post_view_{uuid4().hex[:8]}"
                current_rel.create_view(view_name)
                buffer_name = f"__post_buffer_{uuid4().hex[:8]}"
                con.execute(f"CREATE OR REPLACE TEMP TABLE {buffer_name} AS SELECT * FROM {view_name}")

                con.execute(f"CREATE OR REPLACE VIEW {dataset_name} AS SELECT * FROM {buffer_name}")
                if dataset_name != "source":
                    con.execute(f"CREATE OR REPLACE VIEW source AS SELECT * FROM {buffer_name}")

                current_rel = con.sql(rollup_sql)
                continue

            if trans.pivot and trans_phase != "pre":
                pivot_sql = self._build_pivot_sql(trans.pivot, source_table=dataset_name)
                if not pivot_sql:
                    continue
                logger.debug(f"Post-Transform [Pivot]: {pivot_sql}")

                view_name = f"post_view_{uuid4().hex[:8]}"
                current_rel.create_view(view_name)
                buffer_name = f"__post_buffer_{uuid4().hex[:8]}"
                con.execute(f"CREATE OR REPLACE TEMP TABLE {buffer_name} AS SELECT * FROM {view_name}")

                con.execute(f"CREATE OR REPLACE VIEW {dataset_name} AS SELECT * FROM {buffer_name}")
                if dataset_name != "source":
                    con.execute(f"CREATE OR REPLACE VIEW source AS SELECT * FROM {buffer_name}")

                current_rel = con.sql(pivot_sql)
                continue

            if trans.unpivot and trans_phase != "pre":
                unpivot_sql = self._build_unpivot_sql(trans.unpivot, source_table=dataset_name)
                if not unpivot_sql:
                    continue
                logger.debug(f"Post-Transform [Unpivot]: {unpivot_sql}")

                view_name = f"post_view_{uuid4().hex[:8]}"
                current_rel.create_view(view_name)
                buffer_name = f"__post_buffer_{uuid4().hex[:8]}"
                con.execute(f"CREATE OR REPLACE TEMP TABLE {buffer_name} AS SELECT * FROM {view_name}")

                con.execute(f"CREATE OR REPLACE VIEW {dataset_name} AS SELECT * FROM {buffer_name}")
                if dataset_name != "source":
                    con.execute(f"CREATE OR REPLACE VIEW source AS SELECT * FROM {buffer_name}")

                current_rel = con.sql(unpivot_sql)
                continue

            if trans.derive and trans_phase != "pre":
                logger.debug(f"Post-Transform [Derive]: {trans.derive.field}")
                derive_view = f"post_view_{uuid4().hex[:8]}"
                current_rel.create_view(derive_view)
                
                # Check if field exists to avoid DuplicateError
                field_name = trans.derive.field
                existing_cols = []
                try:
                    existing_cols = current_rel.columns
                except:
                    pass
                
                if field_name in existing_cols:
                    # Replace existing column
                    query = f"SELECT * EXCLUDE ({self._quote_ident(field_name)}), ({trans.derive.sql}) AS {self._quote_ident(field_name)} FROM {derive_view}"
                else:
                    # Add new column
                    query = f"SELECT *, ({trans.derive.sql}) AS {self._quote_ident(field_name)} FROM {derive_view}"
                
                current_rel = con.query(query)
            elif trans.bucket and trans_phase != "pre":
                logger.debug(f"Post-Transform [Bucket]: {trans.bucket.field}")
                bucket_view = f"post_view_{uuid4().hex[:8]}"
                current_rel.create_view(bucket_view)
                sql = self._build_bucket_sql(trans.bucket, source_table=bucket_view)
                if sql:
                    current_rel = con.query(sql)
            elif trans.date_diff and trans_phase != "pre":
                logger.debug(f"Post-Transform [DateDiff]: {trans.date_diff.field}")
                dd_view = f"post_view_{uuid4().hex[:8]}"
                current_rel.create_view(dd_view)
                sql = self._build_date_diff_sql(trans.date_diff, source_table=dd_view)
                if sql:
                    current_rel = con.query(sql)
            elif trans.lookup and trans_phase != "pre":

                logger.debug(f"Post-Transform [Lookup]: {trans.lookup.field}")
                current_rel.create_view("src")
                value_expr = f"ref.{self._quote_ident(trans.lookup.value)}"
                if trans.lookup.default_value is not None:
                    value_expr = f"COALESCE(ref.{self._quote_ident(trans.lookup.value)}, {self._format_literal(trans.lookup.default_value)})"
                query = f"""
                SELECT src.*, {value_expr} AS {self._quote_ident(trans.lookup.field)}
                FROM src
                LEFT JOIN {self._quote_qualified(trans.lookup.reference)} ref ON src.{self._quote_ident(trans.lookup.on)} = ref.{self._quote_ident(trans.lookup.key)}
                """
                current_rel = con.query(query)
            elif trans.join and trans_phase != "pre":
                logger.debug(f"Post-Transform [Join]: {trans.join.reference}")
                current_rel.create_view("source")
                query = self._build_join_sql(trans.join)
                current_rel = con.query(query)
            elif trans.filter and trans_phase != "pre":
                # Also allow filters in post-transformation if needed
                logger.debug(f"Post-Transform [Filter]: {trans.filter.sql}")
                filter_view = f"post_view_{uuid4().hex[:8]}"
                current_rel.create_view(filter_view)
                query = f"SELECT * FROM {filter_view} WHERE {trans.filter.sql}"
                current_rel = con.query(query)
        
        return current_rel

    def _build_join_sql(self, join_cfg, source_table: str = "source") -> str:
        """
        Build SQL for a join transformation.

        Args:
            join_cfg: Join configuration.
            source_table: Source table/view name.

        Returns:
            SQL query string.
        """
        join_type = (join_cfg.type or "left").upper()
        if join_type == "FULL":
            join_type = "FULL OUTER"

        select_fields = ["src.*"]
        for field in join_cfg.fields:
            alias = f"{join_cfg.prefix}{field}" if join_cfg.prefix else field
            default = join_cfg.defaults.get(field) if join_cfg.defaults else None
            if default is not None:
                expr = f"COALESCE(ref.{self._quote_ident(field)}, {self._format_literal(default)}) AS {self._quote_ident(alias)}"
            else:
                expr = f"ref.{self._quote_ident(field)} AS {self._quote_ident(alias)}"
            select_fields.append(expr)

        return f"""
        SELECT {', '.join(select_fields)}
        FROM {source_table} src
        {join_type} JOIN {self._quote_qualified(join_cfg.reference)} ref ON src.{self._quote_ident(join_cfg.on)} = ref.{self._quote_ident(join_cfg.key)}
        """
