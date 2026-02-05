import duckdb
from typing import Tuple, Any, List
from lakeguard.engines.base import EngineAdapter
from loguru import logger
from pathlib import Path

class DuckDBAdapter(EngineAdapter):
    """
    DuckDB execution engine for LakeGuard.
    Handles data processing using native DuckDB SQL.
    """

    def execute(self, df: Any) -> Tuple[Any, Any]:
        """
        Executes the contract using DuckDB.
        Works with DuckDB relations or existing DataFrames.

        Args:
            df: Input dataframe or DuckDB relation.

        Returns:
            Tuple of (good_df, bad_df).
        """
        self.dataset_rule_results = []
        self.schema_drift = {}
        con = duckdb.connect(database=':memory:')
        
        # Register the input and dependencies
        tbl_name = self.contract.dataset or "source"
        con.register(tbl_name, df)
        self._register_links(con)

        # 0. Apply pre-processing (renames, filters, deduplication)
        processed_tbl = self._apply_pre_transformations(con, tbl_name)
        
        # 0.5 Apply schema enforcement
        schema_tbl, schema_errors = self._apply_schema(con, processed_tbl)
        
        # 1. Evaluate Row-Level Rules
        row_rules = self.get_row_rules()
        rule_exprs = []
        for i, rule in enumerate(row_rules):
            rule_exprs.append(f"CAST(({rule.sql}) AS BOOLEAN) as _rule_{i}")
        
        eval_sql = f"SELECT *, {', '.join(rule_exprs) if rule_exprs else '1 as _dummy'} FROM {schema_tbl}"
        con.execute(f"CREATE OR REPLACE VIEW eval_results AS {eval_sql}")
        
        # 2. Accumulate errors into arrays
        error_clauses = []
        category_clauses = []

        if schema_errors:
            error_clauses.extend([f"'{err}'" for err in schema_errors])
            category_clauses.extend([f"'schema'" for _ in schema_errors])
        
        for i, rule in enumerate(row_rules):
            error_msg = f"Rule failed: {rule.name} ({rule.sql})"
            error_clauses.append(f"CASE WHEN _rule_{i} IS NOT TRUE THEN '{error_msg}' ELSE NULL END")
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
        
        # 3. Split Good and Bad
        bad_df = con.execute(f"""
            SELECT *, 'active' as quarantine_state, FALSE as quarantine_reprocessed 
            FROM final_results 
            WHERE len({self.ERROR_COLUMN}) > 0
        """).df()
        
        good_rel = con.execute(f"SELECT * EXCLUDE (_rule_*, _dummy, {self.ERROR_COLUMN}, {self.CATEGORY_COLUMN}) FROM final_results WHERE len({self.ERROR_COLUMN}) = 0")
        
        # 4. Apply Dataset-Level Checks
        self._run_dataset_rules(good_rel, con)

        # 5. Apply heavy Transformations
        good_df = self._apply_post_transformations(good_rel, con)
            
        include_errors = True
        if self.contract.quarantine:
            include_errors = self.contract.quarantine.include_error_reason

        if not include_errors:
            bad_df = bad_df.drop(columns=[self.ERROR_COLUMN, self.CATEGORY_COLUMN], errors="ignore")

        return good_df, bad_df

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
                    cast_exprs = [f"CAST({c} AS VARCHAR) AS {c}" for c in cols]
                    con.execute(f"CREATE OR REPLACE VIEW schema_applied AS SELECT {', '.join(cast_exprs)} FROM {tbl_name}")
                    return "schema_applied", []
            return tbl_name, []

        cols = [row[0] for row in con.execute(f"DESCRIBE {tbl_name}").fetchall()]
        existing = set(cols)
        expected_fields = [f.name for f in self.contract.model.fields]
        expected = set(expected_fields)

        missing = expected - existing
        unknown = existing - expected

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
                    select_exprs.append(f"CAST({field.name} AS {duck_type}) AS {field.name}")
                else:
                    select_exprs.append(field.name)
            else:
                if duck_type:
                    select_exprs.append(f"CAST(NULL AS {duck_type}) AS {field.name}")
                else:
                    select_exprs.append(f"NULL AS {field.name}")

        if policy in ["allow", "quarantine"] and unknown:
            if cast_to_string:
                select_exprs.extend([f"CAST({c} AS VARCHAR) AS {c}" for c in sorted(unknown)])
            else:
                select_exprs.extend(sorted(unknown))

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
                if rule.must_be_between:
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
        
        for trans in self.contract.transformations:
            if trans.sql and (trans.phase or "post").lower() == "pre":
                logger.debug(f"Pre-Transform [SQL]: {trans.sql}")
                view_name = f"pre_trans_{view_idx}"
                con.execute(f"CREATE OR REPLACE VIEW source AS SELECT * FROM {current_tbl}")
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {trans.sql}")
                current_tbl = view_name
                view_idx += 1
                continue

            view_name = f"pre_trans_{view_idx}"
            if trans.rename:
                cols = self._get_columns(con, current_tbl)
                if trans.rename.from_name not in cols:
                    logger.warning(f"Pre-Transform [Rename] skipped; column not found: {trans.rename.from_name}")
                else:
                    logger.debug(f"Pre-Transform [Rename]: {trans.rename.from_name} -> {trans.rename.to_name}")
                    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * REPLACE ({trans.rename.from_name} AS {trans.rename.to_name}) FROM {current_tbl}")
                    current_tbl = view_name
                    view_idx += 1
            elif trans.select:
                logger.debug(f"Pre-Transform [Select]: {trans.select.columns}")
                select_cols = [col for col in trans.select.columns if col]
                if select_cols:
                    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(select_cols)} FROM {current_tbl}")
                    current_tbl = view_name
                    view_idx += 1
            elif trans.drop:
                logger.debug(f"Pre-Transform [Drop]: {trans.drop.columns}")
                cols = self._get_columns(con, current_tbl)
                keep = [col for col in cols if col not in set(trans.drop.columns)]
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
                        exprs.append(f"CAST({col} AS {duck_type}) AS {col}")
                    else:
                        exprs.append(col)
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.trim:
                logger.debug(f"Pre-Transform [Trim]: {trans.trim.fields}")
                cols = self._get_columns(con, current_tbl)
                exprs = []
                for col in cols:
                    if col in trans.trim.fields:
                        if trans.trim.side == "left":
                            exprs.append(f"LTRIM({col}) AS {col}")
                        elif trans.trim.side == "right":
                            exprs.append(f"RTRIM({col}) AS {col}")
                        else:
                            exprs.append(f"TRIM({col}) AS {col}")
                    else:
                        exprs.append(col)
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.lower:
                logger.debug(f"Pre-Transform [Lower]: {trans.lower.fields}")
                cols = self._get_columns(con, current_tbl)
                exprs = []
                for col in cols:
                    if col in trans.lower.fields:
                        exprs.append(f"LOWER({col}) AS {col}")
                    else:
                        exprs.append(col)
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.upper:
                logger.debug(f"Pre-Transform [Upper]: {trans.upper.fields}")
                cols = self._get_columns(con, current_tbl)
                exprs = []
                for col in cols:
                    if col in trans.upper.fields:
                        exprs.append(f"UPPER({col}) AS {col}")
                    else:
                        exprs.append(col)
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.coalesce:
                sources = trans.coalesce.sources or []
                if not sources:
                    sources = [trans.coalesce.field]
                expr_parts = ", ".join(sources + ([self._format_literal(trans.coalesce.default)] if trans.coalesce.default is not None else []))
                output = trans.coalesce.output or trans.coalesce.field
                expr = f"COALESCE({expr_parts}) AS {output}"
                cols = self._get_columns(con, current_tbl)
                exprs = []
                replaced = False
                for col in cols:
                    if col == output:
                        exprs.append(expr)
                        replaced = True
                    else:
                        exprs.append(col)
                if not replaced:
                    exprs.append(expr)
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.split:
                output = trans.split.output or trans.split.field
                cols = self._get_columns(con, current_tbl)
                exprs = []
                split_expr = f"str_split({trans.split.field}, {self._format_literal(trans.split.delimiter)}) AS {output}"
                replaced = False
                for col in cols:
                    if col == output:
                        exprs.append(split_expr)
                        replaced = True
                    else:
                        exprs.append(col)
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
                exprs = select_cols + [f"unnest({trans.explode.field}) AS {output}"]
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                current_tbl = view_name
                view_idx += 1
            elif trans.map_values:
                field = trans.map_values.field
                mapping = trans.map_values.mapping or {}
                if mapping:
                    cases = []
                    for key, value in mapping.items():
                        cases.append(f"WHEN {field} = {self._format_literal(key)} THEN {self._format_literal(value)}")
                    default_expr = self._format_literal(trans.map_values.default) if trans.map_values.default is not None else field
                    case_expr = f"CASE {' '.join(cases)} ELSE {default_expr} END"
                    output = trans.map_values.output or field
                    cols = self._get_columns(con, current_tbl)
                    exprs = []
                    replaced = False
                    for col in cols:
                        if col == output:
                            exprs.append(f"{case_expr} AS {output}")
                            replaced = True
                        else:
                            exprs.append(col)
                    if not replaced:
                        exprs.append(f"{case_expr} AS {output}")
                    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(exprs)} FROM {current_tbl}")
                    current_tbl = view_name
                    view_idx += 1
            elif trans.filter:
                logger.debug(f"Pre-Transform [Filter]: {trans.filter.sql}")
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {current_tbl} WHERE {trans.filter.sql}")
                current_tbl = view_name
                view_idx += 1
            elif trans.deduplicate:
                logger.debug(f"Pre-Transform [Deduplicate]: {trans.deduplicate.on}")
                on_cols = ", ".join(trans.deduplicate.on)
                order_clause = ""
                if trans.deduplicate.sort_by:
                    cols = ", ".join(trans.deduplicate.sort_by)
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
            return rel.df()
            
        current_rel = rel
        tbl_alias = "src_tbl"
        
        for trans in self.contract.transformations:
            if trans.sql and (trans.phase or "post").lower() != "pre":
                logger.debug(f"Post-Transform [SQL]: {trans.sql}")
                current_rel.create_view("source")
                current_rel = con.query(trans.sql)
                continue

            if trans.derive:
                logger.debug(f"Post-Transform [Derive]: {trans.derive.field}")
                current_rel.create_view(tbl_alias)
                query = f"SELECT *, ({trans.derive.sql}) AS {trans.derive.field} FROM {tbl_alias}"
                current_rel = con.query(query)
            elif trans.lookup:
                logger.debug(f"Post-Transform [Lookup]: {trans.lookup.field}")
                current_rel.create_view("src")
                value_expr = f"ref.{trans.lookup.value}"
                if trans.lookup.default_value is not None:
                    value_expr = f"COALESCE(ref.{trans.lookup.value}, {self._format_literal(trans.lookup.default_value)})"
                query = f"""
                SELECT src.*, {value_expr} AS {trans.lookup.field}
                FROM src
                LEFT JOIN {trans.lookup.reference} ref ON src.{trans.lookup.on} = ref.{trans.lookup.key}
                """
                current_rel = con.query(query)
            elif trans.join:
                logger.debug(f"Post-Transform [Join]: {trans.join.reference}")
                current_rel.create_view("source")
                query = self._build_join_sql(trans.join)
                current_rel = con.query(query)
            elif trans.filter:
                 # Also allow filters in post-transformation if needed
                logger.debug(f"Post-Transform [Filter]: {trans.filter.sql}")
                current_rel.create_view(tbl_alias)
                query = f"SELECT * FROM {tbl_alias} WHERE {trans.filter.sql}"
                current_rel = con.query(query)
        
        return current_rel.df()

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
                expr = f"COALESCE(ref.{field}, {self._format_literal(default)}) AS {alias}"
            else:
                expr = f"ref.{field} AS {alias}"
            select_fields.append(expr)

        return f"""
        SELECT {', '.join(select_fields)}
        FROM {source_table} src
        {join_type} JOIN {join_cfg.reference} ref ON src.{join_cfg.on} = ref.{join_cfg.key}
        """
