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
        """
        self.dataset_rule_results = []
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
        for link in self.contract.links:
            try:
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

    def _to_duckdb_type(self, type_name: str) -> str:
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
        if not self.contract.model or not self.contract.model.fields:
            return tbl_name, []

        cols = [row[0] for row in con.execute(f"DESCRIBE {tbl_name}").fetchall()]
        existing = set(cols)
        expected_fields = [f.name for f in self.contract.model.fields]
        expected = set(expected_fields)

        missing = expected - existing
        unknown = existing - expected

        policy = self.contract.schema_policy.unknown_fields if self.contract.schema_policy else "allow"

        select_exprs = []
        for field in self.contract.model.fields:
            duck_type = self._to_duckdb_type(field.type)
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
            select_exprs.extend(sorted(unknown))

        schema_view = "schema_applied"
        con.execute(f"CREATE OR REPLACE VIEW {schema_view} AS SELECT {', '.join(select_exprs)} FROM {tbl_name}")

        schema_errors = []
        if policy == "quarantine" and unknown:
            schema_errors.append(f"Unknown fields present: {', '.join(sorted(unknown))}")

        return schema_view, schema_errors

    def _run_dataset_rules(self, rel: Any, con: duckdb.DuckDBPyConnection):
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
                
                status = "✅ PASS" if passed else "❌ FAIL"
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
        """Apply filters, renames, and deduplication via SQL views."""
        current_tbl = tbl_name
        view_idx = 0
        
        for trans in self.contract.transformations:
            view_name = f"pre_trans_{view_idx}"
            if trans.rename:
                cols = [row[0] for row in con.execute(f"DESCRIBE {current_tbl}").fetchall()]
                if trans.rename.from_name not in cols:
                    logger.warning(f"Pre-Transform [Rename] skipped; column not found: {trans.rename.from_name}")
                else:
                    logger.debug(f"Pre-Transform [Rename]: {trans.rename.from_name} -> {trans.rename.to_name}")
                    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * REPLACE ({trans.rename.from_name} AS {trans.rename.to_name}) FROM {current_tbl}")
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
        """Apply derive and lookup transformations."""
        if not self.contract.transformations:
            return rel.df()
            
        current_rel = rel
        tbl_alias = "src_tbl"
        
        for trans in self.contract.transformations:
            if trans.derive:
                logger.debug(f"Post-Transform [Derive]: {trans.derive.field}")
                current_rel.create_view(tbl_alias)
                query = f"SELECT *, ({trans.derive.sql}) AS {trans.derive.field} FROM {tbl_alias}"
                current_rel = con.query(query)
            elif trans.lookup:
                logger.debug(f"Post-Transform [Lookup]: {trans.lookup.field}")
                current_rel.create_view("src")
                query = f"""
                SELECT src.*, ref.{trans.lookup.value} AS {trans.lookup.field}
                FROM src
                LEFT JOIN {trans.lookup.reference} ref ON src.{trans.lookup.on} = ref.{trans.lookup.key}
                """
                current_rel = con.query(query)
            elif trans.filter:
                 # Also allow filters in post-transformation if needed
                logger.debug(f"Post-Transform [Filter]: {trans.filter.sql}")
                current_rel.create_view(tbl_alias)
                query = f"SELECT * FROM {tbl_alias} WHERE {trans.filter.sql}"
                current_rel = con.query(query)
        
        return current_rel.df()
