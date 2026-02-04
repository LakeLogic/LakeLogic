import duckdb
from typing import Tuple, Any
from lakeguard.engines.base import EngineAdapter
from loguru import logger

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
        con = duckdb.connect(database=':memory:')
        
        # Register the input and dependencies
        tbl_name = self.contract.dataset or "source"
        con.register(tbl_name, df)
        
        # 1. Evaluate Row-Level Rules
        row_rules = self.get_row_rules()
        rule_exprs = []
        for i, rule in enumerate(row_rules):
            rule_exprs.append(f"CAST(({rule.sql}) AS BOOLEAN) as _rule_{i}")
        
        eval_sql = f"SELECT *, {', '.join(rule_exprs) if rule_exprs else '1 as _dummy'} FROM {tbl_name}"
        con.execute(f"CREATE VIEW eval_results AS {eval_sql}")
        
        # 2. Accumulate errors into arrays
        error_clauses = []
        category_clauses = []
        
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
        con.execute(f"CREATE VIEW final_results AS {agg_sql}")
        
        # 3. Split Good and Bad
        bad_df = con.execute(f"""
            SELECT *, 'active' as quarantine_state, FALSE as quarantine_reprocessed 
            FROM final_results 
            WHERE len({self.ERROR_COLUMN}) > 0
        """).df()
        
        good_rel = con.execute(f"SELECT * EXCLUDE (_rule_*, _dummy, {self.ERROR_COLUMN}, {self.CATEGORY_COLUMN}) FROM final_results WHERE len({self.ERROR_COLUMN}) = 0")
        
        # 4. Apply Dataset-Level Checks
        self._run_dataset_rules(good_rel, con)

        # 5. Apply Transformations
        good_df = self._apply_transformations(good_rel, con)
            
        return good_df, bad_df

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
            except Exception as e:
                logger.error(f"Error executing dataset rule '{rule.name}': {e}")

    def _apply_transformations(self, rel: Any, con: duckdb.DuckDBPyConnection) -> Any:
        if not self.contract.transformations:
            return rel.df()
            
        current_rel = rel
        tbl_name = "current_trans"
        
        for trans in self.contract.transformations:
            if trans.rename:
                current_rel = current_rel.project(f"* REPLACE ({trans.rename.from_name} AS {trans.rename.to_name})")
            elif trans.derive:
                current_rel.create_view(tbl_name)
                query = f"SELECT *, ({trans.derive.sql}) AS {trans.derive.field} FROM {tbl_name}"
                current_rel = con.query(query)
            elif trans.lookup:
                current_rel.create_view("src")
                query = f"""
                SELECT src.*, ref.{trans.lookup.value} AS {trans.lookup.field}
                FROM src
                LEFT JOIN {trans.lookup.reference} ref ON src.{trans.lookup.on} = ref.{trans.lookup.key}
                """
                current_rel = con.query(query)
        
        return current_rel.df()
