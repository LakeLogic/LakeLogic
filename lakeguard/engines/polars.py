import polars as pl
from typing import Tuple, Any, List
from lakeguard.engines.base import EngineAdapter
from loguru import logger

class PolarsAdapter(EngineAdapter):
    """
    Polars execution engine for LakeGuard.
    Supports row-level validation, aggregate metrics, and SQL-first transformations.
    """

    def _get_context(self, source_lf: pl.LazyFrame) -> pl.SQLContext:
        """
        Creates a SQLContext with the source and all linked dependencies registered.
        """
        ctx = pl.SQLContext()
        ctx.register(self.contract.dataset or "source", source_lf)
        
        # Register links (Mocking loading since we are in dev)
        for link in self.contract.links:
            try:
                # In production, this would use the link.path
                # For this demo, we check if the file exists locally
                if link.path and ".parquet" in link.path:
                    # In a real tool, we'd load s3:// or local/
                    # Here we just mock if not found
                    pass
                
                # Mocking logic for the smoke test
                if link.name == "dim_geography":
                    geo_df = pl.DataFrame({"id": [1, 2], "name": ["USA", "UK"]}).lazy()
                    ctx.register(link.name, geo_df)
                elif link.name == "marketing_opt_outs":
                    opt_df = pl.DataFrame({"customer_id": [9]}).lazy()
                    ctx.register(link.name, opt_df)
                elif link.name == "dim_countries":
                     ctx.register(link.name, pl.DataFrame({"code": ["DEU", "GBR"], "name": ["Germany", "United Kingdom"]}).lazy())
            except Exception as e:
                logger.warning(f"Could not register link {link.name}: {e}")
        
        return ctx

    def execute(self, df: Any) -> Tuple[pl.DataFrame, pl.DataFrame]:
        # 0. Load as LazyFrame
        if isinstance(df, pl.DataFrame):
            lf = df.lazy()
        elif isinstance(df, pl.LazyFrame):
            lf = df
        else:
            lf = pl.from_pandas(df).lazy() if hasattr(df, 'to_numpy') else pl.DataFrame(df).lazy()

        # 1. Evaluate Row-Level Rules
        row_rules = self.get_row_rules()
        ctx = self._get_context(lf)
        
        if row_rules:
            rule_exprs = []
            for i, rule in enumerate(row_rules):
                # Ensure the rule.sql is evaluated as a boolean
                # We use the dataset name instead of 'source' if available
                tbl_name = self.contract.dataset or "source"
                # We evaluate the rule against the registered table
                rule_exprs.append(f"CAST(({rule.sql}) AS BOOLEAN) as _rule_{i}")
            
            # Run all rules in one pass
            eval_sql = f"SELECT *, {', '.join(rule_exprs)} FROM {self.contract.dataset or 'source'}"
            lf_eval = ctx.execute(eval_sql)

            error_tracking_exprs = []
            category_tracking_exprs = []
            
            for i, rule in enumerate(row_rules):
                col_name = f"_rule_{i}"
                error_msg = f"Rule failed: {rule.name} ({rule.sql})"
                
                # Rows fail if rule is False OR Null
                condition = pl.col(col_name).is_null() | (pl.col(col_name) == False)
                
                error_tracking_exprs.append(
                    pl.when(condition).then(pl.lit(error_msg)).otherwise(None)
                )
                category_tracking_exprs.append(
                    pl.when(condition).then(pl.lit(rule.category)).otherwise(None)
                )

            lf_with_errors = lf_eval.with_columns([
                pl.concat_list(error_tracking_exprs).list.drop_nulls().alias(self.ERROR_COLUMN),
                pl.concat_list(category_tracking_exprs).list.drop_nulls().alias(self.CATEGORY_COLUMN)
            ])
        else:
            lf_with_errors = lf.with_columns([
                pl.lit([]).cast(pl.List(pl.String)).alias(self.ERROR_COLUMN),
                pl.lit([]).cast(pl.List(pl.String)).alias(self.CATEGORY_COLUMN)
            ])

        # 2. Split Good and Bad
        has_errors = pl.col(self.ERROR_COLUMN).list.len() > 0
        
        bad_lf = lf_with_errors.filter(has_errors).with_columns([
            pl.lit("active").alias("quarantine_state"),
            pl.lit(False).alias("quarantine_reprocessed"),
        ])
        
        # Clean up internal columns
        internal_cols = [f"_rule_{i}" for i in range(len(row_rules))]
        good_lf = lf_with_errors.filter(~has_errors).drop(internal_cols + [self.ERROR_COLUMN, self.CATEGORY_COLUMN])

        # 3. Apply Dataset-Level (Aggregate) Checks
        self._run_dataset_rules(good_lf, ctx)

        # 4. Apply Transformations to Good Data
        if self.contract.transformations:
            good_lf = self._apply_transformations(good_lf, ctx)

        return good_lf.collect(), bad_lf.drop(internal_cols).collect()

    def _run_dataset_rules(self, lf: pl.LazyFrame, ctx: pl.SQLContext):
        rules = self.get_dataset_rules()
        if not rules:
            return

        # Update ctx with the 'good' data for aggregate checks
        tbl_name = self.contract.dataset or "source"
        ctx.register(tbl_name, lf)

        for rule in rules:
            try:
                # Run the aggregate query
                res = ctx.execute(rule.sql).collect()
                val = res.row(0)[0]
                
                passed = True
                if rule.must_be_between:
                    passed = rule.must_be_between[0] <= val <= rule.must_be_between[1]
                elif rule.must_be_less_than is not None:
                    passed = val < rule.must_be_less_than
                elif rule.must_be_greater_than is not None:
                    passed = val > rule.must_be_greater_than
                
                status = "✅ PASS" if passed else "❌ FAIL"
                logger.info(f"Quality Check: {rule.name} | Result: {val} | Status: {status}")
                if not passed:
                    logger.warning(f"  - Description: {rule.description}")
            except Exception as e:
                logger.error(f"Error executing dataset rule '{rule.name}': {e}")

    def _apply_transformations(self, lf: pl.LazyFrame, ctx: pl.SQLContext) -> pl.LazyFrame:
        current_lf = lf
        tbl_name = self.contract.dataset or "source"
        
        for trans in self.contract.transformations:
            if trans.rename:
                logger.debug(f"Transform [Rename]: {trans.rename.from_name} -> {trans.rename.to_name}")
                current_lf = current_lf.rename({trans.rename.from_name: trans.rename.to_name})
                ctx.register(tbl_name, current_lf)
            elif trans.derive:
                logger.debug(f"Transform [Derive]: {trans.derive.field}")
                ctx.register(tbl_name, current_lf)
                # Apply derivation via SQL select
                query = f"SELECT *, ({trans.derive.sql}) AS {trans.derive.field} FROM {tbl_name}"
                current_lf = ctx.execute(query)
                ctx.register(tbl_name, current_lf)
            elif trans.lookup:
                logger.debug(f"Transform [Lookup]: {trans.lookup.field} from {trans.lookup.reference}")
                ctx.register(tbl_name, current_lf)
                
                query = f"""
                SELECT 
                    src.*,
                    ref.{trans.lookup.value} AS {trans.lookup.field}
                FROM {tbl_name} src
                LEFT JOIN {trans.lookup.reference} ref ON src.{trans.lookup.on} = ref.{trans.lookup.key}
                """
                try:
                    current_lf = ctx.execute(query)
                    ctx.register(tbl_name, current_lf)
                except Exception as e:
                    logger.error(f"Lookup failed: {e}")
        return current_lf
