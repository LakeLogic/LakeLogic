import polars as pl
from typing import Tuple, Any, List
from lakeguard.engines.base import EngineAdapter
from loguru import logger
from pathlib import Path

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
        self._register_links(ctx)
        
        return ctx

    def _register_links(self, ctx: pl.SQLContext) -> None:
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
                    link_lf = pl.read_parquet(path).lazy()
                elif path.suffix.lower() == ".csv":
                    link_lf = pl.read_csv(path).lazy()
                else:
                    logger.warning(f"Unsupported link format for {link.name}: {path.suffix}")
                    continue

                ctx.register(link.name, link_lf)
            except Exception as e:
                logger.warning(f"Could not register link {link.name}: {e}")

    def _to_polars_dtype(self, type_name: str):
        type_name = (type_name or "").lower().strip()
        mapping = {
            "string": pl.Utf8,
            "varchar": pl.Utf8,
            "text": pl.Utf8,
            "int": pl.Int64,
            "integer": pl.Int64,
            "long": pl.Int64,
            "bigint": pl.Int64,
            "float": pl.Float64,
            "double": pl.Float64,
            "decimal": pl.Float64,
            "bool": pl.Boolean,
            "boolean": pl.Boolean,
            "date": pl.Date,
            "timestamp": pl.Datetime,
            "datetime": pl.Datetime,
        }
        return mapping.get(type_name)

    def _apply_schema(self, lf: pl.LazyFrame) -> Tuple[pl.LazyFrame, List[str]]:
        if not self.contract.model or not self.contract.model.fields:
            return lf, []

        expected_fields = [f.name for f in self.contract.model.fields]
        existing = set(lf.columns)
        expected = set(expected_fields)

        missing = expected - existing
        unknown = existing - expected

        for col in missing:
            lf = lf.with_columns(pl.lit(None).alias(col))

        for field in self.contract.model.fields:
            dtype = self._to_polars_dtype(field.type)
            if dtype is not None:
                lf = lf.with_columns(pl.col(field.name).cast(dtype, strict=False))

        schema_errors: List[str] = []
        policy = self.contract.schema_policy.unknown_fields if self.contract.schema_policy else "allow"
        if policy == "drop" and unknown:
            lf = lf.drop(list(unknown))
        elif policy == "quarantine" and unknown:
            schema_errors.append(f"Unknown fields present: {', '.join(sorted(unknown))}")

        return lf, schema_errors

    def execute(self, df: Any) -> Tuple[pl.DataFrame, pl.DataFrame]:
        self.dataset_rule_results = []
        # 0. Load as LazyFrame
        if isinstance(df, pl.DataFrame):
            lf = df.lazy()
        elif isinstance(df, pl.LazyFrame):
            lf = df
        else:
            lf = pl.from_pandas(df).lazy() if hasattr(df, 'to_numpy') else pl.DataFrame(df).lazy()

        # 0.25 Apply renames, filters, deduplication before schema enforcement
        # This handles the 'supersede' case where we want to clean data ASAP
        if self.contract.transformations:
            lf = self._apply_pre_transformations(lf)

        # 0.5 Apply schema enforcement (casts, missing cols, unknowns)
        lf, schema_errors = self._apply_schema(lf)

        # 1. Evaluate Row-Level Rules
        row_rules = self.get_row_rules()
        ctx = self._get_context(lf)
        
        if row_rules:
            rule_exprs = []
            for i, rule in enumerate(row_rules):
                tbl_name = self.contract.dataset or "source"
                rule_exprs.append(f"CAST(({rule.sql}) AS BOOLEAN) as _rule_{i}")
            
            # Run all rules in one pass
            eval_sql = f"SELECT *, {', '.join(rule_exprs)} FROM {self.contract.dataset or 'source'}"
            lf_eval = ctx.execute(eval_sql)

            error_tracking_exprs = []
            category_tracking_exprs = []

            if schema_errors:
                error_tracking_exprs.extend([pl.lit(err) for err in schema_errors])
                category_tracking_exprs.extend([pl.lit("schema") for _ in schema_errors])
            
            for i, rule in enumerate(row_rules):
                col_name = f"_rule_{i}"
                error_msg = f"Rule failed: {rule.name} ({rule.sql})"
                condition = pl.col(col_name).is_null() | (pl.col(col_name) == False)
                
                error_tracking_exprs.append(pl.when(condition).then(pl.lit(error_msg)).otherwise(None))
                category_tracking_exprs.append(pl.when(condition).then(pl.lit(rule.category)).otherwise(None))

            lf_with_errors = lf_eval.with_columns([
                pl.concat_list(error_tracking_exprs).list.drop_nulls().alias(self.ERROR_COLUMN),
                pl.concat_list(category_tracking_exprs).list.drop_nulls().alias(self.CATEGORY_COLUMN)
            ])
        else:
            schema_error_exprs = [pl.lit(err) for err in schema_errors] if schema_errors else []
            schema_category_exprs = [pl.lit("schema") for _ in schema_errors] if schema_errors else []
            lf_with_errors = lf.with_columns([
                pl.concat_list(schema_error_exprs).list.drop_nulls().alias(self.ERROR_COLUMN)
                if schema_error_exprs else pl.lit([]).cast(pl.List(pl.String)).alias(self.ERROR_COLUMN),
                pl.concat_list(schema_category_exprs).list.drop_nulls().alias(self.CATEGORY_COLUMN)
                if schema_category_exprs else pl.lit([]).cast(pl.List(pl.String)).alias(self.CATEGORY_COLUMN)
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

        # 4. Apply heavy Transformations to Good Data (Derive, Lookup)
        if self.contract.transformations:
            good_lf = self._apply_post_transformations(good_lf, ctx)

        include_errors = True
        if self.contract.quarantine:
            include_errors = self.contract.quarantine.include_error_reason

        if not include_errors:
            bad_lf = bad_lf.drop([self.ERROR_COLUMN, self.CATEGORY_COLUMN])

        return good_lf.collect(), bad_lf.drop(internal_cols).collect()

    def _run_dataset_rules(self, lf: pl.LazyFrame, ctx: pl.SQLContext):
        rules = self.get_dataset_rules()
        if not rules:
            return

        tbl_name = self.contract.dataset or "source"
        ctx.register(tbl_name, lf)

        for rule in rules:
            try:
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
                self.dataset_rule_results.append({
                    "name": rule.name,
                    "value": val,
                    "passed": passed,
                    "description": rule.description
                })
            except Exception as e:
                logger.error(f"Error executing dataset rule '{rule.name}': {e}")

    def _apply_pre_transformations(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Apply filters, renames, and deduplication before schema/rules."""
        current_lf = lf
        existing = set(current_lf.columns)
        for trans in self.contract.transformations:
            if trans.rename:
                if trans.rename.from_name not in existing:
                    logger.warning(f"Pre-Transform [Rename] skipped; column not found: {trans.rename.from_name}")
                    continue
                logger.debug(f"Pre-Transform [Rename]: {trans.rename.from_name} -> {trans.rename.to_name}")
                current_lf = current_lf.rename({trans.rename.from_name: trans.rename.to_name})
                existing.remove(trans.rename.from_name)
                existing.add(trans.rename.to_name)
            else:
                filter_cfg = getattr(trans, "filter", None)
                dedupe_cfg = getattr(trans, "deduplicate", None)
                if filter_cfg:
                    logger.debug(f"Pre-Transform [Filter]: {filter_cfg.sql}")
                    try:
                        ctx = pl.SQLContext()
                        ctx.register("source", current_lf)
                        current_lf = ctx.execute(f"SELECT * FROM source WHERE {filter_cfg.sql}")
                    except Exception as e:
                        logger.warning(f"Pre-Transform [Filter] failed: {e}")
                elif dedupe_cfg:
                    logger.debug(f"Pre-Transform [Deduplicate]: {dedupe_cfg.on}")
                    if dedupe_cfg.sort_by:
                        current_lf = current_lf.sort(dedupe_cfg.sort_by, descending=(dedupe_cfg.order == "desc"))
                    current_lf = current_lf.unique(subset=dedupe_cfg.on, maintain_order=True)
        return current_lf

    def _apply_post_transformations(self, lf: pl.LazyFrame, ctx: pl.SQLContext) -> pl.LazyFrame:
        """Apply derive, lookup, and any remaining transforms."""
        current_lf = lf
        tbl_name = self.contract.dataset or "source"
        
        for trans in self.contract.transformations:
            # Re-register for each step
            ctx.register(tbl_name, current_lf)
            
            if trans.derive:
                logger.debug(f"Post-Transform [Derive]: {trans.derive.field}")
                query = f"SELECT *, ({trans.derive.sql}) AS {trans.derive.field} FROM {tbl_name}"
                current_lf = ctx.execute(query)
            elif trans.lookup:
                logger.debug(f"Post-Transform [Lookup]: {trans.lookup.field} from {trans.lookup.reference}")
                query = f"""
                SELECT 
                    src.*,
                    ref.{trans.lookup.value} AS {trans.lookup.field}
                FROM {tbl_name} src
                LEFT JOIN {trans.lookup.reference} ref ON src.{trans.lookup.on} = ref.{trans.lookup.key}
                """
                current_lf = ctx.execute(query)
            else:
                filter_cfg = getattr(trans, "filter", None)
                if filter_cfg:
                    logger.debug(f"Post-Transform [Filter]: {filter_cfg.sql}")
                    query = f"SELECT * FROM {tbl_name} WHERE {filter_cfg.sql}"
                    current_lf = ctx.execute(query)
                
        return current_lf
