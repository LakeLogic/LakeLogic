import polars as pl
from typing import Tuple, Any, List, Dict
from lakelogic.engines.base import EngineAdapter
from loguru import logger
from pathlib import Path

class PolarsAdapter(EngineAdapter):
    """
    Polars execution engine for LakeLogic.
    Supports row-level validation, aggregate metrics, and SQL-first transformations.
    """
    _link_cache: Dict[str, pl.LazyFrame] = {}

    def _get_context(self, source_lf: pl.LazyFrame) -> pl.SQLContext:
        """
        Creates a SQLContext with the source and all linked dependencies registered.

        Args:
            source_lf: Source LazyFrame.

        Returns:
            SQLContext with registered tables.
        """
        ctx = pl.SQLContext()
        ctx.register(self.contract.dataset or "source", source_lf)
        self._register_links(ctx)
        
        return ctx

    def _register_links(self, ctx: pl.SQLContext) -> None:
        """
        Register linked reference datasets into a SQLContext.

        Args:
            ctx: Polars SQLContext.
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

                cache_enabled = False
                try:
                    cache_enabled = bool(self.contract.metadata.get("cache_reference_links"))
                except Exception:
                    cache_enabled = False
                cache_key = f"{link.name}:{path}"
                if cache_enabled and cache_key in self._link_cache:
                    ctx.register(link.name, self._link_cache[cache_key])
                    continue

                if path.suffix.lower() == ".parquet":
                    link_lf = pl.read_parquet(path).lazy()
                elif path.suffix.lower() == ".csv":
                    link_lf = pl.read_csv(path).lazy()
                else:
                    logger.warning(f"Unsupported link format for {link.name}: {path.suffix}")
                    continue

                if cache_enabled:
                    self._link_cache[cache_key] = link_lf
                ctx.register(link.name, link_lf)
            except Exception as e:
                logger.warning(f"Could not register link {link.name}: {e}")

    def _apply_sql_transformation(self, lf: pl.LazyFrame, sql: str) -> pl.LazyFrame:
        """
        Execute a SQL transformation against the current LazyFrame.

        Args:
            lf: Current LazyFrame.
            sql: SQL query to execute.

        Returns:
            The transformed LazyFrame.
        """
        ctx = pl.SQLContext()
        ctx.register("source", lf)
        if self.contract.dataset:
            ctx.register(self.contract.dataset, lf)
        self._register_links(ctx)
        try:
            return ctx.execute(sql)
        except Exception as exc:
            try:
                import duckdb
            except Exception:
                raise exc
            logger.warning(f"Polars SQL failed; falling back to DuckDB for SQL transform: {exc}")
            con = duckdb.connect(database=":memory:")
            df = lf.collect()
            con.register("source", df)
            if self.contract.dataset:
                con.register(self.contract.dataset, df)
            for link in self.contract.links:
                try:
                    if link.table or (link.type and link.type.lower() == "table"):
                        continue
                    if not link.path:
                        continue
                    if link.path.startswith(("s3://", "gs://", "abfss://", "adl://", "https://")):
                        continue
                    path = Path(link.path)
                    if not path.is_absolute() and hasattr(self.contract, "_base_path"):
                        path = Path(self.contract._base_path) / path
                    if not path.exists():
                        continue
                    if path.suffix.lower() == ".parquet":
                        con.execute(f"CREATE OR REPLACE VIEW {link.name} AS SELECT * FROM read_parquet('{path.as_posix()}')")
                    elif path.suffix.lower() == ".csv":
                        con.execute(f"CREATE OR REPLACE VIEW {link.name} AS SELECT * FROM read_csv_auto('{path.as_posix()}')")
                except Exception:
                    continue
            rel = con.query(sql)
            out = pl.from_pandas(rel.df()).lazy()
            con.close()
            return out

    def _to_polars_dtype(self, type_name: str):
        """
        Map contract type names to Polars dtypes.

        Args:
            type_name: Logical type name from contract.

        Returns:
            Polars dtype or None.
        """
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
        """
        Apply schema casts, missing columns, and unknown field handling.

        Args:
            lf: Input LazyFrame.

        Returns:
            Tuple of (LazyFrame, schema_errors).
        """
        if not self.contract.model or not self.contract.model.fields:
            if self.contract.server and self.contract.server.mode == "ingest" and self.contract.server.cast_to_string:
                columns = lf.collect_schema().names()
                lf = lf.with_columns([pl.col(col).cast(pl.Utf8, strict=False) for col in columns])
            return lf, []

        expected_fields = [f.name for f in self.contract.model.fields]
        existing_cols = lf.collect_schema().names()
        existing = set(existing_cols)
        expected = set(expected_fields)

        missing = expected - existing
        unknown = existing - expected
        
        # Exclude transient and lineage columns from unknown field assessment
        transient_cols = {"rn", "__index_level_0__", "_row_number"}
        unknown = unknown - transient_cols - self._lineage_columns()

        for col in missing:
            lf = lf.with_columns(pl.lit(None).alias(col))

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

        if cast_to_string:
            columns = lf.collect_schema().names()
            lf = lf.with_columns([pl.col(col).cast(pl.Utf8, strict=False) for col in columns])
        else:
            for field in self.contract.model.fields:
                dtype = self._to_polars_dtype(field.type)
                if dtype is not None:
                    lf = lf.with_columns(pl.col(field.name).cast(dtype, strict=False))

        schema_errors: List[str] = []
        if evolution == "strict" and missing:
            schema_errors.append(f"Missing fields: {', '.join(sorted(missing))}")

        if policy == "drop" and unknown:
            lf = lf.drop(list(unknown))
        elif policy == "quarantine" and unknown:
            schema_errors.append(f"Unknown fields present: {', '.join(sorted(unknown))}")

        self.schema_drift = {
            "missing_fields": sorted(missing),
            "unknown_fields": sorted(unknown),
            "policy": policy,
            "evolution": evolution or "",
            "allow_schema_drift": allow_schema_drift,
        }

        return lf, schema_errors

    def execute(self, df: Any) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """
        Execute the contract on a Polars dataframe.

        Args:
            df: Input dataframe (Polars/Pandas/compatible).

        Returns:
            Tuple of (good_df, bad_df).
        """
        self.dataset_rule_results = []
        self.schema_drift = {}
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
        """
        Execute dataset-level quality rules.

        Args:
            lf: LazyFrame of good records.
            ctx: SQLContext for query execution.
        """
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
                if val is None:
                    passed = False
                elif rule.must_be_between:
                    passed = rule.must_be_between[0] <= val <= rule.must_be_between[1]
                elif rule.must_be_less_than is not None:
                    passed = val < rule.must_be_less_than
                elif rule.must_be_greater_than is not None:
                    passed = val > rule.must_be_greater_than
                
                status = "PASS" if passed else "FAIL"
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
        """
        Apply filters, renames, and deduplication before schema/rules.

        Args:
            lf: Input LazyFrame.

        Returns:
            Transformed LazyFrame.
        """
        current_lf = lf
        existing = set(current_lf.collect_schema().names())
        for trans in self.contract.transformations:
            if trans.sql and (trans.phase or "post").lower() == "pre":
                logger.debug(f"Pre-Transform [SQL]: {trans.sql}")
                try:
                    current_lf = self._apply_sql_transformation(current_lf, trans.sql)
                    existing = set(current_lf.collect_schema().names())
                except Exception as e:
                    logger.warning(f"Pre-Transform [SQL] failed: {e}")
                continue

            if trans.rename:
                rename_pairs = trans.rename.iter_pairs()
                if not rename_pairs:
                    continue
                for from_col, to_col in rename_pairs:
                    if from_col not in existing:
                        logger.warning(f"Pre-Transform [Rename] skipped; column not found: {from_col}")
                        continue
                    logger.debug(f"Pre-Transform [Rename]: {from_col} -> {to_col}")
                    current_lf = current_lf.rename({from_col: to_col})
                    existing.remove(from_col)
                    existing.add(to_col)
            elif trans.select:
                logger.debug(f"Pre-Transform [Select]: {trans.select.columns}")
                current_lf = current_lf.select(trans.select.columns)
                existing = set(current_lf.collect_schema().names())
            elif trans.drop:
                logger.debug(f"Pre-Transform [Drop]: {trans.drop.columns}")
                current_lf = current_lf.drop(trans.drop.columns)
                existing = set(current_lf.collect_schema().names())
            elif trans.cast:
                logger.debug(f"Pre-Transform [Cast]: {list(trans.cast.columns.keys())}")
                exprs = []
                for col, dtype_name in trans.cast.columns.items():
                    if col not in existing:
                        continue
                    dtype = self._to_polars_dtype(dtype_name) or pl.Utf8
                    exprs.append(pl.col(col).cast(dtype, strict=False).alias(col))
                if exprs:
                    current_lf = current_lf.with_columns(exprs)
                    existing = set(current_lf.collect_schema().names())
            elif trans.trim:
                logger.debug(f"Pre-Transform [Trim]: {trans.trim.fields}")
                exprs = []
                for col in trans.trim.fields:
                    if col not in existing:
                        continue
                    if trans.trim.side == "left":
                        exprs.append(pl.col(col).str.strip_chars_start().alias(col))
                    elif trans.trim.side == "right":
                        exprs.append(pl.col(col).str.strip_chars_end().alias(col))
                    else:
                        exprs.append(pl.col(col).str.strip_chars().alias(col))
                if exprs:
                    current_lf = current_lf.with_columns(exprs)
            elif trans.lower:
                logger.debug(f"Pre-Transform [Lower]: {trans.lower.fields}")
                exprs = [pl.col(col).str.to_lowercase().alias(col) for col in trans.lower.fields if col in existing]
                if exprs:
                    current_lf = current_lf.with_columns(exprs)
            elif trans.upper:
                logger.debug(f"Pre-Transform [Upper]: {trans.upper.fields}")
                exprs = [pl.col(col).str.to_uppercase().alias(col) for col in trans.upper.fields if col in existing]
                if exprs:
                    current_lf = current_lf.with_columns(exprs)
            elif trans.coalesce:
                sources = trans.coalesce.sources or []
                if not sources:
                    sources = [trans.coalesce.field]
                exprs = [pl.col(col) for col in sources if col in existing]
                if trans.coalesce.default is not None:
                    exprs.append(pl.lit(trans.coalesce.default))
                if exprs:
                    output = trans.coalesce.output or trans.coalesce.field
                    logger.debug(f"Pre-Transform [Coalesce]: {output}")
                    current_lf = current_lf.with_columns(pl.coalesce(exprs).alias(output))
                    existing = set(current_lf.collect_schema().names())
            elif trans.split:
                output = trans.split.output or trans.split.field
                if trans.split.field in existing:
                    logger.debug(f"Pre-Transform [Split]: {trans.split.field} -> {output}")
                    current_lf = current_lf.with_columns(
                        pl.col(trans.split.field).str.split(trans.split.delimiter).alias(output)
                    )
                    existing = set(current_lf.collect_schema().names())
            elif trans.explode:
                output = trans.explode.output or trans.explode.field
                if trans.explode.field in existing:
                    logger.debug(f"Pre-Transform [Explode]: {trans.explode.field} -> {output}")
                    if output != trans.explode.field:
                        current_lf = current_lf.with_columns(pl.col(trans.explode.field).alias(output))
                    current_lf = current_lf.explode(output)
                    existing = set(current_lf.collect_schema().names())
            elif trans.map_values:
                field = trans.map_values.field
                if field in existing:
                    logger.debug(f"Pre-Transform [Map Values]: {field}")
                    expr = None
                    for key, value in trans.map_values.mapping.items():
                        cond = pl.col(field) == pl.lit(key)
                        expr = pl.when(cond).then(pl.lit(value)) if expr is None else expr.when(cond).then(pl.lit(value))
                    if expr is not None:
                        default_val = trans.map_values.default
                        expr = expr.otherwise(pl.lit(default_val) if default_val is not None else pl.col(field))
                        output = trans.map_values.output or field
                        current_lf = current_lf.with_columns(expr.alias(output))
                        existing = set(current_lf.collect_schema().names())
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
        """
        Apply derive, lookup, and any remaining transforms.

        Args:
            lf: Input LazyFrame.
            ctx: SQLContext for SQL execution.

        Returns:
            Transformed LazyFrame.
        """
        current_lf = lf
        tbl_name = self.contract.dataset or "source"
        
        for trans in self.contract.transformations:
            # Re-register for each step
            ctx.register(tbl_name, current_lf)
            
            if trans.sql and (trans.phase or "post").lower() != "pre":
                logger.debug(f"Post-Transform [SQL]: {trans.sql}")
                current_lf = self._apply_sql_transformation(current_lf, trans.sql)
                continue
            if trans.rollup and (trans.phase or "post").lower() != "pre":
                rollup_sql = self._build_rollup_sql(trans.rollup, source_table=tbl_name)
                logger.debug(f"Post-Transform [Rollup]: {rollup_sql}")
                current_lf = self._apply_sql_transformation(current_lf, rollup_sql)
                continue

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
            elif trans.join:
                logger.debug(f"Post-Transform [Join]: {trans.join.reference}")
                join_sql = self._build_join_sql(trans.join, tbl_name=tbl_name)
                current_lf = ctx.execute(join_sql)
            else:
                filter_cfg = getattr(trans, "filter", None)
                if filter_cfg:
                    logger.debug(f"Post-Transform [Filter]: {filter_cfg.sql}")
                    query = f"SELECT * FROM {tbl_name} WHERE {filter_cfg.sql}"
                    current_lf = ctx.execute(query)
                
        return current_lf

    def _format_sql_literal(self, value: Any) -> str:
        """
        Format a literal for SQL.

        Args:
            value: Python value.

        Returns:
            SQL literal.
        """
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        return "'" + str(value).replace("'", "''") + "'"

    def _build_join_sql(self, join_cfg, tbl_name: str = "source") -> str:
        """
        Build a SQL join query for enrichment.

        Args:
            join_cfg: Join configuration.
            tbl_name: Source table name.

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
                expr = f"COALESCE(ref.{field}, {self._format_sql_literal(default)}) AS {alias}"
            else:
                expr = f"ref.{field} AS {alias}"
            select_fields.append(expr)

        return f"""
        SELECT {', '.join(select_fields)}
        FROM {tbl_name} src
        {join_type} JOIN {join_cfg.reference} ref ON src.{join_cfg.on} = ref.{join_cfg.key}
        """
