from pathlib import Path
from typing import Any, List, Tuple

from loguru import logger

from lakelogic.engines.base import EngineAdapter


class SparkAdapter(EngineAdapter):
    """
    Spark execution engine for LakeLogic.
    Uses Spark SQL and Column Expressions for evaluation.
    """

    engine_name: str = "spark"

    def _quote_ident(self, name: str) -> str:
        """
        Quote an identifier for Spark SQL (backticks).
        """
        text = str(name)
        if text.startswith("`") and text.endswith("`"):
            return text
        return f"`{text.replace('`', '``')}`"

    def _regex_sql(self, field: str, pattern: str) -> str:
        """Spark SQL regex predicate — Spark has no ``REGEXP_MATCHES``; use RLIKE."""
        esc = str(pattern).replace("'", "''")
        return f"{self._quote_ident(field)} RLIKE '{esc}'"

    def execute(self, df: Any) -> Tuple[Any, Any]:
        """
        Executes the contract using PySpark.

        Args:
            df: Spark DataFrame to validate.

        Returns:
            Tuple of (good_df, bad_df).
        """
        self.dataset_rule_results = []
        self.schema_drift = {}
        try:
            from pyspark.sql import DataFrame
            from pyspark.sql import functions as F
        except ImportError:  # pragma: no cover - defensive: pyspark is a soft dep
            raise ImportError("pyspark is required for SparkAdapter")

        # Databricks Serverless / Spark Connect returns a different DataFrame class.
        # Build a tuple of all available DataFrame types so isinstance works on both
        # classic and Serverless runtimes.
        _df_types = [DataFrame]
        try:  # pragma: no cover - Spark Connect only present on Databricks Serverless
            from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

            _df_types.append(ConnectDataFrame)
        except ImportError:
            pass
        _df_types = tuple(_df_types)

        # Auto-convert a Python list/tuple of dicts to a Spark DataFrame.
        # This lets callers pass in-memory records (e.g. rows fetched from SQLite,
        # a list of dicts, Pandas-style .to_dict("records")) without having to
        # manually create a SparkSession first.
        if isinstance(df, (list, tuple)):  # pragma: no cover - requires live SparkSession
            from pyspark.sql import SparkSession

            _spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
            df = _spark.createDataFrame(df)

        # Auto-convert Polars DataFrames to Spark DataFrames via Pandas
        elif (
            type(df).__name__ == "DataFrame" and "polars" in type(df).__module__
        ):  # pragma: no cover - requires Spark+Polars+Pandas
            from pyspark.sql import SparkSession

            _spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
            df = _spark.createDataFrame(df.to_pandas())

        elif not isinstance(df, _df_types):
            raise TypeError(f"Expected Spark DataFrame, got {type(df)}")

        # 1. Register Source and Links in Spark Session
        spark = df.sparkSession
        self._temp_src = f"lakelogic_src_{id(self)}"
        tbl_name = self.contract.dataset or self._temp_src
        df.createOrReplaceTempView(tbl_name)
        df.createOrReplaceTempView(self._temp_src)
        self._register_links(spark)

        # 0. Apply pre-processing (renames, filters, deduplication)
        df = self._apply_pre_transformations(df)

        # 0.5 Apply schema enforcement
        df, schema_errors = self._apply_schema(df)
        df.createOrReplaceTempView(tbl_name)

        # 2. Evaluate Row-Level Rules (pre-phase — before transforms)
        row_rules = self.get_row_rules()
        pre_rules = [r for r in row_rules if getattr(r, "phase", "pre").lower() != "post"]
        post_rules = [r for r in row_rules if getattr(r, "phase", "pre").lower() == "post"]

        error_exprs = []
        category_exprs = []

        if schema_errors:
            error_exprs.extend([F.lit(err) for err in schema_errors])
            category_exprs.extend([F.lit("schema") for _ in schema_errors])

        for type_err_col in getattr(self, "_type_err_cols", []):
            error_exprs.append(F.col(type_err_col))
            category_exprs.append(F.when(F.col(type_err_col).isNotNull(), F.lit("schema")).otherwise(F.lit(None)))

        df_eval = df
        internal_cols = []
        if pre_rules:
            rule_exprs = ["*"]
            for i, rule in enumerate(pre_rules):
                rule_exprs.append(f"CAST(({rule.sql}) AS BOOLEAN) as _rule_{i}")
                internal_cols.append(f"_rule_{i}")

            df_eval = df.selectExpr(*rule_exprs)

            for i, rule in enumerate(pre_rules):
                col_name = f"_rule_{i}"
                error_msg = f"[pre] Rule failed: {rule.name} ({rule.sql})"
                cond = F.col(col_name).isNull() | ~F.col(col_name)
                error_exprs.append(F.when(cond, F.lit(error_msg)).otherwise(None))
                category_exprs.append(F.when(cond, F.lit(rule.category)).otherwise(None))

        error_array = F.array(*error_exprs) if error_exprs else F.array().cast("array<string>")
        category_array = F.array(*category_exprs) if category_exprs else F.array().cast("array<string>")

        # Ensure arrays are non-null to avoid NULL comparisons dropping all rows.
        # Use SQL expression instead of Python lambda to avoid UDF worker crash on Windows.
        df_with_errors = df_eval.withColumn(self.ERROR_COLUMN, error_array).withColumn(
            self.CATEGORY_COLUMN, category_array
        )
        df_with_errors = df_with_errors.withColumn(
            self.ERROR_COLUMN,
            F.expr(f"filter({self.ERROR_COLUMN}, x -> x IS NOT NULL)"),
        ).withColumn(
            self.CATEGORY_COLUMN,
            F.expr(f"filter({self.CATEGORY_COLUMN}, x -> x IS NOT NULL)"),
        )

        # 3. Split Good and Bad
        has_errors = F.size(F.col(self.ERROR_COLUMN)) > 0

        bad_df = (
            df_with_errors.filter(has_errors)
            .withColumn("quarantine_state", F.lit("active"))
            .withColumn("quarantine_reprocessed", F.lit(False))
        )

        type_errs = getattr(self, "_type_err_cols", [])
        drop_cols = [self.ERROR_COLUMN, self.CATEGORY_COLUMN] + internal_cols + type_errs
        good_df = df_with_errors.filter(~has_errors).drop(*drop_cols)
        bad_df = bad_df.drop(*internal_cols).drop(*type_errs)

        # 4. Apply Dataset-Level (Aggregate) Checks
        self._run_dataset_rules(good_df)

        # 5. Apply Transformations to Good Data
        if self.contract.transformations:
            good_df = self._apply_post_transformations(good_df)

        # 6. Evaluate post-phase rules (after transforms — can reference derived columns)
        if post_rules:
            post_rule_exprs = ["*"]
            post_internal = []
            for i, rule in enumerate(post_rules):
                col_id = f"_post_rule_{i}"
                post_rule_exprs.append(f"CAST(({rule.sql}) AS BOOLEAN) as {col_id}")
                post_internal.append(col_id)

            good_eval = good_df.selectExpr(*post_rule_exprs)

            post_error_exprs = []
            post_cat_exprs = []
            for i, rule in enumerate(post_rules):
                col_id = f"_post_rule_{i}"
                error_msg = f"[post] Rule failed: {rule.name} ({rule.sql})"
                cond = F.col(col_id).isNull() | ~F.col(col_id)
                post_error_exprs.append(F.when(cond, F.lit(error_msg)).otherwise(None))
                post_cat_exprs.append(F.when(cond, F.lit(rule.category)).otherwise(None))

            post_err_array = F.array(*post_error_exprs)
            post_cat_array = F.array(*post_cat_exprs)
            good_eval = good_eval.withColumn(self.ERROR_COLUMN, post_err_array).withColumn(
                self.CATEGORY_COLUMN, post_cat_array
            )
            good_eval = good_eval.withColumn(
                self.ERROR_COLUMN,
                F.expr(f"filter({self.ERROR_COLUMN}, x -> x IS NOT NULL)"),
            ).withColumn(
                self.CATEGORY_COLUMN,
                F.expr(f"filter({self.CATEGORY_COLUMN}, x -> x IS NOT NULL)"),
            )

            post_has_errors = F.size(F.col(self.ERROR_COLUMN)) > 0

            post_bad = (
                good_eval.filter(post_has_errors)
                .withColumn("quarantine_state", F.lit("active"))
                .withColumn("quarantine_reprocessed", F.lit(False))
            )
            # Union new bad rows with existing bad_df
            if bad_df is not None:
                # Use native unionByName which natively handles missing columns in Spark 3.1+
                # This avoids building a deeply nested Catalyst projection plan.
                bad_df = bad_df.unionByName(post_bad, allowMissingColumns=True)
            else:
                bad_df = post_bad

            good_df = good_eval.filter(~post_has_errors).drop(
                *([self.ERROR_COLUMN, self.CATEGORY_COLUMN] + post_internal)
            )

        include_errors = True
        if self.contract.quarantine:
            include_errors = self.contract.quarantine.include_error_reason

        if not include_errors:
            bad_df = bad_df.drop(self.ERROR_COLUMN, self.CATEGORY_COLUMN)

        return good_df, bad_df

    def _run_dataset_rules(self, df: Any):
        """
        Execute dataset-level quality rules.

        Args:
            df: Spark DataFrame of good records.
        """
        rules = self.get_dataset_rules()
        if not rules:
            return

        tbl_name = self.contract.dataset or getattr(self, "_temp_src", "source")
        df.createOrReplaceTempView(tbl_name)
        spark = df.sparkSession

        import re

        for rule in rules:
            try:
                sql_str = re.sub(r"\bsource\b", tbl_name, rule.sql, flags=re.IGNORECASE)
                res = spark.sql(sql_str).collect()
                val = res[0][0]

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
                logger.info(f"Quality Check (Spark): {rule.name} | Result: {val} {expected} | Status: {status}")
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

    def _apply_pre_transformations(self, df: Any) -> Any:
        """
        Apply pre-processing transformations (rename, filter, deduplicate, and cleanup helpers).

        Args:
            df: Spark DataFrame.

        Returns:
            Transformed Spark DataFrame.
        """
        from pyspark.sql import Window
        from pyspark.sql import functions as F
        from pyspark.sql import types as T

        current_df = df
        existing = set(current_df.columns)
        for trans in self.contract.transformations:
            trans_phase = (trans.phase or "post").lower()

            # ── Execute Transformation ────────────────────────────────────────
            if trans.sql and trans_phase == "pre":
                logger.debug(f"Pre-Transform [SQL]: {trans.sql}")
                current_df.createOrReplaceTempView(self._temp_src)
                import re

                sql_str = re.sub(r"\bsource\b", self._temp_src, trans.sql, flags=re.IGNORECASE)
                for link in self.contract.links:
                    sql_str = re.sub(
                        rf"\b{re.escape(link.name)}\b", f"{link.name}_{id(self)}", sql_str, flags=re.IGNORECASE
                    )
                current_df = current_df.sparkSession.sql(sql_str)
                existing = set(current_df.columns)

            elif trans.derive and trans_phase == "pre":
                logger.debug(f"Pre-Transform [Derive]: {trans.derive.field}")
                _derive_expr = self._transpile_derive_sql(trans.derive)
                current_df = current_df.withColumn(trans.derive.field, F.expr(_derive_expr))
                existing = set(current_df.columns)

            elif trans.pivot and trans_phase == "pre":
                pivot_sql = self._build_pivot_sql(trans.pivot, source_table=self.contract.dataset or self._temp_src)
                if pivot_sql:
                    logger.debug(f"Pre-Transform [Pivot]: {pivot_sql}")
                    current_df.createOrReplaceTempView(self._temp_src)
                    if self.contract.dataset:
                        current_df.createOrReplaceTempView(self.contract.dataset)
                    current_df = current_df.sparkSession.sql(pivot_sql)
                    existing = set(current_df.columns)
                continue

            if trans.unpivot and trans_phase == "pre":
                unpivot_sql = self._build_unpivot_sql(
                    trans.unpivot, source_table=self.contract.dataset or self._temp_src
                )
                if unpivot_sql:
                    logger.debug(f"Pre-Transform [Unpivot]: {unpivot_sql}")
                    current_df.createOrReplaceTempView(self._temp_src)
                    if self.contract.dataset:
                        current_df.createOrReplaceTempView(self.contract.dataset)
                    current_df = current_df.sparkSession.sql(unpivot_sql)
                    existing = set(current_df.columns)
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
                    current_df = current_df.withColumnRenamed(from_col, to_col)
                    existing.remove(from_col)
                    existing.add(to_col)
            elif trans.select:
                logger.debug(f"Pre-Transform [Select]: {trans.select.columns}")
                current_df = current_df.select(*trans.select.columns)
                existing = set(current_df.columns)
            elif trans.drop:
                logger.debug(f"Pre-Transform [Drop]: {trans.drop.columns}")
                current_df = current_df.drop(*trans.drop.columns)
                existing = set(current_df.columns)
            elif trans.cast:
                logger.debug(f"Pre-Transform [Cast]: {list(trans.cast.columns.keys())}")
                for col, dtype_name in trans.cast.columns.items():
                    if col not in current_df.columns:
                        continue
                    spark_type = self._to_spark_type(dtype_name) or dtype_name
                    current_df = current_df.withColumn(col, F.col(col).cast(spark_type))
                existing = set(current_df.columns)
            elif trans.trim:
                logger.debug(f"Pre-Transform [Trim]: {trans.trim.fields}")
                for col in trans.trim.fields:
                    if col not in current_df.columns:
                        continue
                    if trans.trim.side == "left":
                        current_df = current_df.withColumn(col, F.ltrim(F.col(col)))
                    elif trans.trim.side == "right":
                        current_df = current_df.withColumn(col, F.rtrim(F.col(col)))
                    else:
                        current_df = current_df.withColumn(col, F.trim(F.col(col)))
                existing = set(current_df.columns)
            elif trans.lower:
                logger.debug(f"Pre-Transform [Lower]: {trans.lower.fields}")
                for col in trans.lower.fields:
                    if col not in current_df.columns:
                        continue
                    current_df = current_df.withColumn(col, F.lower(F.col(col)))
                existing = set(current_df.columns)
            elif trans.upper:
                logger.debug(f"Pre-Transform [Upper]: {trans.upper.fields}")
                for col in trans.upper.fields:
                    if col not in current_df.columns:
                        continue
                    current_df = current_df.withColumn(col, F.upper(F.col(col)))
                existing = set(current_df.columns)
            elif trans.coalesce:
                sources = trans.coalesce.sources or []
                if not sources:
                    sources = [trans.coalesce.field]
                exprs = [F.col(col) for col in sources if col in current_df.columns]
                if trans.coalesce.default is not None:
                    exprs.append(F.lit(trans.coalesce.default))
                if exprs:
                    output = trans.coalesce.output or trans.coalesce.field
                    logger.debug(f"Pre-Transform [Coalesce]: {output}")
                    current_df = current_df.withColumn(output, F.coalesce(*exprs))
                    existing = set(current_df.columns)
            elif trans.split:
                output = trans.split.output or trans.split.field
                if trans.split.field in current_df.columns:
                    logger.debug(f"Pre-Transform [Split]: {trans.split.field} -> {output}")
                    # to_json gives a compact JSON array string, matching the DuckDB
                    # (to_json) and Polars (compact json.dumps) engines.
                    current_df = current_df.withColumn(
                        output, F.to_json(F.split(F.col(trans.split.field), trans.split.delimiter))
                    )
                    existing = set(current_df.columns)
            elif trans.explode:
                output = trans.explode.output or trans.explode.field
                if trans.explode.field in current_df.columns:
                    logger.debug(f"Pre-Transform [Explode]: {trans.explode.field} -> {output}")
                    current_df = current_df.withColumn(output, F.explode(F.col(trans.explode.field)))
                    existing = set(current_df.columns)
            elif trans.map_values:
                field = trans.map_values.field
                mapping = trans.map_values.mapping or {}
                if field in current_df.columns and mapping:
                    logger.debug(f"Pre-Transform [Map Values]: {field}")
                    expr = None
                    for key, value in mapping.items():
                        cond = F.col(field) == F.lit(key)
                        expr = F.when(cond, F.lit(value)) if expr is None else expr.when(cond, F.lit(value))
                    if expr is not None:
                        default_val = trans.map_values.default
                        expr = expr.otherwise(F.lit(default_val) if default_val is not None else F.col(field))
                        output = trans.map_values.output or field
                        current_df = current_df.withColumn(output, expr)
                        existing = set(current_df.columns)
            elif trans.filter:
                logger.debug(f"Pre-Transform [Filter]: {trans.filter.sql}")
                current_df = current_df.filter(trans.filter.sql)
            elif trans.deduplicate or getattr(trans, "deduplicate_by_latest", None):
                dd = self._resolve_deduplicate(trans)  # handles `deduplicate` + `deduplicate_by_latest`
                if dd:
                    logger.debug(f"Pre-Transform [Deduplicate]: {dd.on}")
                    # Nested/binary columns are not orderable in Spark, so they sit
                    # out the tie-break; the scalar columns still make it stable.
                    try:
                        _sortable = [
                            f.name
                            for f in current_df.schema.fields
                            if not isinstance(f.dataType, (T.ArrayType, T.StructType, T.MapType, T.BinaryType))
                        ]
                    except AttributeError:
                        _sortable = list(getattr(current_df, "columns", []) or [])
                    sort_by, order = self._dedup_order(dd, _sortable)
                    if sort_by:
                        w = Window.partitionBy(*dd.on)
                        order_cols = [F.col(col).desc() if order == "desc" else F.col(col).asc() for col in sort_by]
                        w = w.orderBy(*order_cols)
                        current_df = (
                            current_df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
                        )
                    else:
                        current_df = current_df.dropDuplicates(dd.on)

            # ── Post-Step Sync ──────────────────────────────────────────────
            # Re-register the updated DataFrame as 'source' so that the NEXT
            # transformation phase (especially SQL-based ones) sees the new columns.
            current_df.createOrReplaceTempView(self._temp_src)
            if self.contract.dataset:
                current_df.createOrReplaceTempView(self.contract.dataset)

        return current_df

    # Contract type → Spark SQL cast type
    _CONTRACT_TO_SPARK_TYPE = {
        "integer": "INT",
        "long": "BIGINT",
        "double": "DOUBLE",
        "float": "FLOAT",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "decimal": "DECIMAL(38,10)",
        "short": "SMALLINT",
        "byte": "TINYINT",
        "binary": "BINARY",
    }

    def _cast_to_contract_types(self, df: Any) -> Any:
        """Cast string columns to their contract-declared types.

        When the source is CSV (all columns arrive as StringType), Spark SQL
        functions like ``timestamp_micros()`` fail because they expect a
        typed input (e.g. LongType) rather than a string.

        This method inspects the DataFrame schema: if **every** column is
        ``StringType``, it casts each column whose contract type has a known
        Spark mapping (integer → INT, long → BIGINT, etc.).

        For typed sources (Parquet, Delta, JSON-with-schema) some columns
        will already be non-string, so this method becomes a no-op — existing
        typed DataFrames are never modified.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        if not self.contract.model or not self.contract.model.fields:
            return df

        # Quick check: is this an all-strings DataFrame?
        all_string = all(isinstance(f.dataType, StringType) for f in df.schema.fields)
        if not all_string:
            return df

        # Build a lookup: column_name → contract type
        field_types = {f.name: (f.type or "string").lower() for f in self.contract.model.fields}

        cast_count = 0
        current_df = df
        for col_name in current_df.columns:
            contract_type = field_types.get(col_name, "string")
            spark_type = self._CONTRACT_TO_SPARK_TYPE.get(contract_type)
            if spark_type:
                current_df = current_df.withColumn(col_name, F.col(col_name).cast(spark_type))
                cast_count += 1

        if cast_count:
            logger.debug(f"Auto-cast {cast_count} columns from StringType to contract types")

        return current_df

    def _apply_post_transformations(self, df: Any) -> Any:
        """
        Apply post-processing transformations (derive, lookup, join, SQL).

        Args:
            df: Spark DataFrame.

        Returns:
            Transformed Spark DataFrame.
        """
        # Auto-cast string columns to contract types before transforms
        current_df = self._cast_to_contract_types(df)

        for trans in self.contract.transformations:
            trans_phase = (trans.phase or "post").lower()
            if trans.sql and trans_phase != "pre":
                logger.debug(f"Post-Transform [SQL]: {trans.sql}")
                current_df.createOrReplaceTempView(self._temp_src)
                if self.contract.dataset:
                    current_df.createOrReplaceTempView(self.contract.dataset)
                import re

                sql_str = re.sub(r"\bsource\b", self._temp_src, trans.sql, flags=re.IGNORECASE)
                for link in self.contract.links:
                    sql_str = re.sub(
                        rf"\b{re.escape(link.name)}\b", f"{link.name}_{id(self)}", sql_str, flags=re.IGNORECASE
                    )
                current_df = current_df.sparkSession.sql(sql_str)
                continue
            if trans.rollup and trans_phase != "pre":
                rollup_sql = self._build_rollup_sql(trans.rollup, source_table=self.contract.dataset or self._temp_src)
                logger.debug(f"Post-Transform [Rollup]: {rollup_sql}")
                current_df.createOrReplaceTempView(self._temp_src)
                if self.contract.dataset:
                    current_df.createOrReplaceTempView(self.contract.dataset)
                current_df = current_df.sparkSession.sql(rollup_sql)
                continue

            if trans.pivot and trans_phase != "pre":
                pivot_sql = self._build_pivot_sql(trans.pivot, source_table=self.contract.dataset or self._temp_src)
                if pivot_sql:
                    logger.debug(f"Post-Transform [Pivot]: {pivot_sql}")
                    current_df.createOrReplaceTempView(self._temp_src)
                    if self.contract.dataset:
                        current_df.createOrReplaceTempView(self.contract.dataset)
                    current_df = current_df.sparkSession.sql(pivot_sql)
                continue

            if trans.unpivot and trans_phase != "pre":
                unpivot_sql = self._build_unpivot_sql(
                    trans.unpivot, source_table=self.contract.dataset or self._temp_src
                )
                if unpivot_sql:
                    logger.debug(f"Post-Transform [Unpivot]: {unpivot_sql}")
                    current_df.createOrReplaceTempView(self._temp_src)
                    if self.contract.dataset:
                        current_df.createOrReplaceTempView(self.contract.dataset)
                    current_df = current_df.sparkSession.sql(unpivot_sql)
                continue

            if trans.derive:
                logger.debug(f"Post-Transform [Derive]: {trans.derive.field}")
                from pyspark.sql import functions as F

                _derive_expr = self._transpile_derive_sql(trans.derive)
                current_df = current_df.withColumn(trans.derive.field, F.expr(_derive_expr))
            elif trans.bucket:
                logger.debug(f"Post-Transform [Bucket]: {trans.bucket.field}")
                # For bucket, we use the existing _build_bucket_sql but extract the expression
                # Or just use the SQL approach if it's simpler, but Spark SQL doesn't have EXCLUDE.
                # Let's use withColumn + expr for safety.
                sql = self._build_bucket_sql(trans.bucket, source_table="temp_src")
                # Extract the CASE WHEN part from "SELECT *, (CASE...) AS field FROM temp_src"
                import re

                match = re.search(r"SELECT \*, \((.*)\) AS", sql, re.DOTALL)
                if match:
                    expr_str = match.group(1)
                    from pyspark.sql import functions as F

                    current_df = current_df.withColumn(trans.bucket.field, F.expr(expr_str))
            elif trans.date_diff:
                logger.debug(f"Post-Transform [DateDiff]: {trans.date_diff.field}")
                # Spark's DATEDIFF function is already handled in _build_date_diff_sql
                sql = self._build_date_diff_sql(trans.date_diff, source_table="temp_src")
                import re

                match = re.search(r"SELECT \*, \((.*)\) AS", sql, re.DOTALL)
                if match:
                    expr_str = match.group(1)
                    from pyspark.sql import functions as F

                    current_df = current_df.withColumn(trans.date_diff.field, F.expr(expr_str))
            elif trans.lookup:
                logger.debug(f"Post-Transform [Lookup]: {trans.lookup.field}")
                from pyspark.sql import functions as F

                ref_name = f"{trans.lookup.reference}_{id(self)}"
                ref_df = current_df.sparkSession.table(ref_name)
                if self._should_broadcast(trans.lookup.reference):
                    ref_df = F.broadcast(ref_df)

                # Alias DataFrames to avoid ambiguous columns
                src_df = current_df.alias("src")
                ref_df = ref_df.alias("ref")

                joined = src_df.join(
                    ref_df, F.col(f"src.{trans.lookup.on}") == F.col(f"ref.{trans.lookup.key}"), "left"
                )

                if trans.lookup.default_value is not None:
                    value_col = F.coalesce(F.col(f"ref.{trans.lookup.value}"), F.lit(trans.lookup.default_value))
                else:
                    value_col = F.col(f"ref.{trans.lookup.value}")

                current_df = joined.select("src.*", value_col.alias(trans.lookup.field))
            elif trans.join:
                logger.debug(f"Post-Transform [Join]: {trans.join.reference}")
                from pyspark.sql import functions as F

                ref_name = f"{trans.join.reference}_{id(self)}"
                ref_df = current_df.sparkSession.table(ref_name)
                if self._should_broadcast(trans.join.reference):
                    ref_df = F.broadcast(ref_df)

                src_df = current_df.alias("src")
                ref_df = ref_df.alias("ref")

                join_type = (trans.join.type or "left").lower()
                if join_type == "full":
                    join_type = "outer"

                joined = src_df.join(ref_df, F.col(f"src.{trans.join.on}") == F.col(f"ref.{trans.join.key}"), join_type)

                select_exprs = [F.col("src.*")]
                for field in trans.join.fields:
                    alias = f"{trans.join.prefix}{field}" if trans.join.prefix else field
                    default = trans.join.defaults.get(field) if trans.join.defaults else None
                    if default is not None:
                        col_expr = F.coalesce(F.col(f"ref.{field}"), F.lit(default))
                    else:
                        col_expr = F.col(f"ref.{field}")
                    select_exprs.append(col_expr.alias(alias))

                current_df = joined.select(*select_exprs)
            elif getattr(trans, "json_extract", None):
                cfg = trans.json_extract
                logger.debug(f"Post-Transform [JsonExtract]: {cfg.source} -> {cfg.field}")
                from pyspark.sql import functions as F

                extracted = F.get_json_object(F.col(cfg.source), cfg.path)
                if cfg.cast:
                    spark_type = {
                        "float": "double", "double": "double", "int": "int", "integer": "int",
                        "long": "bigint", "bool": "boolean", "boolean": "boolean", "string": "string",
                    }.get(str(cfg.cast).lower(), "string")
                    extracted = extracted.cast(spark_type)
                current_df = current_df.withColumn(cfg.field, extracted)
            elif getattr(trans, "date_range_explode", None):
                cfg = trans.date_range_explode
                logger.debug(f"Post-Transform [DateRangeExplode]: {cfg.start_col}..{cfg.end_col} => {cfg.output}")
                import re as _re

                from pyspark.sql import functions as F

                def _dp(c):  # tolerant of ISO + compact YYYYMMDD
                    iso = F.concat_ws(
                        "-", F.substring(F.col(c), 1, 4), F.substring(F.col(c), 5, 2), F.substring(F.col(c), 7, 2)
                    )
                    return F.to_date(F.when(F.col(c).rlike(r"^[0-9]{8}$"), iso).otherwise(F.col(c)))

                start_e = _dp(cfg.start_col)
                end_e = (
                    F.least(F.coalesce(_dp(cfg.end_col), F.current_date()), F.current_date())
                    if getattr(cfg, "end_col", None)
                    else F.current_date()
                )
                _intv = str(getattr(cfg, "interval", None) or "1d").strip().lower()
                _m = _re.match(r"(\d+)\s*([a-z]+)", _intv)
                _num = _m.group(1) if _m else "1"
                _unit = {"d": "day", "day": "day", "days": "day", "w": "week", "week": "week",
                         "mo": "month", "month": "month"}.get(_m.group(2) if _m else "d", "day")
                seq = F.sequence(start_e, end_e, F.expr(f"interval {_num} {_unit}"))
                current_df = current_df.withColumn(cfg.output, F.explode(seq))
            elif trans.filter and trans_phase != "pre":
                logger.debug(f"Post-Transform [Filter]: {trans.filter.sql}")
                current_df = current_df.filter(trans.filter.sql)
        return current_df

    def _should_broadcast(self, reference: str) -> bool:
        """
        Determine whether a lookup reference should be broadcast.

        Args:
            reference: Link name used in the transformation.

        Returns:
            True if the link is marked for broadcast.
        """
        for link in self.contract.links:
            if link.name == reference and getattr(link, "broadcast", False):
                return True
        return False

    def _register_links(self, spark) -> None:
        """
        Register linked reference datasets into Spark.

        Args:
            spark: SparkSession.
        """
        for link in self.contract.links:
            try:
                if link.table or (link.type and link.type.lower() == "table"):
                    table_name = link.table or (
                        link.path[6:] if link.path and link.path.startswith("table:") else link.path
                    )
                    if not table_name:
                        logger.warning(f"Link '{link.name}' missing table name.")
                        continue
                    ref_df = spark.table(table_name)
                    if link.columns and not (getattr(link, "filter", None) or getattr(link, "query", None)):
                        available = set(ref_df.columns)
                        select_cols = [c for c in link.columns if c in available]
                        if select_cols:
                            ref_df = ref_df.select(*select_cols)
                    safe_link_name = f"{link.name}_{id(self)}"
                    ref_df.createOrReplaceTempView(safe_link_name)
                    continue

                if not link.path:
                    continue

                is_remote = link.path.startswith(("s3://", "gs://", "abfss://", "adl://", "https://"))
                if is_remote:
                    load_path = link.path
                    link_type = (link.type or "delta").lower()
                else:
                    # Link paths are STORAGE references — resolved by the
                    # registry, not anchored on the contract YAML directory.
                    # Same rule as materialization.py / quarantine.py.
                    path = Path(link.path)
                    if not path.exists():
                        logger.warning(f"Link file not found: {path}")
                        continue
                    load_path = path.as_posix()
                    link_type = (link.type or "").lower()
                    if not link_type:  # pragma: no cover
                        if path.suffix.lower() == ".parquet":
                            link_type = "parquet"
                        elif path.suffix.lower() == ".csv":
                            link_type = "csv"
                        elif path.is_dir() and (path / "_delta_log").exists():
                            link_type = "delta"
                        else:
                            link_type = "parquet"

                if link_type == "delta":
                    ref_df = spark.read.format("delta").load(load_path)
                elif link_type == "parquet":
                    ref_df = spark.read.parquet(load_path)
                elif link_type == "csv":
                    ref_df = spark.read.option("header", "true").csv(load_path)
                else:
                    logger.warning(f"Unsupported or unknown link format for {link.name}: {link_type}")
                    continue

                # Column projection — only keep specified columns. Deferred to the
                # post-pass when filter/query is present (predicate may reference
                # not-yet-projected columns).
                if link.columns and not (getattr(link, "filter", None) or getattr(link, "query", None)):
                    available = set(ref_df.columns)
                    select_cols = [c for c in link.columns if c in available]
                    if select_cols:
                        ref_df = ref_df.select(*select_cols)
                        logger.debug(f"Link '{link.name}' projected to {len(select_cols)} columns")

                safe_link_name = f"{link.name}_{id(self)}"
                ref_df.createOrReplaceTempView(safe_link_name)
            except Exception as e:
                logger.warning(f"Could not register link {link.name}: {e}")

        # Post-pass: load-time row subsetting for links declaring a portable
        # `filter` (WHERE) or an engine-specific `query` escape hatch. Applies
        # after every registration branch. ``{link}`` in a query refers to the
        # just-registered dataset.
        for link in self.contract.links:
            _flt = getattr(link, "filter", None)
            _qry = getattr(link, "query", None)
            if not (_flt or _qry):
                continue
            safe_link_name = f"{link.name}_{id(self)}"
            try:
                _sql = (
                    _qry.replace("{link}", safe_link_name)
                    if _qry
                    else f"SELECT * FROM {safe_link_name} WHERE {_flt}"
                )
                _sub = spark.sql(_sql)
                # Apply the deferred column projection now (after the predicate).
                if link.columns:
                    _keep = [c for c in link.columns if c in set(_sub.columns)]
                    if _keep:
                        _sub = _sub.select(*_keep)
                _sub.createOrReplaceTempView(safe_link_name)
                logger.debug(f"Link '{link.name}' subset via {'query' if _qry else 'filter'}")
            except Exception as e:
                logger.warning(f"Could not apply link subset for '{link.name}': {e}")

    def _to_spark_type(self, type_name: str) -> str:
        """
        Map contract type names to Spark SQL types.

        Args:
            type_name: Logical type name from contract.

        Returns:
            Spark SQL type string.
        """
        type_name = (type_name or "").lower().strip()
        mapping = {
            "string": "string",
            "varchar": "string",
            "text": "string",
            "int": "long",
            "integer": "long",
            "long": "long",
            "bigint": "long",
            "float": "double",
            "double": "double",
            "decimal": "double",
            "bool": "boolean",
            "boolean": "boolean",
            "date": "date",
            "timestamp": "timestamp",
            "datetime": "timestamp",
        }

        # If the type looks like a complex DDL string (struct<...>, array<...>, etc.),
        # return it as-is so Spark can parse it natively.
        if type_name.startswith(("struct<", "array<", "map<")):
            return type_name

        return mapping.get(type_name)

    def _apply_schema(self, df: Any) -> Tuple[Any, List[str]]:
        """
        Apply schema casts, missing columns, and unknown field handling.

        Args:
            df: Spark DataFrame.

        Returns:
            Tuple of (Spark DataFrame, schema_errors).
        """
        from pyspark.sql import functions as F

        if not self.contract.model or not self.contract.model.fields:
            if self.contract.server and self.contract.server.cast_to_string:
                for col in df.columns:
                    df = df.withColumn(col, F.col(col).cast("string"))
            return df, []

        expected_fields = [f.name for f in self.contract.model.fields]
        existing = set(df.columns)
        expected = set(expected_fields)
        missing = expected - existing
        unknown = existing - expected
        system_cols = {c for c in unknown if c.startswith("_lakelogic_")}
        unknown = unknown - system_cols - self._lineage_columns()

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

        select_exprs = []
        self._type_err_cols = []
        for field in self.contract.model.fields:
            if field.name in existing:
                col_expr = F.col(field.name)
            else:
                col_expr = F.lit(None)

            spark_type = "string" if cast_to_string else self._to_spark_type(field.type)
            if spark_type:
                if field.name in existing and not cast_to_string:
                    err_col = f"__type_err_{field.name}"
                    self._type_err_cols.append(err_col)
                    msg = f"Type Mismatch: {field.name} cannot be cast to {field.type}"
                    # Use try_cast to return NULL on failure instead of crashing under ANSI strict mode
                    cast_expr = F.expr(f"try_cast(`{field.name}` as {spark_type})")
                    select_exprs.append(
                        F.when(col_expr.isNotNull() & cast_expr.isNull(), F.lit(msg))
                        .otherwise(F.lit(None))
                        .alias(err_col)
                    )
                # Apply the safe cast to the final column as well
                col_expr = (
                    F.expr(f"try_cast(`{field.name}` as {spark_type})")
                    if field.name in existing
                    else col_expr.cast(spark_type)
                )

            select_exprs.append(col_expr.alias(field.name))

        if policy in ["allow", "quarantine"] and unknown:
            if cast_to_string:
                select_exprs.extend([F.col(c).cast("string") for c in sorted(unknown)])
            else:
                select_exprs.extend([F.col(c) for c in sorted(unknown)])

        df = df.select(*select_exprs)

        # ── Detect downstream reshaping so we don't false-positive here ──────
        # When a contract's model describes the OUTPUT of a later step (a post-
        # phase transform, or a materialization strategy that injects columns),
        # the model fields are legitimately absent from the raw source at THIS
        # stage (post-transforms + materialization run AFTER schema enforcement).
        # Enforcing missing/unknown here would quarantine every row. Covers:
        #   • post-phase transforms: sql, rollup, pivot, unpivot, derive
        #   • SCD2 materialization (injects surrogate key / effective_from-to /
        #     is_current / version columns after this stage)
        _has_post_sql = False
        if self.contract.transformations:
            for _t in self.contract.transformations:
                _phase = (getattr(_t, "phase", None) or "post").lower()
                if _phase == "post" and (
                    getattr(_t, "sql", None)
                    or getattr(_t, "rollup", None)
                    or getattr(_t, "pivot", None)
                    or getattr(_t, "unpivot", None)
                    or getattr(_t, "derive", None)
                ):
                    _has_post_sql = True
                    break
        if not _has_post_sql:
            _mat = getattr(self.contract, "materialization", None)
            if str(getattr(_mat, "strategy", "") or "").lower() == "scd2":
                _has_post_sql = True

        schema_errors = []
        if evolution == "strict" and missing and not _has_post_sql:
            schema_errors.append(f"Missing fields: {', '.join(sorted(missing))}")
        if policy == "quarantine" and unknown and not _has_post_sql:
            schema_errors.append(f"Unknown fields present: {', '.join(sorted(unknown))}")

        self.schema_drift = {
            "missing_fields": sorted(missing),
            "unknown_fields": sorted(unknown),
            "policy": policy,
            "evolution": evolution or "",
        }

        return df, schema_errors
