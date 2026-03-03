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
        except ImportError:
            raise ImportError("pyspark is required for SparkAdapter")

        # Databricks Serverless / Spark Connect returns a different DataFrame class.
        # Build a tuple of all available DataFrame types so isinstance works on both
        # classic and Serverless runtimes.
        _df_types = [DataFrame]
        try:
            from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

            _df_types.append(ConnectDataFrame)
        except ImportError:
            pass
        _df_types = tuple(_df_types)

        # Auto-convert a Python list/tuple of dicts to a Spark DataFrame.
        # This lets callers pass in-memory records (e.g. rows fetched from SQLite,
        # a list of dicts, Pandas-style .to_dict("records")) without having to
        # manually create a SparkSession first.
        if isinstance(df, (list, tuple)):
            from pyspark.sql import SparkSession

            _spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
            df = _spark.createDataFrame(df)

        elif not isinstance(df, _df_types):
            raise TypeError(f"Expected Spark DataFrame, got {type(df)}")

        # 1. Register Source and Links in Spark Session
        spark = df.sparkSession
        tbl_name = self.contract.dataset or "source"
        df.createOrReplaceTempView(tbl_name)
        self._register_links(spark)

        # 0. Apply pre-processing (renames, filters, deduplication)
        df = self._apply_pre_transformations(df)

        # 0.5 Apply schema enforcement
        df, schema_errors = self._apply_schema(df)
        df.createOrReplaceTempView(tbl_name)

        # 2. Evaluate Row-Level Rules
        row_rules = self.get_row_rules()
        error_exprs = []
        category_exprs = []

        if schema_errors:
            error_exprs.extend([F.lit(err) for err in schema_errors])
            category_exprs.extend([F.lit("schema") for _ in schema_errors])

        df_eval = df
        internal_cols = []
        if row_rules:
            rule_exprs = []
            for i, rule in enumerate(row_rules):
                rule_exprs.append(f"CAST(({rule.sql}) AS BOOLEAN) as _rule_{i}")
                internal_cols.append(f"_rule_{i}")

            eval_sql = f"SELECT *, {', '.join(rule_exprs)} FROM {tbl_name}"
            df_eval = spark.sql(eval_sql)

            for i, rule in enumerate(row_rules):
                col_name = f"_rule_{i}"
                error_msg = f"Rule failed: {rule.name} ({rule.sql})"
                cond = F.col(col_name).isNull() | F.col(col_name).isFalse()
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

        drop_cols = [self.ERROR_COLUMN, self.CATEGORY_COLUMN] + internal_cols
        good_df = df_with_errors.filter(~has_errors).drop(*drop_cols)
        bad_df = bad_df.drop(*internal_cols)

        # 4. Apply Dataset-Level (Aggregate) Checks
        self._run_dataset_rules(good_df)

        # 5. Apply Transformations to Good Data
        if self.contract.transformations:
            good_df = self._apply_post_transformations(good_df)

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

        tbl_name = self.contract.dataset or "source"
        df.createOrReplaceTempView(tbl_name)
        spark = df.sparkSession

        for rule in rules:
            try:
                res = spark.sql(rule.sql).collect()
                val = res[0][0]

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
                logger.info(f"Quality Check (Spark): {rule.name} | Result: {val} | Status: {status}")
                self.dataset_rule_results.append(
                    {
                        "name": rule.name,
                        "value": val,
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

        current_df = df
        existing = set(current_df.columns)
        for trans in self.contract.transformations:
            trans_phase = (trans.phase or "post").lower()

            # ── Execute Transformation ────────────────────────────────────────
            if trans.sql and trans_phase == "pre":
                logger.debug(f"Pre-Transform [SQL]: {trans.sql}")
                current_df.createOrReplaceTempView("source")
                current_df = current_df.sparkSession.sql(trans.sql)
                existing = set(current_df.columns)

            elif trans.derive and trans_phase == "pre":
                logger.debug(f"Pre-Transform [Derive]: {trans.derive.field}")
                current_df = current_df.withColumn(trans.derive.field, F.expr(trans.derive.sql))
                existing = set(current_df.columns)

            elif trans.pivot and trans_phase == "pre":
                pivot_sql = self._build_pivot_sql(trans.pivot, source_table=self.contract.dataset or "source")
                if pivot_sql:
                    logger.debug(f"Pre-Transform [Pivot]: {pivot_sql}")
                    current_df.createOrReplaceTempView("source")
                    if self.contract.dataset:
                        current_df.createOrReplaceTempView(self.contract.dataset)
                    current_df = current_df.sparkSession.sql(pivot_sql)
                    existing = set(current_df.columns)
                continue

            if trans.unpivot and trans_phase == "pre":
                unpivot_sql = self._build_unpivot_sql(trans.unpivot, source_table=self.contract.dataset or "source")
                if unpivot_sql:
                    logger.debug(f"Pre-Transform [Unpivot]: {unpivot_sql}")
                    current_df.createOrReplaceTempView("source")
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
                    current_df = current_df.withColumn(output, F.split(F.col(trans.split.field), trans.split.delimiter))
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
            elif trans.deduplicate:
                logger.debug(f"Pre-Transform [Deduplicate]: {trans.deduplicate.on}")
                if trans.deduplicate.sort_by:
                    w = Window.partitionBy(*trans.deduplicate.on)
                    order_cols = []
                    for col in trans.deduplicate.sort_by:
                        order_cols.append(F.col(col).desc() if trans.deduplicate.order == "desc" else F.col(col).asc())
                    w = w.orderBy(*order_cols)
                    current_df = (
                        current_df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
                    )
                else:
                    current_df = current_df.dropDuplicates(trans.deduplicate.on)

            # ── Post-Step Sync ──────────────────────────────────────────────
            # Re-register the updated DataFrame as 'source' so that the NEXT
            # transformation phase (especially SQL-based ones) sees the new columns.
            current_df.createOrReplaceTempView("source")
            if self.contract.dataset:
                current_df.createOrReplaceTempView(self.contract.dataset)

        return current_df

    def _apply_post_transformations(self, df: Any) -> Any:
        """
        Apply post-processing transformations (derive, lookup, join, SQL).

        Args:
            df: Spark DataFrame.

        Returns:
            Transformed Spark DataFrame.
        """
        current_df = df

        for trans in self.contract.transformations:
            trans_phase = (trans.phase or "post").lower()
            if trans.sql and trans_phase != "pre":
                logger.debug(f"Post-Transform [SQL]: {trans.sql}")
                current_df.createOrReplaceTempView("source")
                if self.contract.dataset:
                    current_df.createOrReplaceTempView(self.contract.dataset)
                current_df = current_df.sparkSession.sql(trans.sql)
                continue
            if trans.rollup and trans_phase != "pre":
                rollup_sql = self._build_rollup_sql(trans.rollup, source_table=self.contract.dataset or "source")
                logger.debug(f"Post-Transform [Rollup]: {rollup_sql}")
                current_df.createOrReplaceTempView("source")
                if self.contract.dataset:
                    current_df.createOrReplaceTempView(self.contract.dataset)
                current_df = current_df.sparkSession.sql(rollup_sql)
                continue

            if trans.pivot and trans_phase != "pre":
                pivot_sql = self._build_pivot_sql(trans.pivot, source_table=self.contract.dataset or "source")
                if pivot_sql:
                    logger.debug(f"Post-Transform [Pivot]: {pivot_sql}")
                    current_df.createOrReplaceTempView("source")
                    if self.contract.dataset:
                        current_df.createOrReplaceTempView(self.contract.dataset)
                    current_df = current_df.sparkSession.sql(pivot_sql)
                continue

            if trans.unpivot and trans_phase != "pre":
                unpivot_sql = self._build_unpivot_sql(trans.unpivot, source_table=self.contract.dataset or "source")
                if unpivot_sql:
                    logger.debug(f"Post-Transform [Unpivot]: {unpivot_sql}")
                    current_df.createOrReplaceTempView("source")
                    if self.contract.dataset:
                        current_df.createOrReplaceTempView(self.contract.dataset)
                    current_df = current_df.sparkSession.sql(unpivot_sql)
                continue

            if trans.derive:
                logger.debug(f"Post-Transform [Derive]: {trans.derive.field}")
                from pyspark.sql import functions as F

                current_df = current_df.withColumn(trans.derive.field, F.expr(trans.derive.sql))
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
                current_df.createOrReplaceTempView("src")
                hint = ""
                if self._should_broadcast(trans.lookup.reference):
                    hint = "/*+ BROADCAST(ref) */ "
                value_expr = f"ref.{trans.lookup.value}"
                if trans.lookup.default_value is not None:
                    value_expr = (
                        f"COALESCE(ref.{trans.lookup.value}, {self._format_literal(trans.lookup.default_value)})"
                    )
                query = f"""
                SELECT {hint}src.*, {value_expr} AS {trans.lookup.field}
                FROM src
                LEFT JOIN {trans.lookup.reference} ref ON src.{trans.lookup.on} = ref.{trans.lookup.key}
                """
                current_df = current_df.sparkSession.sql(query)
            elif trans.join:
                logger.debug(f"Post-Transform [Join]: {trans.join.reference}")
                current_df.createOrReplaceTempView("source")
                query = self._build_join_sql(trans.join, broadcast=self._should_broadcast(trans.join.reference))
                current_df = current_df.sparkSession.sql(query)
            elif trans.filter and trans_phase != "pre":
                logger.debug(f"Post-Transform [Filter]: {trans.filter.sql}")
                current_df = current_df.filter(trans.filter.sql)
        return current_df

    def _build_join_sql(self, join_cfg, broadcast: bool = False, source_table: str = "source") -> str:
        """
        Build SQL for a join transformation.

        Args:
            join_cfg: Join configuration.
            broadcast: Whether to broadcast the reference table.
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

        hint = "/*+ BROADCAST(ref) */ " if broadcast else ""
        return f"""
        SELECT {hint}{", ".join(select_fields)}
        FROM {source_table} src
        {join_type} JOIN {join_cfg.reference} ref ON src.{join_cfg.on} = ref.{join_cfg.key}
        """

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
                    ref_df.createOrReplaceTempView(link.name)
                    continue

                if not link.path:
                    continue

                if link.path.startswith(("s3://", "gs://", "abfss://", "adl://", "https://")):
                    logger.warning(
                        f"Link '{link.name}' uses remote path '{link.path}'. Local-only loading supported in OSS demo."
                    )
                    continue

                path = Path(link.path)
                if not path.is_absolute() and hasattr(self.contract, "_base_path"):
                    path = Path(self.contract._base_path) / path
                if not path.exists():
                    logger.warning(f"Link file not found: {path}")
                    continue

                if path.suffix.lower() == ".parquet":
                    ref_df = spark.read.parquet(path.as_posix())
                elif path.suffix.lower() == ".csv":
                    ref_df = spark.read.option("header", "true").csv(path.as_posix())
                else:
                    logger.warning(f"Unsupported link format for {link.name}: {path.suffix}")
                    continue

                ref_df.createOrReplaceTempView(link.name)
            except Exception as e:
                logger.warning(f"Could not register link {link.name}: {e}")

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
            if self.contract.server and self.contract.server.mode == "ingest" and self.contract.server.cast_to_string:
                for col in df.columns:
                    df = df.withColumn(col, F.col(col).cast("string"))
            return df, []

        expected_fields = [f.name for f in self.contract.model.fields]
        existing = set(df.columns)
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
            if field.name in existing:
                col_expr = F.col(field.name)
            else:
                col_expr = F.lit(None)

            spark_type = "string" if cast_to_string else self._to_spark_type(field.type)
            if spark_type:
                col_expr = col_expr.cast(spark_type)

            select_exprs.append(col_expr.alias(field.name))

        if policy in ["allow", "quarantine"] and unknown:
            if cast_to_string:
                select_exprs.extend([F.col(c).cast("string") for c in sorted(unknown)])
            else:
                select_exprs.extend([F.col(c) for c in sorted(unknown)])

        df = df.select(*select_exprs)

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

        return df, schema_errors
