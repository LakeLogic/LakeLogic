from typing import Tuple, Any, List
from lakeguard.engines.base import EngineAdapter
from loguru import logger
from pathlib import Path

class SparkAdapter(EngineAdapter):
    """
    Spark execution engine for LakeGuard.
    Uses Spark SQL and Column Expressions for evaluation.
    """

    def execute(self, df: Any) -> Tuple[Any, Any]:
        """
        Executes the contract using PySpark.
        """
        self.dataset_rule_results = []
        try:
            from pyspark.sql import functions as F
            from pyspark.sql import DataFrame
        except ImportError:
            raise ImportError("pyspark is required for SparkAdapter")

        if not isinstance(df, DataFrame):
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
                cond = F.col(col_name).isNull() | (F.col(col_name) == False)
                error_exprs.append(F.when(cond, F.lit(error_msg)).otherwise(None))
                category_exprs.append(F.when(cond, F.lit(rule.category)).otherwise(None))

            df_with_errors = df_eval.withColumn(
                self.ERROR_COLUMN,
                F.array_remove(F.array(*error_exprs), None)
            ).withColumn(
                self.CATEGORY_COLUMN,
                F.array_remove(F.array(*category_exprs), None)
            )
        else:
            if error_exprs:
                df_with_errors = df_eval.withColumn(self.ERROR_COLUMN, F.array(*error_exprs)) \
                                   .withColumn(self.CATEGORY_COLUMN, F.array(*category_exprs))
            else:
                df_with_errors = df_eval.withColumn(self.ERROR_COLUMN, F.array().cast("array<string>")) \
                                   .withColumn(self.CATEGORY_COLUMN, F.array().cast("array<string>"))

        # 3. Split Good and Bad
        has_errors = F.size(F.col(self.ERROR_COLUMN)) > 0
        
        bad_df = df_with_errors.filter(has_errors) \
            .withColumn("quarantine_state", F.lit("active")) \
            .withColumn("quarantine_reprocessed", F.lit(False))
        
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
                if rule.must_be_between:
                    passed = rule.must_be_between[0] <= val <= rule.must_be_between[1]
                elif rule.must_be_less_than is not None:
                    passed = val < rule.must_be_less_than
                elif rule.must_be_greater_than is not None:
                    passed = val > rule.must_be_greater_than
                
                status = "✅ PASS" if passed else "❌ FAIL"
                logger.info(f"Quality Check (Spark): {rule.name} | Result: {val} | Status: {status}")
                self.dataset_rule_results.append({
                    "name": rule.name,
                    "value": val,
                    "passed": passed,
                    "description": rule.description
                })
            except Exception as e:
                logger.error(f"Error executing dataset rule '{rule.name}': {e}")

    def _apply_pre_transformations(self, df: Any) -> Any:
        from pyspark.sql import functions as F
        from pyspark.sql import Window
        
        current_df = df
        existing = set(current_df.columns)
        for trans in self.contract.transformations:
            if trans.rename:
                if trans.rename.from_name not in existing:
                    logger.warning(f"Pre-Transform [Rename] skipped; column not found: {trans.rename.from_name}")
                else:
                    logger.debug(f"Pre-Transform [Rename]: {trans.rename.from_name} -> {trans.rename.to_name}")
                    current_df = current_df.withColumnRenamed(trans.rename.from_name, trans.rename.to_name)
                    existing.remove(trans.rename.from_name)
                    existing.add(trans.rename.to_name)
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
                    current_df = current_df.withColumn("_rn", F.row_number().over(w)) \
                                          .filter(F.col("_rn") == 1) \
                                          .drop("_rn")
                else:
                    current_df = current_df.dropDuplicates(trans.deduplicate.on)
        return current_df

    def _apply_post_transformations(self, df: Any) -> Any:
        current_df = df
        tbl_name = "current_transform"
        
        for trans in self.contract.transformations:
            if trans.derive:
                logger.debug(f"Post-Transform [Derive]: {trans.derive.field}")
                current_df.createOrReplaceTempView(tbl_name)
                query = f"SELECT *, ({trans.derive.sql}) AS {trans.derive.field} FROM {tbl_name}"
                current_df = current_df.sparkSession.sql(query)
            elif trans.lookup:
                logger.debug(f"Post-Transform [Lookup]: {trans.lookup.field}")
                current_df.createOrReplaceTempView("src")
                query = f"""
                SELECT src.*, ref.{trans.lookup.value} AS {trans.lookup.field}
                FROM src
                LEFT JOIN {trans.lookup.reference} ref ON src.{trans.lookup.on} = ref.{trans.lookup.key}
                """
                current_df = current_df.sparkSession.sql(query)
            elif trans.filter:
                logger.debug(f"Post-Transform [Filter]: {trans.filter.sql}")
                current_df = current_df.filter(trans.filter.sql)
        return current_df

    def _register_links(self, spark) -> None:
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
        return mapping.get(type_name)

    def _apply_schema(self, df: Any) -> Tuple[Any, List[str]]:
        from pyspark.sql import functions as F

        if not self.contract.model or not self.contract.model.fields:
            return df, []

        expected_fields = [f.name for f in self.contract.model.fields]
        existing = set(df.columns)
        expected = set(expected_fields)
        unknown = existing - expected

        select_exprs = []
        for field in self.contract.model.fields:
            if field.name in existing:
                col_expr = F.col(field.name)
            else:
                col_expr = F.lit(None)

            spark_type = self._to_spark_type(field.type)
            if spark_type:
                col_expr = col_expr.cast(spark_type)

            select_exprs.append(col_expr.alias(field.name))

        policy = self.contract.schema_policy.unknown_fields if self.contract.schema_policy else "allow"
        if policy in ["allow", "quarantine"] and unknown:
            select_exprs.extend([F.col(c) for c in sorted(unknown)])

        df = df.select(*select_exprs)

        schema_errors = []
        if policy == "quarantine" and unknown:
            schema_errors.append(f"Unknown fields present: {', '.join(sorted(unknown))}")

        return df, schema_errors
