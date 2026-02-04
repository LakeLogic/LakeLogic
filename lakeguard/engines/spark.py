from typing import Tuple, Any, List
from lakeguard.engines.base import EngineAdapter
from loguru import logger

class SparkAdapter(EngineAdapter):
    """
    Spark execution engine for LakeGuard.
    Uses Spark SQL and Column Expressions for evaluation.
    """

    def execute(self, df: Any) -> Tuple[Any, Any]:
        """
        Executes the contract using PySpark.
        """
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
        
        # 2. Evaluate Row-Level Rules
        row_rules = self.get_row_rules()
        error_exprs = []
        category_exprs = []
        
        if row_rules:
            for rule in row_rules:
                # Rule.sql is a boolean expression evaluated via expr
                cond = f"NOT ({rule.sql}) OR ({rule.sql}) IS NULL"
                error_msg = f"Rule failed: {rule.name} ({rule.sql})"
                
                error_exprs.append(F.when(F.expr(cond), F.lit(error_msg)).otherwise(None))
                category_exprs.append(F.when(F.expr(cond), F.lit(rule.category)).otherwise(None))

            df_with_errors = df.withColumn(
                self.ERROR_COLUMN, 
                F.array_remove(F.array(*error_exprs), None)
            ).withColumn(
                self.CATEGORY_COLUMN,
                F.array_remove(F.array(*category_exprs), None)
            )
        else:
            df_with_errors = df.withColumn(self.ERROR_COLUMN, F.array().cast("array<string>")) \
                               .withColumn(self.CATEGORY_COLUMN, F.array().cast("array<string>"))

        # 3. Split Good and Bad
        has_errors = F.size(F.col(self.ERROR_COLUMN)) > 0
        
        bad_df = df_with_errors.filter(has_errors).withColumns({
            "quarantine_state": F.lit("active"),
            "quarantine_reprocessed": F.lit(False)
        })
        
        good_df = df_with_errors.filter(~has_errors).drop(self.ERROR_COLUMN, self.CATEGORY_COLUMN)

        # 4. Apply Dataset-Level (Aggregate) Checks
        self._run_dataset_rules(good_df)

        # 5. Apply Transformations to Good Data
        if self.contract.transformations:
            good_df = self._apply_transformations(good_df)

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
            except Exception as e:
                logger.error(f"Error executing dataset rule '{rule.name}': {e}")

    def _apply_transformations(self, df: Any) -> Any:
        current_df = df
        tbl_name = "current_transform"
        
        for trans in self.contract.transformations:
            if trans.rename:
                current_df = current_df.withColumnRenamed(trans.rename.from_name, trans.rename.to_name)
            elif trans.derive:
                current_df.createOrReplaceTempView(tbl_name)
                query = f"SELECT *, ({trans.derive.sql}) AS {trans.derive.field} FROM {tbl_name}"
                current_df = current_df.sparkSession.sql(query)
            elif trans.lookup:
                # Minimal lookup implementation for Spark (Assuming link table exists in Spark Catalog)
                current_df.createOrReplaceTempView("src")
                query = f"""
                SELECT src.*, ref.{trans.lookup.value} AS {trans.lookup.field}
                FROM src
                LEFT JOIN {trans.lookup.reference} ref ON src.{trans.lookup.on} = ref.{trans.lookup.key}
                """
                current_df = current_df.sparkSession.sql(query)
        return current_df
