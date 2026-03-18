import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import polars as pl
from loguru import logger

from lakelogic.engines.base import EngineAdapter


class PolarsAdapter(EngineAdapter):
    """
    Polars execution engine for LakeLogic.
    Supports row-level validation, aggregate metrics, and SQL-first transformations.
    """

    _link_cache: Dict[str, pl.LazyFrame] = {}
    engine_name: str = "polars"

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
                    logger.warning(
                        f"Link '{link.name}' references table '{table_name}'."
                        " Table links are supported in Spark only for OSS."
                    )
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
        # ── Idempotency guard ─────────────────────────────────────────────────
        # If the SQL adds new columns (e.g. `creation_date AS creation_date_raw`)
        # and those columns ALREADY exist in the frame (e.g. from generated test
        # data), Polars/DuckDB raise "duplicate output name".
        #
        # IMPORTANT: skip this guard when the query already uses SELECT * EXCLUDE
        # (col) — that syntax already handles overwrite.  Pre-dropping the column
        # first causes the subsequent engine to fail with "column not found to EXCLUDE".
        import re as _re

        _SQL_TYPE_KEYWORDS = {
            "DATE",
            "TIME",
            "TIMESTAMP",
            "DATETIME",
            "INTEGER",
            "INT",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
            "NUMERIC",
            "VARCHAR",
            "STRING",
            "TEXT",
            "BOOLEAN",
            "BOOL",
            "BLOB",
            "BINARY",
        }
        uses_exclude = bool(_re.search(r"\bEXCLUDE\b", sql, _re.IGNORECASE))
        if not uses_exclude:
            existing_cols = set(lf.collect_schema().names())
            # Remove REPLACE(...) blocks then find remaining AS aliases
            sql_no_replace = _re.sub(r"\bREPLACE\s*\([^)]*\)", "", sql, flags=_re.IGNORECASE | _re.DOTALL)
            new_aliases = {m.group(1) for m in _re.finditer(r'\bAS\s+(["\\w]+)', sql_no_replace, _re.IGNORECASE)}
            # Strip optional quotes, exclude SQL type keywords (e.g. CAST(x AS DATE))
            new_aliases = {
                a.strip('"').strip("'")
                for a in new_aliases
                if a.strip('"').strip("'").upper() not in _SQL_TYPE_KEYWORDS
            }
            cols_to_drop = new_aliases & existing_cols
            if cols_to_drop:
                lf = lf.drop(list(cols_to_drop))

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
                        con.execute(
                            f"CREATE OR REPLACE VIEW {link.name} AS SELECT * FROM read_parquet('{path.as_posix()}')"
                        )
                    elif path.suffix.lower() == ".csv":
                        con.execute(
                            f"CREATE OR REPLACE VIEW {link.name} AS SELECT * FROM read_csv_auto('{path.as_posix()}')"
                        )
                except Exception:
                    continue
            rel = con.query(sql)
            out = pl.from_pandas(rel.df()).lazy()
            con.close()
            return out

    @staticmethod
    def _normalize_sql(sql: str) -> str:
        """
        Replace standard SQL temporal functions that Polars SQL does not
        support with equivalent inline literals evaluated at plan-build time.

        Substitutions (case-insensitive):
          CURRENT_TIMESTAMP / NOW()  →  TIMESTAMP 'YYYY-MM-DD HH:MM:SS'
          CURRENT_DATE               →  DATE 'YYYY-MM-DD'
          CURRENT_TIME               →  TIME 'HH:MM:SS'
        """
        import datetime as _dt
        import re as _re

        now = _dt.datetime.now(_dt.timezone.utc)
        today_str = now.strftime("%Y-%m-%d")
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        time_str = now.strftime("%H:%M:%S")

        sql = _re.sub(r"\bNOW\s*\(\s*\)", f"TIMESTAMP '{now_str}'", sql, flags=_re.IGNORECASE)
        sql = _re.sub(
            r"\bCURRENT_TIMESTAMP\b",
            f"TIMESTAMP '{now_str}'",
            sql,
            flags=_re.IGNORECASE,
        )
        sql = _re.sub(r"\bCURRENT_DATE\b", f"DATE '{today_str}'", sql, flags=_re.IGNORECASE)
        sql = _re.sub(r"\bCURRENT_TIME\b", f"TIME '{time_str}'", sql, flags=_re.IGNORECASE)
        return sql

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
            current_schema = lf.collect_schema()
            for field in self.contract.model.fields:
                dtype = self._to_polars_dtype(field.type)
                if dtype is None:
                    continue
                if field.name not in current_schema.names():
                    continue
                current_dtype = current_schema[field.name]
                # List[*] → String: serialise to JSON string first, then cast
                if isinstance(current_dtype, pl.List) and dtype == pl.Utf8:
                    import json as _json

                    lf = lf.with_columns(
                        pl.col(field.name)
                        .map_elements(
                            lambda v: (
                                _json.dumps(
                                    v.to_list() if hasattr(v, "to_list") else v,
                                    ensure_ascii=False,
                                )
                                if v is not None
                                else None
                            ),
                            return_dtype=pl.Utf8,
                        )
                        .alias(field.name)
                    )
                else:
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
        start_time = time.perf_counter()
        self.dataset_rule_results = []
        self.schema_drift = {}
        self.trace = []

        # 0. Load as LazyFrame
        if isinstance(df, pl.DataFrame):
            lf = df.lazy()
        elif isinstance(df, pl.LazyFrame):
            lf = df
        else:
            lf = pl.from_pandas(df).lazy() if hasattr(df, "to_numpy") else pl.DataFrame(df).lazy()

        raw_count = self._get_row_count(lf)
        self._add_trace(
            "Load Source",
            input_rows=None,
            output_rows=raw_count,
            duration_ms=(time.perf_counter() - start_time) * 1000,
        )

        # 0.25 Apply renames, filters, deduplication before schema enforcement
        # This handles the 'supersede' case where we want to clean data ASAP
        if self.contract.transformations:
            step_start = time.perf_counter()
            pre_input_count = raw_count
            lf = self._apply_pre_transformations(lf)
            pre_output_count = self._get_row_count(lf)
            self._add_trace(
                "Pre-Transformations",
                input_rows=pre_input_count,
                output_rows=pre_output_count,
                duration_ms=(time.perf_counter() - step_start) * 1000,
            )

        # 0.5 Apply schema enforcement (casts, missing cols, unknowns)
        step_start = time.perf_counter()
        schema_input_count = self._get_row_count(lf)
        lf, schema_errors = self._apply_schema(lf)
        schema_output_count = self._get_row_count(lf)
        self._add_trace(
            "Schema Enforcement",
            input_rows=schema_input_count,
            output_rows=schema_output_count,
            duration_ms=(time.perf_counter() - step_start) * 1000,
            details={"errors": schema_errors},
        )

        # 0.75 Apply Post-Transformations BEFORE quality rules so that derived
        # columns (snapshot_year, gold_processed_at, postcode_area, etc.) are
        # populated when the row-level NOT-NULL / validity rules run.
        # The good/bad split still happens below — post-transforms just run on
        # all rows so the enriched values are ready for validation.
        ctx = self._get_context(lf)
        post_output_count = self._get_row_count(lf)  # default if no transforms
        if self.contract.transformations:
            step_start = time.perf_counter()
            post_input_count = self._get_row_count(lf)
            lf = self._apply_post_transformations(lf, ctx)
            post_output_count = self._get_row_count(lf)
            self._add_trace(
                "Post-Transformations",
                input_rows=post_input_count,
                output_rows=post_output_count,
                duration_ms=(time.perf_counter() - step_start) * 1000,
            )

        # 1. Evaluate Row-Level Rules
        row_rules = self.get_row_rules()
        ctx = self._get_context(lf)

        if row_rules:
            step_start = time.perf_counter()
            rule_exprs = []
            for i, rule in enumerate(row_rules):
                rule_exprs.append(f"CAST(({rule.sql}) AS BOOLEAN) as _rule_{i}")

            # Run all rules in one pass
            eval_sql = f"SELECT *, {', '.join(rule_exprs)} FROM {self.contract.dataset or 'source'}"
            lf_eval = ctx.execute(eval_sql)
            self._add_trace(
                "Row Rules Evaluation",
                input_rows=post_output_count,
                output_rows=post_output_count,
                duration_ms=(time.perf_counter() - step_start) * 1000,
                details={"sql": eval_sql, "rules_count": len(row_rules)},
            )

            error_tracking_exprs = []
            category_tracking_exprs = []

            if schema_errors:
                error_tracking_exprs.extend([pl.lit(err) for err in schema_errors])
                category_tracking_exprs.extend([pl.lit("schema") for _ in schema_errors])

            for i, rule in enumerate(row_rules):
                col_name = f"_rule_{i}"
                error_msg = f"Rule failed: {rule.name} ({rule.sql})"
                condition = pl.col(col_name).is_null() | pl.col(col_name).not_()

                error_tracking_exprs.append(pl.when(condition).then(pl.lit(error_msg)).otherwise(None))
                category_tracking_exprs.append(pl.when(condition).then(pl.lit(rule.category)).otherwise(None))

            lf_with_errors = lf_eval.with_columns(
                [
                    pl.concat_list(error_tracking_exprs).list.drop_nulls().alias(self.ERROR_COLUMN),
                    pl.concat_list(category_tracking_exprs).list.drop_nulls().alias(self.CATEGORY_COLUMN),
                ]
            )
        else:
            schema_error_exprs = [pl.lit(err) for err in schema_errors] if schema_errors else []
            schema_category_exprs = [pl.lit("schema") for _ in schema_errors] if schema_errors else []
            lf_with_errors = lf.with_columns(
                [
                    pl.concat_list(schema_error_exprs).list.drop_nulls().alias(self.ERROR_COLUMN)
                    if schema_error_exprs
                    else pl.lit([]).cast(pl.List(pl.String)).alias(self.ERROR_COLUMN),
                    pl.concat_list(schema_category_exprs).list.drop_nulls().alias(self.CATEGORY_COLUMN)
                    if schema_category_exprs
                    else pl.lit([]).cast(pl.List(pl.String)).alias(self.CATEGORY_COLUMN),
                ]
            )

        # 2. Split Good and Bad
        has_errors = pl.col(self.ERROR_COLUMN).list.len() > 0

        bad_lf = lf_with_errors.filter(has_errors).with_columns(
            [
                pl.lit("active").alias("quarantine_state"),
                pl.lit(False).alias("quarantine_reprocessed"),
            ]
        )

        # Clean up internal columns
        internal_cols = [f"_rule_{i}" for i in range(len(row_rules))]
        good_lf = lf_with_errors.filter(~has_errors).drop(internal_cols + [self.ERROR_COLUMN, self.CATEGORY_COLUMN])

        # 3. Apply Dataset-Level (Aggregate) Checks
        self._run_dataset_rules(good_lf, ctx)

        # (Post-Transformations already applied at step 0.75 above)

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
                sql = rule.sql.replace("{dataset}", tbl_name)
                res = ctx.execute(sql).collect()
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
            trans_phase = (trans.phase or "post").lower()
            if trans.sql and (trans.phase or "post").lower() == "pre":
                logger.debug(f"Pre-Transform [SQL]: {trans.sql}")
                try:
                    current_lf = self._apply_sql_transformation(current_lf, trans.sql)
                    existing = set(current_lf.collect_schema().names())
                except Exception as e:
                    logger.warning(f"Pre-Transform [SQL] failed: {e}")
                continue

            if trans.json_extract and trans_phase == "pre":
                cfg = trans.json_extract
                if cfg.source not in existing:
                    logger.warning(f"Pre-Transform [JsonExtract]: source column '{cfg.source}' not found, skipping.")
                    continue
                logger.debug(f"Pre-Transform [JsonExtract]: {cfg.source}[{cfg.path}] -> {cfg.field}")
                extracted = pl.col(cfg.source).str.json_path_match(cfg.path)
                if cfg.cast:
                    dtype = self._to_polars_dtype(cfg.cast) or pl.Utf8
                    extracted = extracted.cast(dtype, strict=False)
                current_lf = current_lf.with_columns(extracted.alias(cfg.field))
                existing = set(current_lf.collect_schema().names())
                continue

            if trans.derive and trans_phase == "pre":
                logger.debug(f"Pre-Transform [Derive]: {trans.derive.field}")
                field_name = trans.derive.field
                derive_sql = self._normalize_sql(trans.derive.sql)
                _resolved = False
                try:
                    # Try Polars SQL first via a fresh context (avoids the
                    # idempotency-guard regex bug in _apply_sql_transformation).
                    _ctx = pl.SQLContext()
                    _ctx.register("source", current_lf)
                    if self.contract.dataset:
                        _ctx.register(self.contract.dataset, current_lf)
                    tbl = "source"
                    if field_name in existing:
                        query = f"SELECT * EXCLUDE ({field_name}), ({derive_sql}) AS {field_name} FROM {tbl}"
                    else:
                        query = f"SELECT *, ({derive_sql}) AS {field_name} FROM {tbl}"
                    current_lf = _ctx.execute(query)
                    existing = set(current_lf.collect_schema().names())
                    _resolved = True
                except Exception as e:
                    logger.warning(
                        f"Pre-Transform [Derive] '{field_name}' Polars SQL failed ({e}); "
                        f"trying DuckDB fallback."
                    )
                # DuckDB fallback (mirrors post-derive logic)
                if not _resolved:
                    try:
                        if field_name in existing:
                            _dq = f"SELECT * EXCLUDE ({field_name}), ({derive_sql}) AS {field_name} FROM source"
                        else:
                            _dq = f"SELECT *, ({derive_sql}) AS {field_name} FROM source"
                        current_lf = self._apply_sql_transformation(current_lf, _dq)
                        existing = set(current_lf.collect_schema().names())
                        _resolved = True
                    except Exception as e2:
                        logger.error(
                            f"Pre-Transform [Derive] '{field_name}' FAILED all engines. "
                            f"SQL: {derive_sql} | Polars error + DuckDB error: {e2}"
                        )
                        raise RuntimeError(
                            f"Pre-Transform [Derive] '{field_name}' failed in both Polars SQL and DuckDB. "
                            f"SQL: {derive_sql}"
                        ) from e2
                continue

            if trans.pivot and trans_phase == "pre":
                pivot_sql = self._build_pivot_sql(trans.pivot, source_table=self.contract.dataset or "source")
                if not pivot_sql:
                    continue
                logger.debug(f"Pre-Transform [Pivot]: {pivot_sql}")
                try:
                    current_lf = self._apply_sql_transformation(current_lf, pivot_sql)
                    existing = set(current_lf.collect_schema().names())
                except Exception as e:
                    logger.warning(f"Pre-Transform [Pivot] failed: {e}")
                continue

            if trans.unpivot and trans_phase == "pre":
                unpivot_sql = self._build_unpivot_sql(trans.unpivot, source_table=self.contract.dataset or "source")
                if not unpivot_sql:
                    continue
                logger.debug(f"Pre-Transform [Unpivot]: {unpivot_sql}")
                try:
                    current_lf = self._apply_sql_transformation(current_lf, unpivot_sql)
                    existing = set(current_lf.collect_schema().names())
                except Exception as e:
                    logger.warning(f"Pre-Transform [Unpivot] failed: {e}")
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
                        expr = (
                            pl.when(cond).then(pl.lit(value)) if expr is None else expr.when(cond).then(pl.lit(value))
                        )
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

        # Track known columns explicitly — LazyFrame.columns is unreliable
        # on ctx.execute() results in some Polars versions.
        existing_cols = set(current_lf.collect_schema().names())

        for trans in self.contract.transformations:
            trans_phase = (trans.phase or "post").lower()
            # Re-register both aliases so queries can use either table name
            ctx.register(tbl_name, current_lf)
            if tbl_name != "source":
                ctx.register("source", current_lf)

            if trans.sql and trans_phase != "pre":
                logger.debug(f"Post-Transform [SQL]: {trans.sql}")
                sql = trans.sql.replace("{dataset}", tbl_name).replace("{source}", tbl_name)
                sql = self._normalize_sql(sql)
                current_lf = self._apply_sql_transformation(current_lf, sql)
                continue
            if trans.rollup and trans_phase != "pre":
                rollup_sql = self._build_rollup_sql(trans.rollup, source_table=tbl_name)
                logger.debug(f"Post-Transform [Rollup]: {rollup_sql}")
                current_lf = self._apply_sql_transformation(current_lf, rollup_sql)
                continue

            if trans.pivot and trans_phase != "pre":
                pivot_sql = self._build_pivot_sql(trans.pivot, source_table=tbl_name)
                if not pivot_sql:
                    continue
                logger.debug(f"Post-Transform [Pivot]: {pivot_sql}")
                current_lf = self._apply_sql_transformation(current_lf, pivot_sql)
                continue

            if trans.unpivot and trans_phase != "pre":
                unpivot_sql = self._build_unpivot_sql(trans.unpivot, source_table=tbl_name)
                if not unpivot_sql:
                    continue
                logger.debug(f"Post-Transform [Unpivot]: {unpivot_sql}")
                current_lf = self._apply_sql_transformation(current_lf, unpivot_sql)
                continue

            if trans.derive:
                logger.debug(f"Post-Transform [Derive]: {trans.derive.field}")
                field_name = trans.derive.field
                derive_sql = self._normalize_sql(trans.derive.sql)
                if field_name in existing_cols:
                    _step_query = f"SELECT * EXCLUDE ({field_name}), ({derive_sql}) AS {field_name} FROM _step"
                else:
                    _step_query = f"SELECT *, ({derive_sql}) AS {field_name} FROM _step"
                # ── Root-cause fix: materialize → fresh SQLContext ───────────────────
                # Re-registering a ctx.execute() result into the SAME ctx creates a
                # self-referential lazy plan.  collect_schema() reports the column as
                # present but it is silently lost during later query planning.
                # Collecting to a concrete DataFrame + a fresh ctx breaks the cycle.
                _pre_derive_lf = current_lf
                _resolved = False
                try:
                    _snap_df = current_lf.collect()
                    _fresh = pl.SQLContext()
                    _fresh.register("_step", _snap_df)
                    _fresh.register("source", _snap_df)
                    if tbl_name not in ("_step", "source"):
                        _fresh.register(tbl_name, _snap_df)
                    self._register_links(_fresh)
                    current_lf = _fresh.execute(_step_query)
                    existing_cols = set(current_lf.collect_schema().names())
                    _resolved = True
                except Exception as e:
                    current_lf = _pre_derive_lf
                    logger.warning(f"Post-Transform [Derive] '{field_name}' Polars SQL failed ({e}); trying DuckDB")
                if not _resolved:
                    _dq = _step_query.replace("FROM _step", "FROM source")
                    try:
                        current_lf = self._apply_sql_transformation(current_lf, _dq)
                        existing_cols = set(current_lf.collect_schema().names())
                        _resolved = True
                    except Exception as e2:
                        current_lf = _pre_derive_lf
                        logger.warning(
                            f"Post-Transform [Derive] '{field_name}' DuckDB failed ({e2}); trying Polars expr"
                        )

                if not _resolved:
                    import re as _re2
                    import warnings

                    _expr_sql = derive_sql.strip()
                    _cm = _re2.match(
                        r"CAST\s*\(\s*([a-zA-Z_]\w*)\s+AS\s+([A-Z]+)\s*\)$",
                        _expr_sql,
                        _re2.IGNORECASE,
                    )
                    if _cm:
                        _dt = self._to_polars_dtype(_cm.group(2).lower())
                        if _dt is not None and _cm.group(1) in existing_cols:
                            try:
                                current_lf = current_lf.with_columns(
                                    pl.col(_cm.group(1)).cast(_dt, strict=False).alias(field_name)
                                )
                                existing_cols = set(current_lf.collect_schema().names())
                                _resolved = True
                            except Exception as e3:
                                logger.debug(f"Polars cast fallback: {e3}")
                    if not _resolved:
                        _em = _re2.match(
                            r"EXTRACT\s*\(\s*(YEAR|MONTH|DAY)\s+FROM\s+CAST\s*\(\s*([a-zA-Z_]\w*)\s+AS\s+\w+\s*\)\s*\)$",
                            _expr_sql,
                            _re2.IGNORECASE,
                        )
                        if _em and _em.group(2) in existing_cols:
                            try:
                                _de = pl.col(_em.group(2)).cast(pl.Date, strict=False)
                                _pe = {
                                    "YEAR": _de.dt.year(),
                                    "MONTH": _de.dt.month(),
                                    "DAY": _de.dt.day(),
                                }[_em.group(1).upper()]
                                current_lf = current_lf.with_columns(_pe.alias(field_name))
                                existing_cols = set(current_lf.collect_schema().names())
                                _resolved = True
                            except Exception as e4:
                                logger.debug(f"Polars extract fallback: {e4}")
                    if not _resolved:
                        warnings.warn(
                            f"[LakeLogic] Post-Transform Derive '{field_name}' FAILED all engines. SQL: {derive_sql}",
                            stacklevel=2,
                        )
                        logger.error(f"Post-Transform [Derive] '{field_name}' all engines failed")
            elif trans.bucket:
                logger.debug(f"Post-Transform [Bucket]: {trans.bucket.field}")
                field_name = trans.bucket.field
                sql = self._build_bucket_sql(trans.bucket, source_table=tbl_name)
                if sql:
                    # _build_bucket_sql returns "SELECT *, (CASE...) AS field FROM source"
                    # We need to inject EXCLUDE if the field exists
                    if field_name in existing_cols:
                        sql = sql.replace("SELECT *,", f"SELECT * EXCLUDE ({field_name}),")
                    current_lf = ctx.execute(sql)
                    existing_cols = set(current_lf.collect_schema().names())
            elif trans.date_diff:
                logger.debug(f"Post-Transform [DateDiff]: {trans.date_diff.field}")
                field_name = trans.date_diff.field
                sql = self._build_date_diff_sql(trans.date_diff, source_table=tbl_name)
                if sql:
                    if field_name in existing_cols:
                        sql = sql.replace("SELECT *,", f"SELECT * EXCLUDE ({field_name}),")
                    current_lf = ctx.execute(sql)
                    existing_cols = set(current_lf.collect_schema().names())
            elif trans.json_extract:
                cfg = trans.json_extract
                logger.debug(f"Post-Transform [JsonExtract]: {cfg.source} -> {cfg.field} via {cfg.path}")
                extracted = pl.col(cfg.source).str.json_path_match(cfg.path)
                if cfg.cast:
                    dtype = self._to_polars_dtype(cfg.cast) or pl.Utf8
                    extracted = extracted.cast(dtype, strict=False)
                current_lf = current_lf.with_columns(extracted.alias(cfg.field))
            elif trans.date_range_explode:
                cfg = trans.date_range_explode
                logger.debug(f"Post-Transform [DateRangeExplode]: {cfg.start_col} -> {cfg.end_col} => {cfg.output}")
                # Collect to apply row-wise date_range, then re-wrap as LazyFrame
                df = current_lf.collect()
                import datetime as _dt

                def _make_dates(start_val, end_val):
                    try:
                        if isinstance(start_val, str):
                            start_val = _dt.date.fromisoformat(start_val[:10])
                        elif hasattr(start_val, "date"):
                            start_val = start_val.date()
                        if end_val is None:
                            end_val = _dt.date.today()
                        elif isinstance(end_val, str):
                            end_val = _dt.date.fromisoformat(end_val[:10])
                        elif hasattr(end_val, "date"):
                            end_val = end_val.date()
                        # Clamp: don't explode beyond today
                        end_val = min(end_val, _dt.date.today())
                        if start_val > end_val:
                            return [start_val]
                        return pl.date_range(
                            start=start_val,
                            end=end_val,
                            interval=cfg.interval,
                            eager=True,
                        ).to_list()
                    except Exception:
                        return [None]

                end_col = cfg.end_col
                if end_col and end_col in df.columns:
                    date_series = [_make_dates(r[cfg.start_col], r[end_col]) for r in df.iter_rows(named=True)]
                else:
                    date_series = [_make_dates(r[cfg.start_col], None) for r in df.iter_rows(named=True)]

                df = df.with_columns(pl.Series(name=cfg.output, values=date_series)).explode(cfg.output)
                current_lf = df.lazy()
                # Re-register in ctx after structural change
                ctx.register(tbl_name, current_lf)
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
                if filter_cfg and trans_phase != "pre":
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
        SELECT {", ".join(select_fields)}
        FROM {tbl_name} src
        {join_type} JOIN {join_cfg.reference} ref ON src.{join_cfg.on} = ref.{join_cfg.key}
        """
