from __future__ import annotations

import os
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from lakelogic.engines.base import EngineAdapter


_ENV_PATTERN = re.compile(r"^\${ENV:([A-Z0-9_]+)}$")


def _resolve_env_value(value: Optional[str]) -> Optional[str]:
    """
    Resolve simple environment variable placeholders.

    Args:
        value: Raw value or env placeholder (env:VAR or ${ENV:VAR}).

    Returns:
        Resolved value or None.
    """
    if value is None:
        return None
    if value.startswith("env:"):
        return os.getenv(value[4:].strip())
    match = _ENV_PATTERN.match(value)
    if match:
        return os.getenv(match.group(1))
    return value


class BigQueryAdapter(EngineAdapter):
    """
    BigQuery execution engine for LakeLogic.

    This adapter executes contracts directly in BigQuery using SQL.
    """

    def execute(self, df: Any) -> Tuple[Any, Any]:
        """
        Execute the contract using BigQuery SQL.

        Args:
            df: Source table name (string) or None.

        Returns:
            Tuple of (good_df, bad_df) as pandas DataFrames.
        """
        self.dataset_rule_results = []
        self.schema_drift = {}

        table_name = self._resolve_source_table(df)
        if not table_name:
            raise ValueError("BigQuery adapter requires a source table name.")

        client = self._get_client()
        self._session_id = None
        self._start_session(client)
        self._register_links(client)

        current = table_name
        current = self._apply_transformations(client, current, phase="pre")
        current, schema_errors = self._apply_schema(client, current)

        eval_table = self._apply_row_rules(client, current, schema_errors)
        good_table, bad_table = self._split_good_bad(client, eval_table)

        self._run_dataset_rules(client, good_table)
        good_table = self._apply_transformations(client, good_table, phase="post")

        good_df = self._fetch_dataframe(client, good_table)
        bad_df = self._fetch_dataframe(client, bad_table)

        include_errors = True
        if self.contract.quarantine:
            include_errors = self.contract.quarantine.include_error_reason
        if not include_errors and bad_df is not None:
            bad_df = bad_df.drop(columns=[self.ERROR_COLUMN, self.CATEGORY_COLUMN], errors="ignore")

        return good_df, bad_df

    def _get_client(self):
        """
        Create a BigQuery client.

        Returns:
            google.cloud.bigquery.Client
        """
        try:
            from lakelogic.core.deps import require

            require("google.cloud.bigquery", extra="cloud")
            from google.cloud import bigquery
        except Exception as exc:
            raise ValueError("BigQuery adapter requires google-cloud-bigquery.") from exc

        metadata = self.contract.metadata or {}
        project = _resolve_env_value(
            metadata.get("bigquery_project") or os.getenv("BIGQUERY_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
        )
        # Job location must match the datasets' location (e.g. EU) — else BigQuery runs
        # the query in the default US region and cannot find EU tables.
        location = _resolve_env_value(
            metadata.get("bigquery_location") or os.getenv("BIGQUERY_LOCATION") or os.getenv("BQ_LOCATION")
        )
        return bigquery.Client(project=project, location=location or None)

    def _resolve_source_table(self, df: Any) -> Optional[str]:
        """
        Resolve the source table name.

        Args:
            df: Input object.

        Returns:
            Table name string.
        """
        if isinstance(df, str):
            return df[6:] if df.startswith("table:") else df
        metadata = self.contract.metadata or {}
        return metadata.get("bigquery_source_table") or metadata.get("source_table")

    def _register_links(self, client) -> None:
        """
        Register link tables as temp tables in BigQuery.

        Args:
            client: BigQuery client.
        """
        from lakelogic.engines.base import assert_link_subset_supported

        assert_link_subset_supported(self.contract.links, "bigquery")
        for link in self.contract.links:
            table_name = None
            if link.table:
                table_name = link.table
            elif link.path and link.path.startswith("table:"):
                table_name = link.path[6:]
            elif link.type and link.type.lower() == "table" and link.path:
                table_name = link.path

            if not table_name:
                if link.path:
                    logger.warning(f"BigQuery link '{link.name}' uses file path '{link.path}'. Table links only.")
                continue

            col_clause = ", ".join(link.columns) if link.columns else "*"
            sql = (
                f"CREATE OR REPLACE TEMP TABLE {link.name} AS SELECT {col_clause} FROM {self._quote_table(table_name)}"
            )
            self._execute(client, sql)

    def _temp_name(self, label: str) -> str:
        """
        Create a unique temp table name.

        Args:
            label: Label suffix.

        Returns:
            Temp table name.
        """
        token = uuid.uuid4().hex[:8]
        return f"lg_{label}_{token}"

    def _start_session(self, client) -> None:
        """Open a BigQuery session so CREATE TEMP TABLE is legal and shared.

        A standalone query job cannot use CREATE TEMPORARY TABLE — BigQuery
        requires a script or session. Each contract statement runs as its own
        query job, so we open one session per contract run and bind every
        statement to it (see ``_job_config``) so temp tables created by earlier
        statements are visible to later ones.
        """
        from google.cloud import bigquery

        job = client.query("SELECT 1", job_config=bigquery.QueryJobConfig(create_session=True))
        job.result()
        self._session_id = job.session_info.session_id if job.session_info else None

    def _job_config(self):
        """Return a QueryJobConfig bound to the active session (or None)."""
        session_id = getattr(self, "_session_id", None)
        if not session_id:
            return None
        from google.cloud import bigquery

        return bigquery.QueryJobConfig(connection_properties=[bigquery.ConnectionProperty("session_id", session_id)])

    def _execute(self, client, sql: str) -> None:
        """
        Execute a SQL statement in BigQuery.

        Args:
            client: BigQuery client.
            sql: SQL statement.
        """
        job = client.query(sql, job_config=self._job_config())
        job.result()

    def _quote_ident(self, name: str) -> str:
        """
        Quote an identifier for BigQuery SQL.

        Args:
            name: Raw identifier.

        Returns:
            Quoted identifier.
        """
        text = str(name)
        if text.startswith("`") and text.endswith("`"):
            return text
        escaped = text.replace("`", "\\`")
        return f"`{escaped}`"

    def _quote_table(self, name: str) -> str:
        """Backtick-quote a (possibly project-qualified) table reference.

        BigQuery requires backticks around identifiers containing '-', which includes the
        vast majority of project ids (e.g. `my-project-123.dataset.table`). Wrapping the
        whole `project.dataset.table` path in a single backtick pair is valid BigQuery.
        """
        text = str(name).strip()
        if text.startswith("`") and text.endswith("`"):
            return text
        return f"`{text}`"

    # Warehouse/ANSI type keywords → BigQuery types. Contracts share one raw SQL
    # transformation across engines; Snowflake/Spark accept DOUBLE/FLOAT/BIGINT, but
    # BigQuery only knows FLOAT64/INT64/NUMERIC/STRING/BOOL. Normalising the CAST
    # target keeps the contract identical across all platforms.
    _SQL_TYPE_MAP = {
        "DOUBLE PRECISION": "FLOAT64",
        "DOUBLE": "FLOAT64",
        "FLOAT": "FLOAT64",
        "REAL": "FLOAT64",
        "BIGINT": "INT64",
        "INTEGER": "INT64",
        "INT": "INT64",
        "SMALLINT": "INT64",
        "TINYINT": "INT64",
        "NUMBER": "NUMERIC",
        "DECIMAL": "NUMERIC",
        "VARCHAR": "STRING",
        "NVARCHAR": "STRING",
        "CHAR": "STRING",
        "TEXT": "STRING",
        "BOOLEAN": "BOOL",
    }

    def _normalize_sql_types(self, sql: str) -> str:
        """Rewrite CAST target types in raw transformation SQL to BigQuery types.

        Scoped to the CAST-close position ``AS <type>)`` (optionally with whitespace)
        so it never rewrites a column alias like ``SUM(x) AS total`` — an alias is not
        immediately followed by a closing paren.
        """
        if not sql:
            return sql
        import re

        alternation = "|".join(sorted((k.replace(" ", r"\s+") for k in self._SQL_TYPE_MAP), key=len, reverse=True))

        def _repl(m):
            key = re.sub(r"\s+", " ", m.group(1).upper())
            return f"AS {self._SQL_TYPE_MAP.get(key, m.group(1))})"

        return re.sub(rf"\bAS\s+({alternation})\s*\)", _repl, sql, flags=re.IGNORECASE)

    @staticmethod
    def _split_top_level(s: str) -> List[str]:
        """Split a function argument list on top-level commas (respecting nested
        parens and string literals)."""
        args: List[str] = []
        depth = 0
        in_str = None
        buf: List[str] = []
        for ch in s:
            if in_str:
                buf.append(ch)
                if ch == in_str:
                    in_str = None
                continue
            if ch in ("'", '"'):
                in_str = ch
                buf.append(ch)
            elif ch == "(":
                depth += 1
                buf.append(ch)
            elif ch == ")":
                depth -= 1
                buf.append(ch)
            elif ch == "," and depth == 0:
                args.append("".join(buf))
                buf = []
            else:
                buf.append(ch)
        if buf:
            args.append("".join(buf))
        return args

    def _rewrite_call(self, sql: str, fname: str, builder) -> str:
        """Rewrite every ``fname(...)`` call via ``builder(args)`` (balanced-paren,
        nesting-aware). ``builder`` returns the replacement string, or None to leave
        the call untouched."""
        import re

        out = sql
        pos = 0
        while True:
            m = re.search(rf"\b{fname}\s*\(", out[pos:], flags=re.IGNORECASE)
            if not m:
                break
            start = pos + m.start()
            open_paren = pos + m.end() - 1
            depth = 0
            i = open_paren
            while i < len(out):
                if out[i] == "(":
                    depth += 1
                elif out[i] == ")":
                    depth -= 1
                    if depth == 0:
                        break
                i += 1
            args = [a.strip() for a in self._split_top_level(out[open_paren + 1 : i])]
            rep = builder(args)
            if rep is None:
                pos = i + 1
                continue
            out = out[:start] + rep + out[i + 1 :]
            pos = start + len(rep)
        return out

    def _rewrite_functions(self, sql: str) -> str:
        """Translate warehouse/Spark SQL that BigQuery lacks. Contracts share one raw
        SQL string across engines, so the translation lives here, not in the contract.

        - ``CONCAT_WS(sep, a, …)`` → ``ARRAY_TO_STRING([a, …], sep)`` (both skip NULLs).
        - ``TO_DATE(x)`` → ``DATE(CAST(x AS TIMESTAMP))`` (BigQuery has no TO_DATE;
          robust for string dates/datetimes, timestamps and dates alike).
        - bare ``UNION`` → ``UNION DISTINCT`` (BigQuery requires an explicit quantifier).
        """
        if not sql:
            return sql
        import re

        out = self._rewrite_call(
            sql,
            "CONCAT_WS",
            lambda a: f"ARRAY_TO_STRING([{', '.join(a[1:])}], {a[0]})" if len(a) >= 2 else None,
        )
        out = self._rewrite_call(
            out,
            "TO_DATE",
            lambda a: f"DATE(CAST({a[0]} AS TIMESTAMP))" if len(a) == 1 else None,
        )
        out = re.sub(r"\bUNION\b(?!\s+(?:ALL|DISTINCT)\b)", "UNION DISTINCT", out, flags=re.IGNORECASE)
        return out

    @staticmethod
    def _escape_str(text: str) -> str:
        """Escape a Python string for a single-quoted BigQuery string literal.

        BigQuery uses backslash escaping; ANSI quote-doubling ('') is parsed as two
        adjacent (concatenated) string literals — a syntax error — so a rule message
        like ``valid_email (EMAIL LIKE '%@%')`` must escape the quotes as ``\\'``.
        """
        return str(text).replace("\\", "\\\\").replace("'", "\\'")

    def _qualify(self, alias: str, name: str) -> str:
        """Build a qualified identifier (alias + column)."""
        return f"{alias}.{self._quote_ident(name)}"

    def _create_source_alias(self, client, table_name: str) -> None:
        """
        Create/replace the source table for SQL transformations.

        Args:
            client: BigQuery client.
            table_name: Table to expose as `source`.
        """
        self._execute(client, f"CREATE OR REPLACE TEMP TABLE source AS SELECT * FROM {self._quote_table(table_name)}")
        if self.contract.dataset:
            self._execute(
                client,
                f"CREATE OR REPLACE TEMP TABLE {self.contract.dataset} AS SELECT * FROM {self._quote_table(table_name)}",
            )

    def _apply_transformations(self, client, table_name: str, phase: str) -> str:
        """
        Apply contract transformations for the given phase.

        Args:
            client: BigQuery client.
            table_name: Current table name.
            phase: "pre" or "post".

        Returns:
            New table name after transformations.
        """
        current = table_name
        idx = 0
        for trans in self.contract.transformations:
            trans_phase = (trans.phase or "post").lower()
            if phase == "pre" and trans_phase != "pre":
                continue
            if phase == "post" and trans_phase == "pre":
                continue

            if trans.sql:
                self._create_source_alias(client, current)
                step = self._temp_name(f"{phase}_sql_{idx}")
                self._execute(
                    client,
                    f"CREATE OR REPLACE TEMP TABLE {step} AS {self._rewrite_functions(self._normalize_sql_types(trans.sql))}",
                )
                current = step
                idx += 1
                continue

            columns = self._get_columns(client, current)
            sql = self._structured_to_sql(trans, columns)
            if sql:
                self._create_source_alias(client, current)
                step = self._temp_name(f"{phase}_struct_{idx}")
                self._execute(client, f"CREATE OR REPLACE TEMP TABLE {step} AS {sql}")
                current = step
                idx += 1

        return current

    def _structured_to_sql(self, trans, columns: Optional[List[str]] = None) -> Optional[str]:
        """
        Convert a structured transformation into SQL.

        Args:
            trans: Transformation object.
            columns: Current column names.

        Returns:
            SQL string or None.
        """
        columns = columns or []

        def select_with_replacements(replacements: Dict[str, str], extra_exprs: Optional[List[str]] = None) -> str:
            exprs: List[str] = []
            for col in columns:
                qcol = self._quote_ident(col)
                if col in replacements:
                    exprs.append(f"{replacements[col]} AS {qcol}")
                else:
                    exprs.append(qcol)
            if extra_exprs:
                exprs.extend(extra_exprs)
            return f"SELECT {', '.join(exprs)} FROM source"

        if trans.rename:
            if not columns and self.contract.model:
                columns = [f.name for f in self.contract.model.fields]
            if not columns:
                logger.warning("Rename transformation skipped; could not resolve columns.")
                return None
            rename_pairs = trans.rename.iter_pairs()
            if not rename_pairs:
                return None
            rename_map = dict(rename_pairs)
            for src in rename_map:
                if src not in columns:
                    logger.warning(f"Rename transformation skipped; column not found: {src}")
            dest_set = set(rename_map.values())
            select_exprs: List[str] = []
            for col in columns:
                if col in rename_map:
                    select_exprs.append(f"{self._quote_ident(col)} AS {self._quote_ident(rename_map[col])}")
                elif col in dest_set and col not in rename_map:
                    continue
                else:
                    select_exprs.append(self._quote_ident(col))
            return f"SELECT {', '.join(select_exprs)} FROM source"

        if trans.select:
            return f"SELECT {', '.join(self._quote_ident(col) for col in trans.select.columns)} FROM source"

        if trans.drop:
            if not columns:
                logger.warning("Drop transformation skipped; could not resolve columns.")
                return None
            keep = [self._quote_ident(col) for col in columns if col not in set(trans.drop.columns)]
            if not keep:
                logger.warning("Drop transformation skipped; empty column set.")
                return None
            return f"SELECT {', '.join(keep)} FROM source"

        if trans.cast:
            replacements: Dict[str, str] = {}
            for col, dtype in trans.cast.columns.items():
                target_type = self._to_bigquery_type(dtype)
                replacements[col] = f"SAFE_CAST({self._quote_ident(col)} AS {target_type})"
            return select_with_replacements(replacements)

        if trans.trim:
            replacements = {}
            for field in trans.trim.fields:
                qfield = self._quote_ident(field)
                if trans.trim.side == "left":
                    replacements[field] = f"LTRIM({qfield})"
                elif trans.trim.side == "right":
                    replacements[field] = f"RTRIM({qfield})"
                else:
                    replacements[field] = f"TRIM({qfield})"
            return select_with_replacements(replacements)

        if trans.lower:
            replacements = {field: f"LOWER({self._quote_ident(field)})" for field in trans.lower.fields}
            return select_with_replacements(replacements)

        if trans.upper:
            replacements = {field: f"UPPER({self._quote_ident(field)})" for field in trans.upper.fields}
            return select_with_replacements(replacements)

        if trans.coalesce:
            sources = trans.coalesce.sources or []
            if not sources:
                sources = [trans.coalesce.field]
            parts = sources[:]
            if trans.coalesce.default is not None:
                parts.append(self._format_literal(trans.coalesce.default))
            sources_q = ", ".join(self._quote_ident(part) for part in sources)
            default_part = (
                ", " + self._format_literal(trans.coalesce.default) if trans.coalesce.default is not None else ""
            )
            expr = f"COALESCE({sources_q}{default_part})"
            output = trans.coalesce.output or trans.coalesce.field
            extra_exprs = [f"{expr} AS {self._quote_ident(output)}"]
            replacements: Dict[str, str] = {}
            if output in columns:
                replacements[output] = expr
                extra_exprs = None
            return select_with_replacements(replacements, extra_exprs)

        if trans.split:
            output = trans.split.output or trans.split.field
            expr = f"SPLIT({self._quote_ident(trans.split.field)}, {self._format_literal(trans.split.delimiter)})"
            replacements: Dict[str, str] = {}
            extra_exprs = [f"{expr} AS {self._quote_ident(output)}"]
            if output in columns:
                replacements[output] = expr
                extra_exprs = None
            return select_with_replacements(replacements, extra_exprs)

        if trans.explode:
            output = trans.explode.output or trans.explode.field
            if not columns:
                logger.warning("Explode transformation skipped; could not resolve columns.")
                return None
            select_cols = [f"src.{self._quote_ident(col)}" for col in columns if col != output]
            if output == trans.explode.field:
                select_cols = [f"src.{self._quote_ident(col)}" for col in columns if col != trans.explode.field]
            select_exprs = select_cols + [f"{self._quote_ident(output)}"]
            return (
                f"SELECT {', '.join(select_exprs)} "
                f"FROM source src, UNNEST(src.{self._quote_ident(trans.explode.field)}) AS {self._quote_ident(output)}"
            )

        if trans.map_values:
            field = trans.map_values.field
            qfield = self._quote_ident(field)
            mapping = trans.map_values.mapping or {}
            if not mapping:
                return None
            cases = []
            for key, value in mapping.items():
                cases.append(f"WHEN {qfield} = {self._format_literal(key)} THEN {self._format_literal(value)}")
            default_expr = (
                self._format_literal(trans.map_values.default) if trans.map_values.default is not None else qfield
            )
            case_expr = f"CASE {' '.join(cases)} ELSE {default_expr} END"
            output = trans.map_values.output or field
            replacements = {}
            extra_exprs = [f"{case_expr} AS {self._quote_ident(output)}"]
            if output in columns:
                replacements[output] = case_expr
                extra_exprs = None
            return select_with_replacements(replacements, extra_exprs)

        if trans.pivot:
            return self._build_pivot_sql(trans.pivot, source_table="source")

        if trans.unpivot:
            return self._build_unpivot_sql(trans.unpivot, source_table="source")

        if trans.filter:
            return f"SELECT * FROM source WHERE {trans.filter.sql}"

        if trans.deduplicate:
            on_cols = ", ".join(self._quote_ident(col) for col in trans.deduplicate.on)
            order_clause = ""
            if trans.deduplicate.sort_by:
                cols = ", ".join(self._quote_ident(col) for col in trans.deduplicate.sort_by)
                order_clause = f"ORDER BY {cols} {trans.deduplicate.order}"
            return f"""
            SELECT * FROM (
              SELECT *, ROW_NUMBER() OVER(PARTITION BY {on_cols} {order_clause}) AS _rn
              FROM source
            ) WHERE _rn = 1
            """

        if trans.derive:
            return f"SELECT *, ({trans.derive.sql}) AS {self._quote_ident(trans.derive.field)} FROM source"

        if trans.lookup:
            value_expr = f"ref.{self._quote_ident(trans.lookup.value)}"
            if trans.lookup.default_value is not None:
                default_val = self._format_literal(trans.lookup.default_value)
                value_expr = f"COALESCE(ref.{self._quote_ident(trans.lookup.value)}, {default_val})"
            ref_q = self._quote_ident(trans.lookup.reference)
            on_q = self._quote_ident(trans.lookup.on)
            key_q = self._quote_ident(trans.lookup.key)
            return f"""
            SELECT src.*, {value_expr} AS {self._quote_ident(trans.lookup.field)}
            FROM source src
            LEFT JOIN {ref_q} ref ON src.{on_q} = ref.{key_q}
            """

        if trans.join:
            join_type = (trans.join.type or "left").upper()
            if join_type == "FULL":
                join_type = "FULL OUTER"
            select_fields = ["src.*"]
            for field in trans.join.fields:
                alias = f"{trans.join.prefix}{field}" if trans.join.prefix else field
                default = trans.join.defaults.get(field) if trans.join.defaults else None
                if default is not None:
                    coalesce_val = self._format_literal(default)
                    expr = f"COALESCE(ref.{self._quote_ident(field)}, {coalesce_val}) AS {self._quote_ident(alias)}"
                else:
                    expr = f"ref.{self._quote_ident(field)} AS {self._quote_ident(alias)}"
                select_fields.append(expr)
            ref_q2 = self._quote_ident(trans.join.reference)
            on_q2 = self._quote_ident(trans.join.on)
            key_q2 = self._quote_ident(trans.join.key)
            return f"""
            SELECT {", ".join(select_fields)}
            FROM source src
            {join_type} JOIN {ref_q2} ref ON src.{on_q2} = ref.{key_q2}
            """

        return None

    def _get_columns(self, client, table_name: str) -> List[str]:
        """
        Fetch column names for a table.

        Args:
            client: BigQuery client.
            table_name: Table name.

        Returns:
            List of column names.
        """
        job = client.query(f"SELECT * FROM {self._quote_table(table_name)} LIMIT 0", job_config=self._job_config())
        result = job.result()
        return [field.name for field in result.schema]

    def _to_bigquery_type(self, type_name: str) -> str:
        """
        Map contract types to BigQuery SQL types.

        Args:
            type_name: Logical type name.

        Returns:
            BigQuery SQL type.
        """
        type_name = (type_name or "").lower().strip()
        mapping = {
            "string": "STRING",
            "varchar": "STRING",
            "text": "STRING",
            "int": "INT64",
            "integer": "INT64",
            "long": "INT64",
            "bigint": "INT64",
            "float": "FLOAT64",
            "double": "FLOAT64",
            "decimal": "FLOAT64",
            "bool": "BOOL",
            "boolean": "BOOL",
            "date": "DATE",
            "timestamp": "TIMESTAMP",
            "datetime": "TIMESTAMP",
        }
        return mapping.get(type_name, "STRING")

    def _apply_schema(self, client, table_name: str) -> Tuple[str, List[str]]:
        """
        Apply schema casts and drift handling.

        Args:
            client: BigQuery client.
            table_name: Current table.

        Returns:
            Tuple of (schema_table, schema_errors).
        """
        if not self.contract.model or not self.contract.model.fields:
            return table_name, []

        existing_cols = self._get_columns(client, table_name)
        # BigQuery preserves the source's column case (e.g. lowercase CSV headers),
        # while contracts declare fields in the platform-neutral canonical case
        # (often UPPERCASE, matching Snowflake). Match case-insensitively so a
        # `cancel_reason_code` source column resolves to a `CANCEL_REASON_CODE`
        # field instead of being treated as both missing AND unknown (which also
        # collides — BigQuery column names are case-insensitive).
        existing_by_lower = {c.lower(): c for c in existing_cols}
        expected_fields = [f.name for f in self.contract.model.fields]
        expected_lower = {name.lower() for name in expected_fields}

        missing = {f for f in expected_fields if f.lower() not in existing_by_lower}
        unknown = {c for c in existing_cols if c.lower() not in expected_lower}
        system_cols = {c for c in unknown if c.startswith("_lakelogic_")}
        unknown = unknown - system_cols - self._lineage_columns()

        server = self.contract.server
        from lakelogic.core.models import SchemaPolicy as _SP

        _sp_defaults = _SP()
        evolution = _sp_defaults.evolution
        policy = _sp_defaults.unknown_fields
        cast_to_string = False

        if server and server.mode == "ingest":
            cast_to_string = bool(server.cast_to_string)
            if server.schema_policy:
                evolution = (server.schema_policy.evolution or _sp_defaults.evolution).lower()
                policy = (server.schema_policy.unknown_fields or _sp_defaults.unknown_fields).lower()

        select_exprs = []
        for field in self.contract.model.fields:
            target_type = "STRING" if cast_to_string else self._to_bigquery_type(field.type)
            qfield = self._quote_ident(field.name)
            src = existing_by_lower.get(field.name.lower())
            if src is not None:
                # cast the real source column, alias it to the canonical field name
                qsrc = self._quote_ident(src)
                select_exprs.append(f"SAFE_CAST({qsrc} AS {target_type}) AS {qfield}")
            else:
                select_exprs.append(f"CAST(NULL AS {target_type}) AS {qfield}")

        if policy in ["allow", "quarantine"] and unknown:
            if cast_to_string:
                select_exprs.extend(
                    [
                        f"SAFE_CAST({self._quote_ident(col)} AS STRING) AS {self._quote_ident(col)}"
                        for col in sorted(unknown)
                    ]
                )
            else:
                select_exprs.extend([self._quote_ident(col) for col in sorted(unknown)])

        schema_table = self._temp_name("schema")
        self._execute(
            client,
            f"CREATE OR REPLACE TEMP TABLE {schema_table} AS SELECT {', '.join(select_exprs)} FROM {self._quote_table(table_name)}",
        )

        # ── Detect post-phase SQL transforms that reshape columns ────────────
        _has_post_sql = False
        if self.contract.transformations:
            for _t in self.contract.transformations:
                _phase = (getattr(_t, "phase", None) or "post").lower()
                if _phase == "post" and getattr(_t, "sql", None):
                    _has_post_sql = True
                    break

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

        return schema_table, schema_errors

    def _apply_row_rules(self, client, table_name: str, schema_errors: List[str]) -> str:
        """
        Apply row-level rules and produce error arrays.

        Args:
            client: BigQuery client.
            table_name: Current table.
            schema_errors: Schema error messages.

        Returns:
            Eval table name.
        """
        row_rules = self.get_row_rules()
        error_exprs = []
        category_exprs = []

        for err in schema_errors:
            safe = self._escape_str(err)
            error_exprs.append(f"IF(TRUE, '{safe}', NULL)")
            category_exprs.append("IF(TRUE, 'schema', NULL)")

        for rule in row_rules:
            err = self._escape_str(f"Rule failed: {rule.name} ({rule.sql})")
            cond = f"NOT COALESCE(({rule.sql}), FALSE)"
            error_exprs.append(f"IF({cond}, '{err}', NULL)")
            cat = self._escape_str(rule.category or "rule")
            category_exprs.append(f"IF({cond}, '{cat}', NULL)")

        error_array = (
            "ARRAY(SELECT err FROM UNNEST([" + ", ".join(error_exprs) + "]) AS err WHERE err IS NOT NULL)"
            if error_exprs
            else "[]"
        )
        category_array = (
            "ARRAY(SELECT err FROM UNNEST([" + ", ".join(category_exprs) + "]) AS err WHERE err IS NOT NULL)"
            if category_exprs
            else "[]"
        )

        eval_table = self._temp_name("eval")
        self._execute(
            client,
            f"""
            CREATE OR REPLACE TEMP TABLE {eval_table} AS
            SELECT *, {error_array} AS {self.ERROR_COLUMN}, {category_array} AS {self.CATEGORY_COLUMN}
            FROM {self._quote_table(table_name)}
            """,
        )
        return eval_table

    def _split_good_bad(self, client, eval_table: str) -> Tuple[str, str]:
        """
        Split eval table into good and bad tables.

        Args:
            client: BigQuery client.
            eval_table: Eval table name.

        Returns:
            Tuple of (good_table, bad_table).
        """
        good_table = self._temp_name("good")
        bad_table = self._temp_name("bad")

        self._execute(
            client,
            f"""
            CREATE OR REPLACE TEMP TABLE {bad_table} AS
            SELECT *, 'active' AS quarantine_state, FALSE AS quarantine_reprocessed
            FROM {eval_table}
            WHERE ARRAY_LENGTH({self.ERROR_COLUMN}) > 0
            """,
        )
        self._execute(
            client,
            f"""
            CREATE OR REPLACE TEMP TABLE {good_table} AS
            SELECT * EXCEPT({self.ERROR_COLUMN}, {self.CATEGORY_COLUMN})
            FROM {eval_table}
            WHERE ARRAY_LENGTH({self.ERROR_COLUMN}) = 0
            """,
        )
        return good_table, bad_table

    def _run_dataset_rules(self, client, table_name: str) -> None:
        """
        Execute dataset-level quality rules.

        Args:
            client: BigQuery client.
            table_name: Good table name.
        """
        rules = self.get_dataset_rules()
        if not rules:
            return

        self._execute(
            client,
            f"CREATE OR REPLACE TEMP TABLE {self.contract.dataset or 'source'} AS SELECT * FROM {table_name}",
        )

        for rule in rules:
            job = client.query(rule.sql, job_config=self._job_config())
            result = job.result()
            val = list(result)[0][0] if result.total_rows else None

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

            self.dataset_rule_results.append(
                {
                    "name": rule.name,
                    "value": f"{val} {expected}".strip(),
                    "passed": passed,
                    "description": rule.description,
                }
            )

    def _fetch_dataframe(self, client, table_name: str):
        """
        Fetch a table into a pandas DataFrame.

        Args:
            client: BigQuery client.
            table_name: Table name.

        Returns:
            pandas.DataFrame
        """
        job = client.query(f"SELECT * FROM {table_name}", job_config=self._job_config())
        return job.result().to_dataframe()
