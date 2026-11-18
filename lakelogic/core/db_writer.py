"""Native database writer: load data into a database its fastest way (``format: database``).

dlt writes files to disk, normalises them, loads a ``_staging`` schema and records the load in
its own tables - about 0.4-0.7 s per write however small the batch, and a merge rewrote every
row. A trading app loading bars for many symbols every few minutes measured 0.6-1.7 s per write
with dlt and 0.06-0.12 s with Polars -> ``COPY`` -> upsert (2026-11-18). This module is that
fast path. ``format: dlt`` stays as it was, for every database without an adapter here.

Contract keys (on ``materialization`` or ``quarantine``)::

    format: database
    connection: env:PG_URL            # a secret reference, never a literal URL with a password
    table: market_data.market_bars    # schema.table
    write:                            # all optional
      on_conflict: error              # append: error (default) | ignore (needs primary_key)
      only_changed: true              # merge: skip rows whose values have not changed
      batch_size: 100000              # rows per COPY chunk
      create_table: true              # create the table from the data/model when missing
      allow_empty_overwrite: false    # overwrite with 0 rows empties the table only when true

Every batch is ONE transaction: bulk load, apply, and (append) the retry-safety delete. On any
error it rolls back and raises - nothing is half-written. A merge updates a row only when a
business column changed, so re-sending unchanged rows rewrites nothing; LakeLogic's own
``_lakelogic_*`` columns are refreshed on a change but never count as one.

Retry safety for append: when a ``batch_key`` is given (the caller's id, or one derived from the
source files), the table carries ``_lakelogic_batch_key`` and a retried batch first deletes its
own earlier rows, so it is never doubled.
"""
from __future__ import annotations

import io
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlparse

from loguru import logger

BATCH_KEY_COLUMN = "_lakelogic_batch_key"
_WRITE_KEYS = {"on_conflict", "only_changed", "batch_size", "create_table", "allow_empty_overwrite"}
_STRATEGIES = {"append", "merge", "upsert", "overwrite", "replace"}


class DatabaseWriteError(ValueError):
    """A native database write that could not be done - message names the table and cause,
    with any secret scrubbed."""


def database_config(block: Any) -> Dict[str, Any]:
    """``connection`` / ``table`` / ``write`` of a ``format: database`` block, declared or extra."""
    if block is None:
        return {}
    data: Dict[str, Any] = {}
    extra = dict(getattr(block, "model_extra", None) or {})
    for key in ("connection", "table", "write"):
        value = getattr(block, key, None)
        if value is None:
            value = extra.get(key)
        if value is not None:
            data[key] = value.model_dump(exclude_none=True) if hasattr(value, "model_dump") else value
    write = dict(data.get("write") or {})
    unknown = sorted(set(write) - _WRITE_KEYS)
    if unknown:
        raise DatabaseWriteError(f"Unknown write option(s) {unknown}; allowed: {sorted(_WRITE_KEYS)}.")
    data["write"] = write
    return data


def dialect_of(url: str) -> str:
    """The database a connection URL names: ``postgresql``, ``duckdb``, ``sqlite``, …"""
    scheme = (urlparse(url).scheme or "").split("+", 1)[0].lower()
    return {"postgres": "postgresql", "postgresql": "postgresql", "duckdb": "duckdb", "sqlite": "sqlite",
            "mysql": "mysql", "mariadb": "mysql", "mssql": "mssql", "sqlserver": "mssql"}.get(scheme, scheme)


def _split_table(table: str) -> Tuple[Optional[str], str]:
    parts = [p.strip().strip('"') for p in str(table).split(".") if p.strip()]
    if not parts:
        raise DatabaseWriteError("format: database needs `table:` (schema.table).")
    return (parts[-2], parts[-1]) if len(parts) >= 2 else (None, parts[0])


def _to_polars(df: Any):
    """A Polars frame with nested columns as text (lists of strings joined, others JSON)."""
    import polars as pl

    from lakelogic.core.dlt_sink import nested_to_json, to_arrow

    if not isinstance(df, pl.DataFrame):
        df = pl.from_arrow(to_arrow(df))
    fixes = []
    for name, dtype in df.schema.items():
        if isinstance(dtype, pl.List) and dtype.inner == pl.String:
            fixes.append(pl.col(name).list.join("; ").alias(name))
    if fixes:
        df = df.with_columns(fixes)
    if any(isinstance(t, (pl.List, pl.Struct, pl.Array)) for t in df.schema.values()):
        df = pl.from_arrow(nested_to_json(df.to_arrow()))
    return df


# ── Postgres ────────────────────────────────────────────────────────────────────────────────── #


def _contract_type(dtype: Any) -> str:
    """The contract type a Polars column carries (physical -> contract, the registry's inverse)."""
    import polars as pl

    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
        return "bigint"
    if dtype == pl.Float32:
        return "float"
    if dtype == pl.Float64:
        return "double"
    if dtype == pl.Boolean:
        return "boolean"
    if dtype == pl.Date:
        return "date"
    if isinstance(dtype, pl.Datetime):
        return "timestamp_tz" if dtype.time_zone else "timestamp_ntz"
    if isinstance(dtype, pl.Decimal):
        return "decimal"
    if dtype == pl.Binary:
        return "binary"
    return "string"


def _ddl(dtype: Any, dialect: str, is_key: bool = False) -> str:
    """The column type to CREATE on ``dialect``, from the one type registry (core/types.py).
    Two physical limits the registry cannot know: a decimal keeps its own precision, and a key
    column cannot be an unbounded text type on MySQL / SQL Server."""
    import polars as pl

    from lakelogic.core import types as registry

    ctype = _contract_type(dtype)
    if ctype == "decimal" and isinstance(dtype, pl.Decimal):
        return f"NUMERIC({dtype.precision or 38},{dtype.scale or 0})"
    if ctype == "string" and is_key and dialect in ("mysql", "mssql"):
        return registry.ddl_type("varchar", dialect).replace("4000", "450")
    return registry.ddl_type(ctype, dialect)


class PostgresWriter:
    """Postgres through psycopg2: ``COPY`` (CSV) into the target or a staging table, then one
    ``INSERT … ON CONFLICT`` statement, all in one transaction."""

    dialect = "postgresql"
    strategies = {"append", "merge", "upsert", "overwrite", "replace"}

    def __init__(self, url: str, secrets: Sequence[str] = ()) -> None:
        self._url = url
        self._secrets = [s for s in secrets if s]
        self._conn = None
        self._known: Dict[str, List[str]] = {}  # table -> its columns, checked once per writer

    # -- connection --------------------------------------------------------------------- #
    def _connection(self):
        if self._conn is None or self._conn.closed:
            try:
                import psycopg2
            except ImportError as exc:
                raise DatabaseWriteError(
                    "format: database on Postgres needs psycopg2: pip install \"lakelogic[postgresql]\""
                ) from exc
            try:
                self._conn = psycopg2.connect(self._url)
            except Exception as exc:  # noqa: BLE001 - said plainly, secrets scrubbed
                raise DatabaseWriteError(f"Could not connect to Postgres: {self._scrub(exc)}") from None
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()

    def __del__(self) -> None:  # pragma: no cover - best effort
        try:
            self.close()
        except Exception:
            pass

    def _scrub(self, exc: Any) -> str:
        from lakelogic.core.dlt_sink import _scrub

        return _scrub(str(exc), self._secrets + [self._url])

    # -- table ---------------------------------------------------------------------------- #
    def _ensure_table(self, cur, schema: Optional[str], table: str, df, primary_key: Sequence[str],
                      create: bool, evolution: str) -> List[str]:
        from psycopg2 import sql

        key = f"{schema}.{table}"
        if key in self._known:
            existing = self._known[key]
        else:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (schema or "public", table),
            )
            existing = [r[0] for r in cur.fetchall()]
        ident = sql.Identifier(schema, table) if schema else sql.Identifier(table)
        if not existing:
            if not create:
                raise DatabaseWriteError(f"Table {key} does not exist and write.create_table is false.")
            if schema:
                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            cols = [sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(_ddl(t, "postgresql", c in primary_key))) for c, t in df.schema.items()]
            if primary_key:
                cols.append(sql.SQL("PRIMARY KEY ({})").format(sql.SQL(", ").join(map(sql.Identifier, primary_key))))
            cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(ident, sql.SQL(", ").join(cols)))
            existing = list(df.columns)
            logger.info(f"Created {key} ({len(existing)} columns)")
        else:
            new = [c for c in df.columns if c not in existing]
            if new and evolution == "strict":
                raise DatabaseWriteError(
                    f"{key} has no column(s) {new} and schema_policy.evolution is strict; add them or relax it."
                )
            for c in new:  # never dropped, never retyped
                cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                    ident, sql.Identifier(c), sql.SQL(_ddl(df.schema[c], "postgresql"))))
                existing.append(c)
                logger.info(f"{key}: added column {c}")
        self._known[key] = existing
        return existing

    def _copy(self, cur, target_sql, df, batch_size: int) -> None:
        """Stream ``df`` into ``target_sql`` with ``COPY … FROM STDIN`` in CSV chunks."""
        from psycopg2 import sql

        cols = sql.SQL(", ").join(map(sql.Identifier, df.columns))
        stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER false)").format(target_sql, cols)
        stmt_text = stmt.as_string(cur)
        for start in range(0, max(df.height, 1), batch_size):
            chunk = df.slice(start, batch_size)
            if chunk.height == 0:
                break
            buf = io.BytesIO()
            # Strings quoted, so an empty string stays "" and only a null is NULL.
            chunk.write_csv(buf, include_header=False, quote_style="non_numeric", datetime_format="%Y-%m-%dT%H:%M:%S%.f%z")
            buf.seek(0)
            cur.copy_expert(stmt_text, buf)

    # -- write ------------------------------------------------------------------------------ #
    def write(
        self,
        df: Any,
        *,
        table: str,
        strategy: str = "append",
        primary_key: Sequence[str] = (),
        write: Optional[Mapping[str, Any]] = None,
        batch_key: Optional[str] = None,
        evolution: str = "allow",
    ) -> Dict[str, Any]:
        from psycopg2 import sql

        opts = dict(write or {})
        strategy = (strategy or "append").lower()
        if strategy not in self.strategies:
            raise DatabaseWriteError(
                f"Strategy '{strategy}' is not supported by format: database on Postgres "
                f"(supported: {sorted(self.strategies)}). Use format: dlt for it."
            )
        pk = [c for c in (primary_key or [])]
        if strategy in ("merge", "upsert") and not pk:
            raise DatabaseWriteError("Strategy 'merge' needs the contract's primary_key.")
        on_conflict = str(opts.get("on_conflict") or "error").lower()
        if on_conflict == "ignore" and not pk:
            raise DatabaseWriteError("write.on_conflict: ignore needs the contract's primary_key.")
        batch_size = int(opts.get("batch_size") or 100_000)

        frame = _to_polars(df)
        if batch_key and strategy == "append" and not pk:
            import polars as pl

            frame = frame.with_columns(pl.lit(batch_key).alias(BATCH_KEY_COLUMN))
        missing_keys = [k for k in pk if k not in frame.columns]
        if missing_keys:
            raise DatabaseWriteError(f"Primary key column(s) {missing_keys} are not in the data.")
        if strategy in ("merge", "upsert") and frame.height:
            counts = frame.group_by(pk).len()
            bad = counts.filter(counts["len"] > 1)
            if bad.height:
                first = bad.row(0, named=True)
                shown = ", ".join(str(first[k]) for k in pk)
                raise DatabaseWriteError(
                    f"{bad.height} key(s) appear more than once in this batch, e.g. ({shown}) "
                    f"{first['len']} times; a merge needs one row per key (add a deduplicate step)."
                )
        if strategy in ("overwrite", "replace") and frame.height == 0 and not opts.get("allow_empty_overwrite"):
            raise DatabaseWriteError(
                f"Refusing to overwrite {table} with 0 rows (it would empty the table). "
                "Set write.allow_empty_overwrite: true if that is intended."
            )

        schema, name = _split_table(table)
        target = sql.Identifier(schema, name) if schema else sql.Identifier(name)
        conn = self._connection()
        result: Dict[str, Any] = {"target": f"postgres:{schema + '.' if schema else ''}{name}", "format": "database",
                                  "dialect": self.dialect, "strategy": strategy}
        try:
            with conn.cursor() as cur:
                self._ensure_table(cur, schema, name, frame, pk, bool(opts.get("create_table", True)), evolution)
                cols = list(frame.columns)
                col_list = sql.SQL(", ").join(map(sql.Identifier, cols))
                if strategy == "append" and on_conflict != "ignore":
                    if batch_key and not pk:
                        cur.execute(sql.SQL("DELETE FROM {} WHERE {} = %s").format(target, sql.Identifier(BATCH_KEY_COLUMN)),
                                    (batch_key,))
                        result["replaced_rows"] = cur.rowcount
                    self._copy(cur, target, frame, batch_size)
                    result.update(rows_written=frame.height, inserted=frame.height, updated=0)
                elif strategy in ("overwrite", "replace"):
                    cur.execute(sql.SQL("TRUNCATE {}").format(target))
                    self._copy(cur, target, frame, batch_size)
                    result.update(rows_written=frame.height, inserted=frame.height, updated=0)
                else:
                    stg = sql.Identifier("_ll_stg")
                    cur.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(stg, target))
                    self._copy(cur, stg, frame, batch_size)
                    conflict = sql.SQL(", ").join(map(sql.Identifier, pk))
                    if strategy == "append":  # on_conflict: ignore
                        cur.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO NOTHING").format(
                            target, col_list, col_list, stg, conflict))
                        result.update(rows_written=cur.rowcount, inserted=cur.rowcount, updated=0,
                                      skipped=frame.height - cur.rowcount)
                    else:
                        result.update(self._merge(cur, target, stg, cols, pk, bool(opts.get("only_changed", True))))
            conn.commit()
        except DatabaseWriteError:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001 - rolled back, said plainly, secrets scrubbed
            conn.rollback()
            raise DatabaseWriteError(f"Write to {result['target']} failed and was rolled back: {self._scrub(exc)}") from None
        logger.info(
            f"Wrote {result.get('rows_written', 0)} row(s) to {result['target']} ({strategy}"
            + (f": {result.get('inserted', 0)} inserted, {result.get('updated', 0)} updated" if strategy in ("merge", "upsert") else "")
            + ")"
        )
        return result

    def _merge(self, cur, target, stg, cols: List[str], pk: List[str], only_changed: bool) -> Dict[str, Any]:
        from psycopg2 import sql

        non_keys = [c for c in cols if c not in pk]
        business = [c for c in non_keys if not c.lower().startswith("_lakelogic_")]
        col_list = sql.SQL(", ").join(map(sql.Identifier, cols))
        conflict = sql.SQL(", ").join(map(sql.Identifier, pk))
        if non_keys:
            sets = sql.SQL(", ").join(sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in non_keys)
            action = sql.SQL("DO UPDATE SET {}").format(sets)
            if only_changed and business:
                left = sql.SQL(", ").join(sql.SQL("{}.{}").format(target, sql.Identifier(c)) for c in business)
                right = sql.SQL(", ").join(sql.SQL("EXCLUDED.{}").format(sql.Identifier(c)) for c in business)
                action = sql.SQL("{} WHERE ({}) IS DISTINCT FROM ({})").format(
                    action, sql.SQL("ROW({})").format(left), sql.SQL("ROW({})").format(right))
        else:
            action = sql.SQL("DO NOTHING")
        stmt = sql.SQL(
            "WITH up AS (INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) {} RETURNING (xmax = 0) AS ins) "
            "SELECT count(*) FILTER (WHERE ins), count(*) FILTER (WHERE NOT ins) FROM up"
        ).format(target, col_list, col_list, stg, conflict, action)
        cur.execute(stmt)
        inserted, updated = cur.fetchone()
        return {"rows_written": int(inserted) + int(updated), "inserted": int(inserted), "updated": int(updated)}


# ── DuckDB / SQLite ───────────────────────────────────────────────────────────────────────── #


def _q(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


class _EmbeddedWriter:
    """DuckDB and SQLite: insert from the frame, then ``INSERT … ON CONFLICT`` - the same
    strategies, options and one-transaction rule as Postgres. Mostly for local apps and tests."""

    strategies = {"append", "merge", "upsert", "overwrite", "replace"}

    def __init__(self, url: str, dialect: str) -> None:
        self.dialect = dialect
        self._path = re.sub(r"^[a-z0-9+]+:///?", "", url, flags=re.I) or ":memory:"
        self._conn = None

    def _connection(self):
        if self._conn is None:
            if self.dialect == "duckdb":
                import duckdb

                self._conn = duckdb.connect(self._path)
            else:
                import sqlite3

                self._conn = sqlite3.connect(self._path)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _exec(self, sql_text: str, params: Sequence[Any] = ()):
        return self._connection().execute(sql_text, list(params))

    def _columns(self, schema: Optional[str], table: str) -> List[str]:
        if self.dialect == "duckdb":
            rows = self._exec(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? "
                "ORDER BY ordinal_position", (schema or "main", table)).fetchall()
        else:
            rows = [(r[1],) for r in self._exec(f"PRAGMA table_info({_q(table)})").fetchall()]
        return [r[0] for r in rows]

    def _insert(self, target: str, frame) -> None:
        cols = ", ".join(_q(c) for c in frame.columns)
        if self.dialect == "duckdb":
            conn = self._connection()
            conn.register("_ll_src", frame.to_arrow())
            try:
                conn.execute(f"INSERT INTO {target} ({cols}) SELECT {cols} FROM _ll_src")
            finally:
                conn.unregister("_ll_src")
        else:
            marks = ", ".join("?" for _ in frame.columns)
            self._connection().executemany(f"INSERT INTO {target} ({cols}) VALUES ({marks})", frame.rows())

    def write(self, df: Any, *, table: str, strategy: str = "append", primary_key: Sequence[str] = (),
              write: Optional[Mapping[str, Any]] = None, batch_key: Optional[str] = None,
              evolution: str = "allow") -> Dict[str, Any]:
        import polars as pl

        opts = dict(write or {})
        strategy = (strategy or "append").lower()
        pk = list(primary_key or [])
        if strategy not in self.strategies:
            raise DatabaseWriteError(f"Strategy '{strategy}' is not supported by format: database on {self.dialect}.")
        if strategy in ("merge", "upsert") and not pk:
            raise DatabaseWriteError("Strategy 'merge' needs the contract's primary_key.")
        on_conflict = str(opts.get("on_conflict") or "error").lower()
        if on_conflict == "ignore" and not pk:
            raise DatabaseWriteError("write.on_conflict: ignore needs the contract's primary_key.")
        frame = _to_polars(df)
        if self.dialect == "sqlite":  # sqlite3 takes no dates/datetimes as values from Polars rows
            frame = frame.with_columns([pl.col(c).cast(pl.String) for c, t in frame.schema.items()
                                        if t in (pl.Date, pl.Time) or isinstance(t, pl.Datetime)])
        if batch_key and strategy == "append" and not pk:
            frame = frame.with_columns(pl.lit(batch_key).alias(BATCH_KEY_COLUMN))
        if strategy in ("merge", "upsert") and frame.height:
            counts = frame.group_by(pk).len()
            bad = counts.filter(counts["len"] > 1)
            if bad.height:
                first = bad.row(0, named=True)
                raise DatabaseWriteError(
                    f"{bad.height} key(s) appear more than once in this batch, e.g. "
                    f"({', '.join(str(first[k]) for k in pk)}); a merge needs one row per key.")
        if strategy in ("overwrite", "replace") and frame.height == 0 and not opts.get("allow_empty_overwrite"):
            raise DatabaseWriteError(f"Refusing to overwrite {table} with 0 rows (it would empty the table).")

        schema, name = _split_table(table)
        if self.dialect == "sqlite":
            schema = None
        target = f"{_q(schema)}.{_q(name)}" if schema else _q(name)
        conn = self._connection()
        result: Dict[str, Any] = {"target": f"{self.dialect}:{schema + '.' if schema else ''}{name}",
                                  "format": "database", "dialect": self.dialect, "strategy": strategy}
        try:
            self._exec("BEGIN")
            existing = self._columns(schema, name)
            if not existing:
                if not opts.get("create_table", True):
                    raise DatabaseWriteError(f"Table {table} does not exist and write.create_table is false.")
                if schema:
                    self._exec(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)}")
                cols = [f"{_q(c)} {_ddl(t, self.dialect, c in pk)}" for c, t in frame.schema.items()]
                if pk:
                    cols.append(f"PRIMARY KEY ({', '.join(_q(k) for k in pk)})")
                self._exec(f"CREATE TABLE {target} ({', '.join(cols)})")
            else:
                new = [c for c in frame.columns if c not in existing]
                if new and evolution == "strict":
                    raise DatabaseWriteError(f"{table} has no column(s) {new} and schema_policy.evolution is strict.")
                for c in new:
                    self._exec(f"ALTER TABLE {target} ADD COLUMN {_q(c)} {_ddl(frame.schema[c], self.dialect)}")
            cols = ", ".join(_q(c) for c in frame.columns)
            if strategy in ("overwrite", "replace"):
                self._exec(f"DELETE FROM {target}")
                self._insert(target, frame)
                result.update(rows_written=frame.height)
            elif strategy == "append" and on_conflict != "ignore":
                if batch_key and not pk:
                    self._exec(f"DELETE FROM {target} WHERE {_q(BATCH_KEY_COLUMN)} = ?", (batch_key,))
                self._insert(target, frame)
                result.update(rows_written=frame.height)
            else:
                stg = _q("_ll_stg")
                self._exec(f"CREATE TEMP TABLE {stg} AS SELECT * FROM {target} WHERE 1 = 0")
                self._insert(stg, frame)
                conflict = ", ".join(_q(k) for k in pk)
                if strategy == "append":
                    action = "DO NOTHING"
                else:
                    non_keys = [c for c in frame.columns if c not in pk]
                    business = [c for c in non_keys if not c.lower().startswith("_lakelogic_")]
                    sets = ", ".join(f"{_q(c)} = excluded.{_q(c)}" for c in non_keys)
                    action = f"DO UPDATE SET {sets}" if sets else "DO NOTHING"
                    if sets and business and opts.get("only_changed", True):
                        changed = " OR ".join(f"{target}.{_q(c)} IS DISTINCT FROM excluded.{_q(c)}" for c in business)
                        action += f" WHERE {changed}"
                where = " WHERE true" if self.dialect == "sqlite" else ""  # sqlite's upsert parse rule
                self._exec(f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {stg}{where} ON CONFLICT ({conflict}) {action}")
                self._exec(f"DROP TABLE {stg}")
                result.update(rows_written=frame.height)
            self._exec("COMMIT")
        except Exception as exc:
            try:
                self._exec("ROLLBACK")
            except Exception:  # noqa: BLE001
                pass
            if isinstance(exc, DatabaseWriteError):
                raise
            raise DatabaseWriteError(f"Write to {result['target']} failed and was rolled back: {exc}") from None
        return result


# ── MySQL / SQL Server ────────────────────────────────────────────────────────────────────── #


def _check_batch(frame, table: str, strategy: str, pk: List[str], opts: Mapping[str, Any]) -> None:
    """The refusals every adapter shares: merge needs a key, one row per key, no empty overwrite."""
    if strategy in ("merge", "upsert") and not pk:
        raise DatabaseWriteError("Strategy 'merge' needs the contract's primary_key.")
    if str(opts.get("on_conflict") or "error").lower() == "ignore" and not pk:
        raise DatabaseWriteError("write.on_conflict: ignore needs the contract's primary_key.")
    missing = [k for k in pk if k not in frame.columns]
    if missing:
        raise DatabaseWriteError(f"Primary key column(s) {missing} are not in the data.")
    if strategy in ("merge", "upsert") and frame.height:
        counts = frame.group_by(pk).len()
        bad = counts.filter(counts["len"] > 1)
        if bad.height:
            first = bad.row(0, named=True)
            raise DatabaseWriteError(
                f"{bad.height} key(s) appear more than once in this batch, e.g. "
                f"({', '.join(str(first[k]) for k in pk)}); a merge needs one row per key.")
    if strategy in ("overwrite", "replace") and frame.height == 0 and not opts.get("allow_empty_overwrite"):
        raise DatabaseWriteError(f"Refusing to overwrite {table} with 0 rows (it would empty the table).")


def _url_parts(url: str) -> Dict[str, Any]:
    from urllib.parse import parse_qs, unquote

    u = urlparse(url)
    return {"host": u.hostname or "localhost", "port": u.port, "user": unquote(u.username or ""),
            "password": unquote(u.password or ""), "database": (u.path or "/").lstrip("/") or None,
            "query": {k: v[-1] for k, v in parse_qs(u.query).items()}}


class _ServerWriter:
    """Shared flow for MySQL and SQL Server: create/evolve the table, then load and apply in one
    transaction. Subclasses supply the dialect's SQL."""

    strategies = {"append", "merge", "upsert", "overwrite", "replace"}
    dialect = "server"

    def __init__(self, url: str) -> None:
        self._url = url
        self._conn = None
        self._known: Dict[str, List[str]] = {}

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def _scrub(self, exc: Any) -> str:
        from lakelogic.core.dlt_sink import _scrub

        return _scrub(str(exc), [self._url, _url_parts(self._url)["password"]])

    def write(self, df: Any, *, table: str, strategy: str = "append", primary_key: Sequence[str] = (),
              write: Optional[Mapping[str, Any]] = None, batch_key: Optional[str] = None,
              evolution: str = "allow") -> Dict[str, Any]:
        import polars as pl

        opts = dict(write or {})
        strategy = (strategy or "append").lower()
        if strategy not in self.strategies:
            raise DatabaseWriteError(f"Strategy '{strategy}' is not supported by format: database on {self.dialect}.")
        pk = list(primary_key or [])
        frame = _to_polars(df)
        if batch_key and strategy == "append" and not pk:
            frame = frame.with_columns(pl.lit(batch_key).alias(BATCH_KEY_COLUMN))
        _check_batch(frame, table, strategy, pk, opts)
        schema, name = _split_table(table)
        result: Dict[str, Any] = {"target": f"{self.dialect}:{schema + '.' if schema else ''}{name}",
                                  "format": "database", "dialect": self.dialect, "strategy": strategy}
        try:
            conn = self._connection()
        except DatabaseWriteError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DatabaseWriteError(f"Could not connect to {self.dialect}: {self._scrub(exc)}") from None
        try:
            cur = conn.cursor()
            self._ensure(cur, schema, name, frame, pk, bool(opts.get("create_table", True)), evolution)
            conn.commit()  # DDL first: MySQL commits it implicitly anyway
            on_conflict = str(opts.get("on_conflict") or "error").lower()
            if strategy in ("overwrite", "replace"):
                cur.execute(f"DELETE FROM {self._t(schema, name)}")
                self._load(cur, self._t(schema, name), frame)
                result.update(rows_written=frame.height, inserted=frame.height, updated=0)
            elif strategy == "append" and on_conflict != "ignore":
                if batch_key and not pk:
                    cur.execute(f"DELETE FROM {self._t(schema, name)} WHERE {self._q(BATCH_KEY_COLUMN)} = {self._mark}",
                                (batch_key,))
                self._load(cur, self._t(schema, name), frame)
                result.update(rows_written=frame.height, inserted=frame.height, updated=0)
            else:
                result.update(self._apply(cur, schema, name, frame, pk, strategy,
                                          bool(opts.get("only_changed", True))))
            conn.commit()
        except DatabaseWriteError:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise DatabaseWriteError(f"Write to {result['target']} failed and was rolled back: {self._scrub(exc)}") from None
        logger.info(f"Wrote {result.get('rows_written', 0)} row(s) to {result['target']} ({strategy})")
        return result

    def _ensure(self, cur, schema, name, frame, pk, create: bool, evolution: str) -> None:
        key = f"{schema}.{name}"
        existing = self._known.get(key) or self._columns(cur, schema, name)
        if not existing:
            if not create:
                raise DatabaseWriteError(f"Table {key} does not exist and write.create_table is false.")
            self._create(cur, schema, name, frame, pk)
            existing = list(frame.columns)
        else:
            new = [c for c in frame.columns if c not in existing]
            if new and evolution == "strict":
                raise DatabaseWriteError(f"{key} has no column(s) {new} and schema_policy.evolution is strict.")
            for c in new:
                cur.execute(f"ALTER TABLE {self._t(schema, name)} ADD {self._q(c)} {_ddl(frame.schema[c], self.dialect)}")
                existing.append(c)
        self._known[key] = existing

    def _create(self, cur, schema, name, frame, pk) -> None:
        cols = [f"{self._q(c)} {_ddl(t, self.dialect, c in pk)}" + (" NOT NULL" if c in pk else "")
                for c, t in frame.schema.items()]
        if pk:
            cols.append(f"PRIMARY KEY ({', '.join(self._q(k) for k in pk)})")
        cur.execute(f"CREATE TABLE {self._t(schema, name)} ({', '.join(cols)})")
        logger.info(f"Created {schema}.{name}")

    def _rows(self, frame) -> List[tuple]:
        return frame.rows()


class MySQLWriter(_ServerWriter):
    dialect = "mysql"
    _mark = "%s"

    def _connection(self):
        if self._conn is None:
            try:
                import pymysql
            except ImportError as exc:
                raise DatabaseWriteError("format: database on MySQL needs pymysql: pip install \"lakelogic[mysql]\"") from exc
            p = _url_parts(self._url)
            self._conn = pymysql.connect(host=p["host"], port=p["port"] or 3306, user=p["user"], password=p["password"],
                                         database=p["database"], autocommit=False, charset="utf8mb4")
        return self._conn

    @staticmethod
    def _q(name: str) -> str:
        return "`" + str(name).replace("`", "``") + "`"

    def _t(self, schema, name) -> str:
        return f"{self._q(schema)}.{self._q(name)}" if schema else self._q(name)

    def _columns(self, cur, schema, name) -> List[str]:
        if schema:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {self._q(schema)}")
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = COALESCE(%s, DATABASE()) "
                    "AND table_name = %s ORDER BY ordinal_position", (schema, name))
        return [r[0] for r in cur.fetchall()]

    def _rows(self, frame) -> List[tuple]:
        import polars as pl

        tz = [c for c, t in frame.schema.items() if isinstance(t, pl.Datetime) and t.time_zone]
        if tz:  # MySQL DATETIME has no zone: store UTC
            frame = frame.with_columns([pl.col(c).dt.convert_time_zone("UTC").dt.replace_time_zone(None) for c in tz])
        return frame.rows()

    def _load(self, cur, target: str, frame) -> None:
        cols = ", ".join(self._q(c) for c in frame.columns)
        marks = ", ".join(["%s"] * len(frame.columns))
        cur.executemany(f"INSERT INTO {target} ({cols}) VALUES ({marks})", self._rows(frame))

    def _apply(self, cur, schema, name, frame, pk, strategy, only_changed) -> Dict[str, Any]:
        target = self._t(schema, name)
        stg = self._q("_ll_stg")
        cur.execute(f"CREATE TEMPORARY TABLE {stg} LIKE {target}")
        try:
            self._load(cur, stg, frame)
            cols = ", ".join(self._q(c) for c in frame.columns)
            on = " AND ".join(f"t.{self._q(k)} = s.{self._q(k)}" for k in pk)
            cur.execute(f"SELECT COUNT(*) FROM {stg} s WHERE NOT EXISTS (SELECT 1 FROM {target} t WHERE {on})")
            new_rows = int(cur.fetchone()[0])
            if strategy == "append":  # on_conflict: ignore
                cur.execute(f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {stg} s "
                            f"WHERE NOT EXISTS (SELECT 1 FROM {target} t WHERE {on})")
                return {"rows_written": new_rows, "inserted": new_rows, "updated": 0, "skipped": frame.height - new_rows}
            non_keys = [c for c in frame.columns if c not in pk]
            sets = ", ".join(f"{self._q(c)} = s_new.{self._q(c)}" for c in non_keys) or \
                ", ".join(f"{self._q(k)} = {self._q(k)}" for k in pk)
            # MySQL changes nothing (and counts 0) for a row whose values are the same.
            cur.execute(f"INSERT INTO {target} ({cols}) SELECT * FROM (SELECT {cols} FROM {stg}) AS s_new "
                        f"ON DUPLICATE KEY UPDATE {sets}")
            updated = max(0, (int(cur.rowcount) - new_rows) // 2)
            return {"rows_written": new_rows + updated, "inserted": new_rows, "updated": updated}
        finally:
            cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {stg}")


class SQLServerWriter(_ServerWriter):
    dialect = "mssql"
    _mark = "?"

    def _connection(self):
        if self._conn is None:
            try:
                import pyodbc
            except ImportError as exc:
                raise DatabaseWriteError("format: database on SQL Server needs pyodbc: pip install \"lakelogic[azuresql]\"") from exc
            p = _url_parts(self._url)
            q = p["query"]
            driver = q.get("driver") or next((d for d in reversed(pyodbc.drivers()) if "ODBC Driver" in d), "SQL Server")
            server = f"{p['host']},{p['port']}" if p["port"] else p["host"]
            parts = [f"DRIVER={{{driver}}}", f"SERVER={server}", f"DATABASE={p['database'] or 'master'}",
                     f"UID={p['user']}", f"PWD={p['password']}"]
            if str(q.get("TrustServerCertificate", q.get("trust_server_certificate", ""))).lower() in ("yes", "true", "1"):
                parts.append("TrustServerCertificate=yes")
            if q.get("Encrypt"):
                parts.append(f"Encrypt={q['Encrypt']}")
            self._conn = pyodbc.connect(";".join(parts), autocommit=False)
        return self._conn

    @staticmethod
    def _q(name: str) -> str:
        return "[" + str(name).replace("]", "]]") + "]"

    def _t(self, schema, name) -> str:
        return f"{self._q(schema or 'dbo')}.{self._q(name)}"

    def _columns(self, cur, schema, name) -> List[str]:
        s = schema or "dbo"
        cur.execute(f"IF SCHEMA_ID(?) IS NULL EXEC('CREATE SCHEMA {self._q(s)}')", (s,))
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? "
                    "ORDER BY ordinal_position", (s, name))
        return [r[0] for r in cur.fetchall()]

    def _load(self, cur, target: str, frame) -> None:
        if not frame.height:
            return
        cols = ", ".join(self._q(c) for c in frame.columns)
        marks = ", ".join(["?"] * len(frame.columns))
        cur.fast_executemany = True
        cur.executemany(f"INSERT INTO {target} ({cols}) VALUES ({marks})", self._rows(frame))

    def _apply(self, cur, schema, name, frame, pk, strategy, only_changed) -> Dict[str, Any]:
        target = self._t(schema, name)
        stg = "#ll_stg"
        cur.execute(f"IF OBJECT_ID('tempdb..{stg}') IS NOT NULL DROP TABLE {stg}")
        cur.execute(f"SELECT TOP 0 * INTO {stg} FROM {target}")
        try:
            self._load(cur, stg, frame)
            cols = [self._q(c) for c in frame.columns]
            on = " AND ".join(f"t.{self._q(k)} = s.{self._q(k)}" for k in pk)
            if strategy == "append":  # on_conflict: ignore
                cur.execute(f"INSERT INTO {target} ({', '.join(cols)}) SELECT {', '.join('s.' + c for c in cols)} "
                            f"FROM {stg} s WHERE NOT EXISTS (SELECT 1 FROM {target} t WITH (UPDLOCK, HOLDLOCK) WHERE {on})")
                n = int(cur.rowcount)
                return {"rows_written": n, "inserted": n, "updated": 0, "skipped": frame.height - n}
            non_keys = [c for c in frame.columns if c not in pk]
            business = [c for c in non_keys if not c.lower().startswith("_lakelogic_")]
            sets = ", ".join(f"t.{self._q(c)} = s.{self._q(c)}" for c in non_keys)
            # "Only changed" as EXCEPT: NULL-safe and on every SQL Server version.
            changed = (f" AND EXISTS (SELECT {', '.join('s.' + self._q(c) for c in business)} "
                       f"EXCEPT SELECT {', '.join('t.' + self._q(c) for c in business)})") if only_changed and business else ""
            matched = f"WHEN MATCHED{changed} THEN UPDATE SET {sets} " if sets else ""
            cur.execute(
                f"SET NOCOUNT ON; DECLARE @a TABLE (act NVARCHAR(10)); "
                f"MERGE {target} WITH (HOLDLOCK) AS t USING {stg} AS s ON {on} {matched}"
                f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)}) "
                f"OUTPUT $action INTO @a; "
                f"SELECT SUM(CASE WHEN act = 'INSERT' THEN 1 ELSE 0 END), SUM(CASE WHEN act = 'UPDATE' THEN 1 ELSE 0 END) FROM @a;"
            )
            ins, upd = cur.fetchone()
            ins, upd = int(ins or 0), int(upd or 0)
            return {"rows_written": ins + upd, "inserted": ins, "updated": upd}
        finally:
            cur.execute(f"IF OBJECT_ID('tempdb..{stg}') IS NOT NULL DROP TABLE {stg}")


# ── registry ──────────────────────────────────────────────────────────────────────────────── #

_WRITERS: Dict[str, Any] = {}


def writer_for(connection: Any) -> Any:
    """The writer for a ``connection`` (a secret reference or URL), reused per connection so a
    processor that writes many batches keeps one open connection."""
    from lakelogic.core.dlt_sink import resolve_credentials

    if not connection:
        raise DatabaseWriteError("format: database needs `connection:` (e.g. env:PG_URL).")
    url = resolve_credentials(connection)
    if not isinstance(url, str):
        raise DatabaseWriteError("format: database `connection` must resolve to a connection URL.")
    cached = _WRITERS.get(url)
    if cached is not None:
        return cached
    dialect = dialect_of(url)
    if dialect == "postgresql":
        writer = PostgresWriter(url)
    elif dialect in ("duckdb", "sqlite"):
        writer = _EmbeddedWriter(url, dialect)
    elif dialect == "mysql":
        writer = MySQLWriter(url)
    elif dialect == "mssql":
        writer = SQLServerWriter(url)
    else:
        raise DatabaseWriteError(
            f"format: database does not support '{dialect}' yet (supported: postgresql, mysql, mssql, duckdb, "
            "sqlite). Use format: dlt for this database."
        )
    _WRITERS[url] = writer
    return writer


def write_database(df: Any, block: Any, *, strategy: str = "append", primary_key: Iterable[str] = (),
                   batch_key: Optional[str] = None, evolution: str = "allow",
                   default_table: Optional[str] = None) -> Dict[str, Any]:
    """Write ``df`` as a ``format: database`` block (materialization or quarantine) says."""
    cfg = database_config(block)
    table = cfg.get("table") or default_table
    if not table:
        raise DatabaseWriteError("format: database needs `table:` (schema.table).")
    return writer_for(cfg.get("connection")).write(
        df, table=table, strategy=strategy, primary_key=list(primary_key or []), write=cfg.get("write"),
        batch_key=batch_key, evolution=evolution,
    )
