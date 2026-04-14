"""
Security-group-aware PII masking engine for LakeLogic.

Extends the existing ``gdpr.py`` masking with:
- **Security group mapping**: per-field ``security_groups`` + ``masking`` strategy
- **Strategies**: ``nullify``, ``hash``, ``redact``, ``partial``, ``encrypt``
- **User context**: caller passes their groups; only fields they lack access to are masked
- **Databricks UC mask generation**: auto-generate ``CREATE FUNCTION`` + ``ALTER TABLE`` SQL
- **Variable-driven Encryption**: pass data encryption keys directly via environment
  variables (e.g. injected via Azure Key Vault)

Usage::

    from lakelogic.core.masking_engine import MaskingEngine

    # Mask data for a user who is in the "analytics" group but not "pii-readers"
    engine = MaskingEngine(contract)
    masked_df = engine.apply(df, user_groups=["analytics"])

    # Generate Databricks UC mask SQL
    sql = engine.generate_uc_masks(catalog="main", schema="gold")

Contract YAML::

    model:
      fields:
        - name: email
          type: string
          pii: true
          security_groups: ["pii-readers", "compliance"]
          masking: "partial"

        - name: ssn
          type: string
          pii: true
          security_groups: ["hr-admins"]
          masking: "encrypt"
"""

from __future__ import annotations

import hashlib
import re
import uuid
from typing import Any, Dict, List, Optional

from loguru import logger

from lakelogic.core.models import DataContract, FieldDefinition

# ── Masking Strategy Constants ───────────────────────────────────────────────

VALID_MASKING_STRATEGIES = {"nullify", "hash", "redact", "partial", "encrypt"}
# Deprecated: "tokenize" — use "encrypt" instead (reversible via Key Vault)
_DEPRECATED_STRATEGIES = {"tokenize": "encrypt"}
DEFAULT_STRATEGY = "redact"


# ── Token Store (in-memory, pluggable) ───────────────────────────────────────


class TokenStore:
    """
    Simple in-memory bi-directional token store for reversible tokenization.

    Production deployments should replace this with a persistent store
    (Redis, DynamoDB, Azure Table Storage, etc.) by subclassing.
    """

    def __init__(self) -> None:
        self._forward: Dict[str, str] = {}  # original → token
        self._reverse: Dict[str, str] = {}  # token → original

    def tokenize(self, value: Any) -> Optional[str]:
        """Return a deterministic token for a value. None → None."""
        if value is None:
            return None
        key = str(value)
        if key not in self._forward:
            token = f"tok_{uuid.uuid4().hex[:12]}"
            self._forward[key] = token
            self._reverse[token] = key
        return self._forward[key]

    def detokenize(self, token: str) -> Optional[str]:
        """Reverse a token back to its original value."""
        return self._reverse.get(token)

    def export(self) -> Dict[str, str]:
        """Export the full token mapping (for persistence)."""
        return dict(self._reverse)


# ── Masking Functions ────────────────────────────────────────────────────────


def _apply_partial_format(value: Any, fmt: str) -> Optional[str]:
    """Apply a custom partial masking format template.

    Supported tokens:
        ``{first1}`` – ``{first9}``: first N characters
        ``{last1}``  – ``{last9}``:  last N characters
        ``{domain}``:                email domain (after @)

    Everything else in the format string is kept as literal text.

    Examples::

        _apply_partial_format("john@company.com", "{first1}***@{domain}")
        # → "j***@company.com"

        _apply_partial_format("+44 7700 900123", "***-***-{last4}")
        # → "***-***-0123"

        _apply_partial_format("SW1A 2AA", "{first2}** ***")
        # → "SW** ***"
    """
    if value is None or str(value) == "None":
        return None
    text = str(value)
    if not text:
        return text

    result = fmt

    # Resolve {domain} — email domain
    if "{domain}" in result:
        if "@" in text:
            domain = text.rsplit("@", 1)[1]
        else:
            domain = "***"
        result = result.replace("{domain}", domain)

    # Resolve {firstN}
    for n in range(1, 10):
        token = f"{{first{n}}}"
        if token in result:
            result = result.replace(token, text[:n] if len(text) >= n else text)

    # Resolve {lastN}
    for n in range(1, 10):
        token = f"{{last{n}}}"
        if token in result:
            # For phone numbers, extract digits for {last4} etc
            digits = re.sub(r"[^0-9]", "", text)
            if digits and len(digits) >= n:
                result = result.replace(token, digits[-n:])
            elif len(text) >= n:
                result = result.replace(token, text[-n:])
            else:
                result = result.replace(token, text)

    return result


def _apply_partial(value: Any, field_name: str = "", fmt: Optional[str] = None) -> Optional[str]:
    """
    Partially mask a value, preserving structure for support/debugging.

    If ``fmt`` is provided, uses the custom template (see ``_apply_partial_format``).
    Otherwise auto-detects the value type:

    - Email: ``j***@domain.com``
    - Phone: ``***-***-1234``
    - General string: first char + ``***`` + last char
    """
    if value is None or str(value) == "None":
        return None
    text = str(value)
    if not text:
        return text

    # Custom format template
    if fmt:
        return _apply_partial_format(value, fmt)

    # Auto-detect: Email
    if "@" in text:
        local, domain = text.rsplit("@", 1)
        if len(local) > 1:
            return f"{local[0]}***@{domain}"
        return f"***@{domain}"

    # Auto-detect: Phone (7+ digits)
    digits = re.sub(r"[^0-9]", "", text)
    if len(digits) >= 7:
        return f"***-***-{digits[-4:]}"

    # Auto-detect: General string
    if len(text) > 2:
        return f"{text[0]}{'*' * (len(text) - 2)}{text[-1]}"
    return "*" * len(text)


def _prepare_fernet_key(key: str) -> bytes:
    import base64
    import hashlib

    # Fernet requires a 32-byte url-safe base64-encoded key
    # We hash the provided string key to ensure it's exactly 32 bytes, then base64 encode it.
    key_bytes = key.encode("utf-8") if key else b"default-lakelogic-encryption-key"
    hashed = hashlib.sha256(key_bytes).digest()
    return base64.urlsafe_b64encode(hashed)


def _apply_encrypt(value: Any, key: str = "") -> Optional[str]:
    """
    Symmetric Fernet encryption using the provided key.
    """
    if value is None:
        return None

    try:
        from cryptography.fernet import Fernet
    except ImportError:
        raise ImportError("The 'cryptography' package is required for the 'encrypt' strategy. Please install it.")

    fern = Fernet(_prepare_fernet_key(key))
    encrypted = fern.encrypt(str(value).encode("utf-8"))
    return f"enc:{encrypted.decode('ascii')}"


def _apply_decrypt(encrypted_value: str, key: str = "") -> Optional[str]:
    """Reverse the Fernet encryption from _apply_encrypt."""
    if not encrypted_value or not encrypted_value.startswith("enc:"):
        return encrypted_value

    try:
        from cryptography.fernet import Fernet
    except ImportError:
        raise ImportError("The 'cryptography' package is required for decryption.")

    fern = Fernet(_prepare_fernet_key(key))
    try:
        decrypted = fern.decrypt(encrypted_value[4:].encode("ascii"))
        return decrypted.decode("utf-8")
    except Exception:
        # Invalid key or tampered payload
        return None


def _apply_hash(value: Any, salt: str = "") -> Optional[str]:
    """One-way SHA-256 hash."""
    if value is None:
        return None
    raw = f"{salt}{value}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


# ── Main Masking Engine ──────────────────────────────────────────────────────


class MaskingEngine:
    """
    Security-group-aware masking engine.

    Reads PII field annotations from a DataContract and applies per-field
    masking strategies based on the caller's security group membership.

    Parameters
    ----------
    contract : DataContract
        Contract with PII-annotated fields (``pii: true``, ``security_groups``, ``masking``).
    token_store : TokenStore, optional
        Custom token store for ``tokenize`` strategy. Defaults to in-memory.
    encryption_key : str, optional
        Key for ``encrypt`` strategy. In production, load from Key Vault / KMS.
    hash_salt : str, optional
        Salt for ``hash`` strategy.
    """

    def __init__(
        self,
        contract: DataContract,
        *,
        token_store: Optional[TokenStore] = None,
        encryption_key: str = "",
        hash_salt: str = "",
    ) -> None:
        self.contract = contract
        self.token_store = token_store or TokenStore()
        self.encryption_key = encryption_key
        self.hash_salt = hash_salt
        self._pii_fields = self._extract_pii_fields()

    def _extract_pii_fields(self) -> List[FieldDefinition]:
        """Get all PII-flagged fields from the contract."""
        if not self.contract.model or not self.contract.model.fields:
            return []
        return [f for f in self.contract.model.fields if f.pii]

    def get_fields_to_mask(self, user_groups: Optional[List[str]] = None) -> List[FieldDefinition]:
        """
        Return PII fields that should be masked for a user with the given groups.

        If ``user_groups`` is None, ALL PII fields are returned (no access).
        If a field has no ``security_groups``, it's always masked (public PII).
        If the user belongs to at least one of the field's groups, it's unmasked.
        """
        if user_groups is None:
            return list(self._pii_fields)

        user_groups_set = set(user_groups)
        result = []
        for field in self._pii_fields:
            if not field.security_groups:
                # No groups defined → always mask
                result.append(field)
            elif not user_groups_set.intersection(field.security_groups):
                # User is NOT in any of the allowed groups → mask
                result.append(field)
            # else: user IS in an allowed group → skip (unmasked)
        return result

    def _mask_value(self, value: Any, strategy: str, field_name: str = "", masking_format: Optional[str] = None) -> Any:
        """Apply a masking strategy to a single value."""
        # Guard: null values pass through unchanged
        if value is None or (isinstance(value, str) and value == "None"):
            return None

        # Handle deprecated strategies
        if strategy in _DEPRECATED_STRATEGIES:
            resolved = _DEPRECATED_STRATEGIES[strategy]
            logger.warning(
                f"Masking strategy '{strategy}' is deprecated — using '{resolved}' instead. Update your contract."
            )
            strategy = resolved

        if strategy == "nullify":
            return None
        elif strategy == "hash":
            return _apply_hash(value, self.hash_salt)
        elif strategy == "redact":
            return "***REDACTED***" if value is not None else None
        elif strategy == "partial":
            return _apply_partial(value, field_name, fmt=masking_format)
        elif strategy == "encrypt":
            return _apply_encrypt(value, self.encryption_key)
        else:
            logger.warning(f"Unknown masking strategy '{strategy}', falling back to redact")
            return "***REDACTED***" if value is not None else None

    def apply(
        self,
        df: Any,
        *,
        user_groups: Optional[List[str]] = None,
        strategy_override: Optional[str] = None,
    ) -> Any:
        """
        Mask PII columns in a DataFrame based on user group membership.

        Parameters
        ----------
        df : DataFrame
            Polars, Pandas, or DuckDB DataFrame.
        user_groups : list of str, optional
            Security groups the current user belongs to. If None, all PII is masked.
        strategy_override : str, optional
            Override all per-field strategies with a single strategy.

        Returns
        -------
        DataFrame with PII columns masked for unauthorized fields.
        """
        fields_to_mask = self.get_fields_to_mask(user_groups)
        if not fields_to_mask:
            logger.info("User has access to all PII fields — no masking applied.")
            return df

        # Separate fields with explicit masking from those that only have pii: true
        fields_with_masking = [f for f in fields_to_mask if strategy_override or f.masking]
        fields_without_masking = [f for f in fields_to_mask if not strategy_override and not f.masking]

        if fields_without_masking:
            names = ", ".join(f.name for f in fields_without_masking)
            logger.warning(
                f"PII fields detected without masking strategy: [{names}]. "
                f"Set 'masking:' (nullify|hash|redact|partial|encrypt) "
                f"in your contract to enable masking for these fields."
            )

        if not fields_with_masking:
            logger.info("No PII fields with explicit masking strategy — skipping masking.")
            return df

        VALID_STRATEGIES = {"nullify", "hash", "redact", "partial", "encrypt"}
        field_strategies = {}
        for f in fields_with_masking:
            strategy = strategy_override or f.masking
            if strategy and strategy.lower() not in VALID_STRATEGIES:
                logger.warning(
                    f"Invalid masking strategy '{strategy}' for field '{f.name}'. "
                    f"Valid options are: nullify, hash, redact, partial, encrypt."
                )
            field_strategies[f.name] = (strategy, getattr(f, "masking_format", None))

        logger.info(
            f"PII masking: {len(fields_with_masking)} field(s) for user_groups={user_groups or '(none)'}: "
            f"{', '.join(f'{k}→{v[0]}' for k, v in field_strategies.items())}"
        )

        # Dispatch by DataFrame type
        try:
            import polars as pl

            if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
                return self._apply_polars(df, field_strategies)
        except ImportError:
            pass

        try:
            import pandas as pd

            if isinstance(df, pd.DataFrame):
                return self._apply_pandas(df, field_strategies)
        except ImportError:
            pass

        # Spark DataFrame
        try:
            from pyspark.sql import DataFrame as SparkDataFrame

            if isinstance(df, SparkDataFrame):
                return self._apply_spark(df, field_strategies)
        except ImportError:
            pass

        if hasattr(df, "fetchdf"):
            import duckdb
            import pandas as pd

            pdf = df.fetchdf()
            result = self._apply_pandas(pdf, field_strategies)
            return duckdb.from_df(result)

        raise TypeError(f"Unsupported dataframe type: {type(df)}")

    def _apply_polars(self, df: Any, field_strategies: Dict[str, tuple]) -> Any:
        """Apply masking to a Polars DataFrame."""
        import polars as pl

        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        for col_name, (strategy, fmt) in field_strategies.items():
            if col_name not in df.columns:
                continue

            if strategy == "nullify":
                df = df.with_columns(pl.lit(None).alias(col_name))
            elif strategy == "redact":
                df = df.with_columns(
                    pl.when(pl.col(col_name).is_not_null())
                    .then(pl.lit("***REDACTED***"))
                    .otherwise(pl.lit(None))
                    .alias(col_name)
                )
            elif strategy in ("hash", "partial", "tokenize", "encrypt"):
                df = df.with_columns(
                    pl.col(col_name)
                    .cast(pl.Utf8)
                    .map_elements(
                        lambda v, _s=strategy, _n=col_name, _f=fmt: self._mask_value(v, _s, _n, _f),
                        return_dtype=pl.Utf8,
                        skip_nulls=True,
                    )
                    .alias(col_name)
                )

        return df

    def _apply_pandas(self, df: Any, field_strategies: Dict[str, tuple]) -> Any:
        """Apply masking to a Pandas DataFrame."""
        df = df.copy()
        for col_name, (strategy, fmt) in field_strategies.items():
            if col_name not in df.columns:
                continue

            if strategy == "nullify":
                df[col_name] = None
            elif strategy == "redact":
                df.loc[df[col_name].notna(), col_name] = "***REDACTED***"
            elif strategy in ("hash", "partial", "tokenize", "encrypt"):
                df[col_name] = df[col_name].apply(
                    lambda v, _s=strategy, _n=col_name, _f=fmt: self._mask_value(v, _s, _n, _f)
                )

        return df

    def _apply_spark(self, df: Any, field_strategies: Dict[str, tuple]) -> Any:
        """Apply masking to a PySpark DataFrame using native SQL expressions."""
        from pyspark.sql import functions as F

        for col_name, (strategy, fmt) in field_strategies.items():
            if col_name not in df.columns:
                continue

            if strategy == "nullify":
                df = df.withColumn(col_name, F.lit(None).cast("string"))

            elif strategy == "redact":
                df = df.withColumn(
                    col_name,
                    F.when(F.col(col_name).isNotNull(), F.lit("***REDACTED***")).otherwise(F.lit(None)),
                )

            elif strategy == "hash":
                salt = self.hash_salt
                if salt:
                    df = df.withColumn(col_name, F.sha2(F.concat(F.lit(salt), F.col(col_name).cast("string")), 256))
                else:
                    df = df.withColumn(col_name, F.sha2(F.col(col_name).cast("string"), 256))

            elif strategy == "partial":
                if fmt:
                    # Custom format — use UDF for template flexibility
                    from pyspark.sql.types import StringType

                    @F.udf(StringType())
                    def _partial_mask_udf(val, _fmt=fmt):
                        return _apply_partial(val, fmt=_fmt)

                    df = df.withColumn(col_name, _partial_mask_udf(F.col(col_name).cast("string")))
                else:
                    # Auto-detect: Email → j***@domain.com, General → first + *** + last
                    df = df.withColumn(
                        col_name,
                        F.when(
                            F.col(col_name).contains("@"),
                            F.concat(
                                F.substring(F.col(col_name), 1, 1),
                                F.lit("***@"),
                                F.element_at(F.split(F.col(col_name), "@"), 2),
                            ),
                        ).otherwise(
                            F.when(
                                F.length(F.col(col_name)) > 2,
                                F.concat(
                                    F.substring(F.col(col_name), 1, 1),
                                    F.lit("***"),
                                    F.substring(F.col(col_name), F.length(F.col(col_name)), 1),
                                ),
                            ).otherwise(F.lit("**"))
                        ),
                    )

            elif strategy == "encrypt":
                # Use Spark's native aes_encrypt if available, fall back to sha2
                try:
                    key = self.encryption_key or "default-key"
                    # Pad key to 16 bytes for AES-128
                    padded_key = (key * ((16 // len(key)) + 1))[:16]
                    df = df.withColumn(
                        col_name,
                        F.concat(
                            F.lit("enc:"),
                            F.base64(F.expr(f"aes_encrypt(CAST(`{col_name}` AS STRING), '{padded_key}')")),
                        ),
                    )
                except Exception:
                    # Fallback: deterministic hash for environments without aes_encrypt
                    logger.warning(f"aes_encrypt not available for '{col_name}', falling back to sha2")
                    df = df.withColumn(col_name, F.sha2(F.col(col_name).cast("string"), 256))

            elif strategy == "tokenize":
                # Deprecated: tokenize → encrypt. Keep for backward compat.
                logger.warning(
                    f"Masking strategy 'tokenize' is deprecated for field '{col_name}' — "
                    f"use 'encrypt' instead. Falling back to sha2-based token."
                )
                df = df.withColumn(
                    col_name,
                    F.concat(F.lit("tok_"), F.substring(F.sha2(F.col(col_name).cast("string"), 256), 1, 12)),
                )

        return df

    # ── PII Vault Helpers ────────────────────────────────────────────────────

    def get_vault_fields(self) -> List[FieldDefinition]:
        """Return PII fields that specify a ``pii_vault`` for reversible storage."""
        return [f for f in self._pii_fields if f.pii_vault]

    def extract_vault_df(self, df: Any, primary_key_columns: Optional[List[str]] = None) -> Any:
        """
        Extract a vault DataFrame containing only PK + PII vault columns.

        This is the DataFrame that should be written to the encrypted PII vault
        for later re-identification by authorized users.

        Parameters
        ----------
        df : DataFrame
            Full unmasked DataFrame (pre-masking).
        primary_key_columns : list of str, optional
            Primary key columns to include. Defaults to contract's ``primary_key``.

        Returns
        -------
        DataFrame with only PK + PII vault columns.
        """
        vault_fields = self.get_vault_fields()
        if not vault_fields:
            logger.info("No PII vault fields defined — skipping vault extraction.")
            return None

        pk_cols = primary_key_columns or self.contract.primary_key or []
        vault_col_names = [f.name for f in vault_fields]
        select_cols = list(dict.fromkeys(pk_cols + vault_col_names))  # dedupe, preserve order

        try:
            import polars as pl

            if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
                if isinstance(df, pl.LazyFrame):
                    df = df.collect()
                present = [c for c in select_cols if c in df.columns]
                logger.info(f"PII vault extraction: {len(present)} columns ({', '.join(present)})")
                return df.select(present)
        except ImportError:
            pass

        try:
            import pandas as pd

            if isinstance(df, pd.DataFrame):
                present = [c for c in select_cols if c in df.columns]
                logger.info(f"PII vault extraction: {len(present)} columns ({', '.join(present)})")
                return df[present].copy()
        except ImportError:
            pass

        raise TypeError(f"Unsupported dataframe type: {type(df)}")

    # ── Databricks UC Mask Generation ────────────────────────────────────────

    def generate_uc_masks(
        self,
        catalog: str = "main",
        schema: str = "gold",
        *,
        function_schema: str = "security",
        table_name: Optional[str] = None,
        uc_secret_scope: str = "lakelogic-secrets",
        uc_secret_key: str = "pii-encryption-key",
    ) -> str:
        """
        Generate Databricks Unity Catalog column masking SQL.

        Produces ``CREATE FUNCTION`` and ``ALTER TABLE ... SET MASK`` statements
        for each PII field with ``security_groups``.

        Parameters
        ----------
        catalog : str
            UC catalog name.
        schema : str
            UC schema for the target table.
        function_schema : str
            UC schema for the masking functions (default: ``security``).
        table_name : str, optional
            Table name. Defaults to ``contract.dataset``.

        Returns
        -------
        SQL string with all masking DDL statements.
        """
        if not self._pii_fields:
            return "-- No PII fields defined in contract\n"

        tbl = table_name or (self.contract.dataset or "unknown_table")
        full_table = f"{catalog}.{schema}.{tbl}"
        sql_parts = [
            "-- ==========================================================",
            f"-- UC Column Masks for {full_table}",
            "-- Generated by LakeLogic MaskingEngine",
            "-- ==========================================================",
            "",
        ]

        for field in self._pii_fields:
            strategy = field.masking or DEFAULT_STRATEGY
            groups = field.security_groups
            func_name = f"{catalog}.{function_schema}.mask_{tbl}_{field.name}"

            # Build the is_member() check
            if groups:
                group_checks = " OR ".join(f"is_member('{g}')" for g in groups)
                condition = f"({group_checks})"
            else:
                condition = "FALSE"  # No groups → always mask

            # Strategy-specific SQL expression
            if strategy == "nullify":
                mask_expr = "NULL"
            elif strategy == "hash":
                mask_expr = "sha2(CAST(val AS STRING), 256)"
            elif strategy == "redact":
                mask_expr = "'***REDACTED***'"
            elif strategy == "partial":
                if field.name.lower() in ("email", "email_address"):
                    mask_expr = "regexp_replace(CAST(val AS STRING), '(.).*(@.*)', '$1***$2')"
                else:
                    mask_expr = "concat(left(CAST(val AS STRING), 1, '***', right(CAST(val AS STRING), 1))"
            elif strategy == "tokenize":
                # Deprecated: map to encrypt
                mask_expr = f"base64(aes_encrypt(CAST(val AS STRING), secret('{uc_secret_scope}', '{uc_secret_key}')))"
            elif strategy == "encrypt":
                mask_expr = f"base64(aes_encrypt(CAST(val AS STRING), secret('{uc_secret_scope}', '{uc_secret_key}')))"
            else:
                mask_expr = "'***REDACTED***'"

            sql_parts.extend(
                [
                    f"-- Mask: {field.name} (strategy: {strategy}, groups: {groups or 'none'})",
                    f"CREATE OR REPLACE FUNCTION {func_name}(val STRING)",
                    f"  RETURN IF({condition}, val, {mask_expr});",
                    "",
                    f"ALTER TABLE {full_table}",
                    f"  ALTER COLUMN {field.name} SET MASK {func_name};",
                    "",
                ]
            )

        return "\n".join(sql_parts)

    # ── Reporting ────────────────────────────────────────────────────────────

    def report(self, user_groups: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Generate a masking policy report for auditing.

        Shows which fields are PII, who can access them, and what masking
        strategy is applied for unauthorized users.
        """
        policy = []
        user_groups_set = set(user_groups or [])

        for field in self._pii_fields:
            field_groups = set(field.security_groups)
            has_access = bool(user_groups_set.intersection(field_groups)) if field_groups else False
            strategy = field.masking or DEFAULT_STRATEGY

            policy.append(
                {
                    "field": field.name,
                    "type": field.type,
                    "pii": True,
                    "security_groups": field.security_groups,
                    "masking_strategy": strategy,
                    "user_has_access": has_access,
                    "action": "unmasked" if has_access else f"masked ({strategy})",
                    "reversible": strategy == "encrypt",
                    "pii_vault": field.pii_vault,
                }
            )

        return {
            "contract": self.contract.info.title if self.contract.info else "unknown",
            "total_pii_fields": len(self._pii_fields),
            "masked_for_user": sum(1 for p in policy if not p["user_has_access"]),
            "unmasked_for_user": sum(1 for p in policy if p["user_has_access"]),
            "user_groups": user_groups or [],
            "vault_fields": [f.name for f in self.get_vault_fields()],
            "fields": policy,
        }
