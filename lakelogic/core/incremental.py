"""
lakelogic.core.incremental
--------------------------
Resolves the time boundary for incremental layer-to-layer processing.

Given a source layer (e.g. Bronze) that may have accumulated multiple days of
partitions, this module answers: **which partitions have NOT yet been processed
into the target layer (Silver / Gold)?**

Five strategies — all produce the same ``Boundary(from_dt, to_dt)`` output:

  1. ``max_target`` *(default)* — query MAX(watermark_field) from the target
                         Delta table.  Processes everything newer than that
                         high-water mark.  Self-healing: if silver is manually
                         modified, the next run re-reads based on the timestamp
                         boundary, not a version pointer.

  2. ``pipeline_log``  — query a pipeline audit log table for the last
                         successful run.  More reliable than watermark when the
                         target table can be partially overwritten.

  3. ``manifest``      — read a JSON manifest file listing already-processed
                         partition values.  Lightweight, no Spark required.

  4. ``date_range``    — explicit from/to dates or a human-readable lookback
                         string (e.g. ``"7 days"``, ``"3 hours"``).
                         Useful for ad-hoc backfills and Databricks Widgets.

  5. ``delta_version`` — use Delta transaction log versions to identify new
                         commits.  Fastest for large tables: reads only the
                         specific Parquet files added/changed between versions.
                         State is stored in target table TBLPROPERTIES
                         (``lakelogic.last_source_version``).

Default strategy
~~~~~~~~~~~~~~~~
When ``watermark_strategy`` is not specified in the contract source block,
``max_target`` is used.  When ``watermark_field`` is also omitted,
the processor defaults to ``_lakelogic_processed_at`` (stamped by lineage).

Choosing a strategy
~~~~~~~~~~~~~~~~~~~

                    filt = (
                        (F.col("dataset") == dataset)
                        & (F.col("stage") != "no_new_data")
                        & (F.col("stage") != "reprocess")
                    )
                    if data_layer:
                        filt = filt & (F.col("data_layer") == data_layer)
                    if domain:
                        filt = filt & (F.col("domain") == domain)
                    if system:
                        filt = filt & (F.col("system") == system)

                    row = (
                        spark.table(log_table)
                        .filter(filt)
                        .agg(
                            F.max("max_watermark_value").alias("last_watermark"),
                            F.max(
                                F.get_json_object("report_json", "$.incremental_metadata.to_version").cast("int")
                            ).alias("last_json_version"),
                        )
                        .collect()[0]
                    )

                    _wm_val = row["last_watermark"]
                    _json_val = row["last_json_version"]

                    if _wm_val is not None:
                        last_version = int(_wm_val)
                        logger.info(
                            f"Healed missing Delta property from {log_table}.max_watermark_value. Resuming from {last_version}."
                        )
                    elif _json_val is not None:
                        last_version = int(_json_val)
                        logger.info(
                            f"Healed missing Delta property from {log_table}.report_json. Resuming from {last_version}."
                        )
                except Exception as log_exc:
                    logger.debug(f"Fallback to {log_table} failed for {dataset}: {log_exc}")

            if last_version is None:
                logger.warning(
                    f"INCREMENTAL RESET: No Delta version state for [{dataset}]. Full load from v{default_version}."
                )
                logger.warning(
                    f"INCREMENTAL RESET context: domain={domain}, system={system}, data_layer={data_layer}, log_table={log_table}"
                )

            # 2. Resolve target version from source history
            source_name = source_path[6:] if source_path.startswith("table:") else f"delta.`{source_path}`"
            curr_version = spark.sql(f"DESCRIBE HISTORY {source_name} LIMIT 1").collect()[0]["version"]

            to_v = to_version if to_version is not None else curr_version

            skip_sync = False
            if last_version == curr_version:
                logger.info(
                    f"Target version ({last_version}) matches source version ({curr_version}). No sync required."
                )
                from_v = curr_version
                to_v = curr_version
                skip_sync = True
            else:
                from_v = (last_version + 1) if last_version is not None else default_version
                # Handle source rollback / drop-recreate (target claims to be ahead of source)
                if from_v > to_v:
                    logger.warning(
                        f"Target version ({last_version}) > current source version ({curr_version}). "
                        f"Source table may have been dropped or re-created. Resetting to FULL reload from version {default_version}."
                    )
                    from_v = default_version
                    to_v = curr_version

            meta = {
                "from_version": from_v,
                "to_version": to_v,
                "source_path": source_path,
                "target_path": target_path,
                "skip_sync": skip_sync,
            }

            # Map version numbers to dummy datetimes (Boundary requires datetimes)
            # We use the epoch + version-seconds as a stable-ish representation
            return Boundary(
                from_dt=datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=from_v),
                to_dt=datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=to_v),
                strategy="delta_version",
                metadata=meta,
            )

        except Exception as exc:
            # Fallback to full load if Spark/Delta check fails
            logger.warning(f"Delta version resolution failed for {source_path}: {exc}")
            return Boundary(
                from_dt=datetime(1900, 1, 1, tzinfo=timezone.utc),
                to_dt=datetime.now(timezone.utc),
                strategy="delta_version",
                metadata={"error": str(exc), "from_version": 0},
            )

    # ── Strategy 2: Pipeline log / audit table ────────────────────────────────

    @classmethod
    def from_pipeline_log(
        cls,
        pipeline_name: str,
        *,
        log_table: str = "pipeline_runs",
        dataset: Optional[str] = None,
        data_layer: Optional[str] = None,
        domain: Optional[str] = None,
        system: Optional[str] = None,
        to_dt: Optional[datetime] = None,
        default_from: Optional[Union[datetime, str]] = None,
    ) -> Boundary:
        """
        Strategy: query the LakeLogic ``_run_logs`` table for the last
        successful run of a specific contract/dataset.

        When ``dataset`` is provided (recommended), the method queries the
        ``_run_logs`` table written by ``run_log.py`` using the ``dataset``
        column (which holds the actual target table name, e.g.
        ``bronze_google_analytics_events``).  Additional precision filters
        (``data_layer``, ``domain``, ``system``) are applied when available.

        Falls back to the legacy ``pipeline_runs`` table query (using
        ``pipeline_name`` + ``processed_through``) when ``dataset`` is not
        provided, for backward compatibility.

        Parameters
        ----------
        pipeline_name : str
            Registered name of the pipeline — used only in legacy fallback.
        log_table : str
            Spark/Databricks table name for the run log (e.g.
            ``"`catalog`.domain._run_logs"``).
        dataset : str, optional
            Target table name stored in the ``dataset`` column of
            ``_run_logs`` (e.g. ``"bronze_google_analytics_events"``).
        data_layer : str, optional
            Layer filter (``bronze``, ``silver``, ``gold``).
        domain : str, optional
            Domain filter (e.g. ``"marketing"``).
        system : str, optional
            System filter (e.g. ``"google_analytics"``).
        to_dt : datetime, optional
            Upper bound — defaults to NOW (UTC).
        default_from : datetime or ISO string, optional
            Fallback when no successful run exists.

        Example
        -------
        ::

            boundary = IncrementalBoundary.from_pipeline_log(
                pipeline_name="bronze_google_analytics_events",
                log_table="`lakelogic-lakehouse-dev-001`.marketing._run_logs",
                dataset="bronze_google_analytics_events",
                data_layer="bronze",
                domain="marketing",
                system="google_analytics",
            )
        """
        try:
            from pyspark.sql import SparkSession
            import pyspark.sql.functions as F

            spark = SparkSession.getActiveSession()
            if spark is None:
                raise RuntimeError("No active Spark session")

            if dataset:
                # ── Modern path: query _run_logs by dataset column ────────
                filt = (
                    (F.col("dataset") == dataset) & (F.col("stage") != "no_new_data") & (F.col("stage") != "reprocess")
                )
                if data_layer:
                    filt = filt & (F.col("data_layer") == data_layer)
                if domain:
                    filt = filt & (F.col("domain") == domain)
                if system:
                    filt = filt & (F.col("system") == system)

                row = (
                    spark.table(log_table)
                    .filter(filt)
                    .agg(
                        F.max("max_source_mtime").alias("last_source_mtime"),
                        F.max("max_watermark_value").alias("last_watermark"),
                        F.max("timestamp").alias("last_success"),
                    )
                    .collect()[0]
                )

                # Priority: max_source_mtime (epoch of upstream table's last processed row)
                #         → max_watermark_value (explicit watermark from prior run)
                #         → timestamp (fallback to run timestamp)
                _src_mtime = row["last_source_mtime"]
                if _src_mtime is not None:
                    # max_source_mtime is stored as epoch seconds (float/int)
                    last_success = datetime.fromtimestamp(float(_src_mtime), tz=timezone.utc)
                    logger.info(f"Pipeline log boundary from max_source_mtime: {last_success.isoformat()}")
                else:
                    last_success_str = row["last_watermark"] or row["last_success"]
                    if last_success_str is None:
                        raise ValueError(
                            f"No successful run found in {log_table} for dataset={dataset!r} data_layer={data_layer!r}"
                        )

                    # timestamp is stored as ISO string in _run_logs
                    if isinstance(last_success_str, str):
                        last_success = datetime.fromisoformat(last_success_str.replace("Z", "+00:00"))
                    elif isinstance(last_success_str, datetime):
                        last_success = last_success_str
                    else:
                        last_success = datetime.fromisoformat(str(last_success_str))

                from_dt = last_success + timedelta(seconds=1)
                meta = {
                    "last_success": str(last_success),
                    "source": "max_source_mtime" if _src_mtime is not None else "max_watermark_value",
                    "dataset": dataset,
                    "data_layer": data_layer or "",
                    "domain": domain or "",
                    "system": system or "",
                    "log_table": log_table,
                }
            else:
                # ── Legacy path: query pipeline_runs by pipeline_name ─────
                row = (
                    spark.table(log_table)
                    .filter((F.col("pipeline_name") == pipeline_name) & (F.col("status") == "success"))
                    .agg(F.max("processed_through").alias("last_success"))
                    .collect()[0]
                )
                last_success = row["last_success"]
                if last_success is None:
                    raise ValueError(f"No successful run found for {pipeline_name!r}")

                from_dt = (
                    last_success + timedelta(seconds=1)
                    if isinstance(last_success, datetime)
                    else datetime(last_success.year, last_success.month, last_success.day) + timedelta(days=1)
                )
                meta = {
                    "last_success": str(last_success),
                    "pipeline_name": pipeline_name,
                }

        except Exception as exc:
            if default_from is not None:
                from_dt = datetime.fromisoformat(default_from) if isinstance(default_from, str) else default_from
            else:
                from_dt = datetime.now(timezone.utc) - timedelta(days=90)
            meta = {"fallback_reason": str(exc), "pipeline_name": pipeline_name}

        _to = to_dt or datetime.now(timezone.utc)
        return Boundary(from_dt=from_dt, to_dt=_to, strategy="pipeline_log", metadata=meta)

    # ── Strategy 3: Manifest file ─────────────────────────────────────────────

    @classmethod
    def from_manifest(
        cls,
        manifest_path: str,
        partition_field: str = "_snapshot_date",
        *,
        to_dt: Optional[datetime] = None,
        default_from: Optional[Union[datetime, str]] = None,
    ) -> Boundary:
        """
        Strategy: read a JSON manifest file listing already-processed partition values.

        The manifest is a JSON file with the schema::

            {
              "pipeline": "bronze_to_silver_zoopla_listings",
              "processed_partitions": ["2024-03-01", "2024-03-02", "2024-03-03"],
              "last_updated": "2024-03-03T02:15:00Z"
            }

        The boundary ``from_dt`` is set to the day AFTER the latest processed partition.

        Parameters
        ----------
        manifest_path : str
            Path to the manifest JSON file (local path, ADLS path, or S3 URI).
            For cloud paths: pass a local temp path after downloading with your
            cloud SDK, or use ``dbutils.fs.cp`` on Databricks.
        partition_field : str
            For documentation only — not used at runtime (manifest stores the values).
        to_dt : datetime, optional
            Upper bound — defaults to NOW (UTC).

        Example manifest file
        ---------------------
        ::

            {
              "pipeline": "bronze_to_silver_zoopla",
              "processed_partitions": ["2024-03-01", "2024-03-02"],
              "last_updated": "2024-03-02T03:00:00Z"
            }

        Example usage
        -------------
        ::

            boundary = IncrementalBoundary.from_manifest(
                manifest_path="/dbfs/mnt/meta/manifests/bronze_to_silver_zoopla.json"
            )
            # Manifest shows last processed = '2024-03-02'
            # boundary.from_date == date(2024, 3, 3)
        """
        try:
            p = Path(manifest_path)
            if not p.exists():
                raise FileNotFoundError(f"Manifest not found: {manifest_path}")

            data = json.loads(p.read_text(encoding="utf-8"))
            partitions: List[str] = data.get("processed_partitions", [])

            if not partitions:
                raise ValueError("Manifest has no processed_partitions")

            last_partition = max(partitions)  # ISO string comparison works for dates
            last_dt = (
                datetime.fromisoformat(last_partition)
                if "T" in last_partition
                else datetime.strptime(last_partition, "%Y-%m-%d")
            )
            from_dt = last_dt + timedelta(days=1)
            meta = {
                "manifest_path": manifest_path,
                "last_partition": last_partition,
                "total_processed": len(partitions),
            }

        except Exception as exc:
            if default_from is not None:
                from_dt = datetime.fromisoformat(default_from) if isinstance(default_from, str) else default_from
            else:
                from_dt = datetime.now(timezone.utc) - timedelta(days=90)
            meta = {"fallback_reason": str(exc), "manifest_path": manifest_path}

        _to = to_dt or datetime.now(timezone.utc)
        return Boundary(from_dt=from_dt, to_dt=_to, strategy="manifest", metadata=meta)

    @classmethod
    def update_manifest(
        cls,
        manifest_path: str,
        pipeline: str,
        new_partitions: List[str],
    ) -> None:
        """
        Append newly-processed partition values to a manifest file.

        Call this AFTER a successful pipeline run to record what was processed.

        Example
        -------
        ::

            IncrementalBoundary.update_manifest(
                manifest_path="/dbfs/mnt/meta/manifests/bronze_to_silver_zoopla.json",
                pipeline="bronze_to_silver_zoopla",
                new_partitions=["2024-03-03", "2024-03-04"],
            )
        """
        p = Path(manifest_path)
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
        else:
            data = {"pipeline": pipeline, "processed_partitions": []}

        existing = set(data.get("processed_partitions", []))
        existing.update(new_partitions)
        data["processed_partitions"] = sorted(existing)
        data["last_updated"] = datetime.now(timezone.utc).isoformat() + "Z"

        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")

    # ── Strategy 4a: Explicit date range ─────────────────────────────────────

    @classmethod
    def from_date_range(
        cls,
        from_date: Union[str, date, datetime],
        to_date: Optional[Union[str, date, datetime]] = None,
        *,
        partition_filters: Optional[Dict[str, Any]] = None,
    ) -> Boundary:
        """
        Strategy: explicit from/to date range.

        Ideal for Databricks Widget parameters or ad-hoc backfills.

        Parameters
        ----------
        from_date : str, date, or datetime
            Inclusive start (e.g. ``"2024-03-01"`` or a ``datetime`` object).
        to_date : str, date, or datetime, optional
            Inclusive end. Defaults to NOW (UTC).
        partition_filters : dict, optional
            Static (non-temporal) partition values to AND into every filter.
            Example: ``{"country": "GB"}``

        Example
        -------
        ::

            # Databricks Widget pattern
            from_date = dbutils.widgets.get("from_date")   # "2024-03-01"
            to_date   = dbutils.widgets.get("to_date")     # "2024-03-31"
            boundary  = IncrementalBoundary.from_date_range(from_date, to_date)

            # Backfill March 2024, GB only
            boundary = IncrementalBoundary.from_date_range(
                "2024-03-01", "2024-03-31",
                partition_filters={"country": "GB"},
            )
            df.filter(boundary.spark_filter(date_parts=["year", "month", "day"]))
            # → "country = 'GB' AND MAKE_DATE(year,month,day) BETWEEN ..."
        """

        def _to_dt(v) -> datetime:
            if isinstance(v, datetime):
                return v
            if isinstance(v, date):
                return datetime(v.year, v.month, v.day)
            return datetime.fromisoformat(str(v))

        from_dt = _to_dt(from_date)
        to_dt = _to_dt(to_date) if to_date is not None else datetime.now(timezone.utc)
        return Boundary(
            from_dt=from_dt,
            to_dt=to_dt,
            strategy="date_range",
            partition_filters=partition_filters or {},
            metadata={"from_date": str(from_date), "to_date": str(to_date or "now")},
        )

    # ── Strategy 4b: Lookback string ─────────────────────────────────────────

    @classmethod
    def from_lookback(
        cls,
        lookback: str,
        *,
        reference_dt: Optional[datetime] = None,
        partition_filters: Optional[Dict[str, Any]] = None,
    ) -> Boundary:
        """
        Strategy: human-readable lookback from NOW (or a reference point).

        Parameters
        ----------
        lookback : str
            Duration string. Examples:

            ============= ===========
            String        Window
            ============= ===========
            ``"15 mins"`` last 15 minutes
            ``"3 hours"`` last 3 hours
            ``"7 days"``  last 7 days
            ``"2 weeks"`` last 2 weeks
            ``"1 month"`` last ~30 days
            ``"1 year"``  last ~365 days
            ============= ===========

        reference_dt : datetime, optional
            Anchor point. Defaults to ``datetime.now(timezone.utc)``.
        partition_filters : dict, optional
            Static (non-temporal) partition values to AND into every filter.
            Example: ``{"country": "GB", "region": "south"}``

        Examples
        --------
        ::

            # Last 7 days, all partitions
            boundary = IncrementalBoundary.from_lookback("7 days")

            # Last 7 days, GB only (table partitioned by country, year, month, day)
            boundary = IncrementalBoundary.from_lookback(
                "7 days",
                partition_filters={"country": "GB"},
            )
            df.filter(boundary.spark_filter(date_parts=["year", "month", "day"]))
            # → "country = 'GB' AND MAKE_DATE(year, month, day) >= '...' AND ..."

            # Near-real-time micro-batch
            boundary = IncrementalBoundary.from_lookback("30 mins")
        """
        delta = _parse_lookback(lookback)
        to_dt = reference_dt or datetime.now(timezone.utc)
        from_dt = to_dt - delta
        return Boundary(
            from_dt=from_dt,
            to_dt=to_dt,
            strategy="lookback",
            partition_filters=partition_filters or {},
            metadata={"lookback": lookback, "delta_seconds": delta.total_seconds()},
        )

    # ── Convenience: resolve from contract SourceConfig ────────────────────────

    @classmethod
    def from_contract(
        cls,
        contract_path: str,
        *,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        lookback: Optional[str] = None,
        target_path: Optional[str] = None,
        pipeline_name: Optional[str] = None,
        manifest_path: Optional[str] = None,
        partition_filters: Optional[Dict[str, Any]] = None,
    ) -> Boundary:
        """
        Resolve boundary from a LakeLogic contract ``source`` block.

        Runtime overrides always take precedence, enabling Databricks Widget /
        CLI parameterisation and the **per-partition pipeline pattern**.

        Partition filter merging
        ------------------------
        ``partition_filters`` at runtime is **merged on top of** any
        ``partition_filters`` declared in the contract.  Runtime values win.
        This lets you bake defaults into the contract and add the country/
        region at runtime, OR put nothing in the contract and supply
        everything at runtime.

        Per-country pipeline patterns
        -----------------------------
        ::

            # Pattern A — country-specific contract (one YAML per country)
            #   bronze_listings_gb.yaml: partition_filters: {country: GB}
            boundary = IncrementalBoundary.from_contract(
                "contracts/bronze_listings_gb.yaml",
                lookback="7 days",
            )

            # Pattern B — generic contract, country at runtime
            for country in ["GB", "DE", "FR", "ES"]:
                boundary = IncrementalBoundary.from_contract(
                    "contracts/bronze_listings.yaml",
                    lookback="7 days",
                    partition_filters={"country": country},
                )
                (bronze_df
                    .filter(boundary.spark_filter(date_parts=["year", "month", "day"]))
                    .write.format("delta")
                    .save(f"{SILVER_ROOT}/{country}")
                )

            # Pattern C — Databricks Widgets
            boundary = IncrementalBoundary.from_contract(
                CONTRACT_PATH,
                lookback=dbutils.widgets.get("lookback"),
                partition_filters={"country": dbutils.widgets.get("country")},
            )
        """
        import yaml as _yaml

        data = _yaml.safe_load(Path(contract_path).read_text(encoding="utf-8"))
        src = data.get("source") or {}

        strategy = src.get("watermark_strategy", "max_target")
        wm_field = src.get("watermark_field", "_snapshot_date")
        contract_lookback = src.get("lookback")

        # Merge: contract is base, runtime partition_filters wins on conflicts
        merged_pf: Dict[str, Any] = {**src.get("partition_filters", {})}
        if partition_filters:
            merged_pf.update(partition_filters)

        if lookback:
            return cls.from_lookback(lookback, partition_filters=merged_pf or None)

        if from_date:
            return cls.from_date_range(from_date, to_date, partition_filters=merged_pf or None)

        if strategy == "lookback":
            lb = contract_lookback or "7 days"
            return cls.from_lookback(lb, partition_filters=merged_pf or None)

        if strategy == "pipeline_log":
            log_table = src.get("pipeline_log_table", "pipeline_runs")
            p_name = pipeline_name or src.get("pipeline_name", Path(contract_path).stem)
            b = cls.from_pipeline_log(
                p_name,
                log_table=log_table,
                dataset=src.get("dataset"),
                data_layer=src.get("data_layer"),
                domain=src.get("domain"),
                system=src.get("system"),
            )
            b.partition_filters = merged_pf
            return b

        if strategy == "manifest":
            mp = manifest_path or src.get("manifest_path", "")
            b = cls.from_manifest(mp, partition_field=wm_field)
            b.partition_filters = merged_pf
            return b

        if strategy == "date_range":
            fd = from_date or src.get("from_date")
            td = to_date or src.get("to_date")
            return cls.from_date_range(fd, td, partition_filters=merged_pf or None)

        if strategy == "delta_version":
            tp = target_path or src.get("target_path", "")
            sp = src.get("path", "")
            return cls.from_delta_version(
                sp,
                tp,
                dataset=src.get("dataset"),
                data_layer=src.get("data_layer"),
                domain=src.get("domain"),
                system=src.get("system"),
                log_table=src.get("pipeline_log_table", "pipeline_runs"),
            )

        tp = target_path or src.get("target_path", "")
        b = cls.from_max_target(tp, watermark_field=wm_field)
        b.partition_filters = merged_pf
        return b

    # ── Convenience: resolve from SourceConfig model ────────────────────────

    @classmethod
    def from_source_config(cls, source_config, **overrides) -> Boundary:
        """
        Resolve boundary from a ``SourceConfig`` Pydantic model instance.

        Supports a ``partition_filters`` override kwarg using the same merge
        semantics as :meth:`from_contract` (runtime wins over contract).

        Examples
        --------
        ::

            from lakelogic.core.models import DataContract
            contract = DataContract.from_yaml("bronze_listings.yaml")

            # Per-country pipeline
            for country in ["GB", "DE", "FR"]:
                boundary = IncrementalBoundary.from_source_config(
                    contract.source,
                    lookback="7 days",
                    partition_filters={"country": country},
                )
                bronze_df.filter(
                    boundary.spark_filter(date_parts=["year", "month", "day"])
                ).write.format("delta").save(f"{SILVER_ROOT}/{country}")
        """
        cfg = source_config.model_dump() if hasattr(source_config, "model_dump") else vars(source_config)

        # Extract partition_filters separately — needs merge, not plain replace
        runtime_pf: Dict[str, Any] = overrides.pop("partition_filters", None) or {}
        cfg.update({k: v for k, v in overrides.items() if v is not None})

        merged_pf: Dict[str, Any] = {**cfg.get("partition_filters", {})}
        merged_pf.update(runtime_pf)

        lookback = cfg.get("lookback")
        from_date = cfg.get("from_date")
        to_date = cfg.get("to_date")
        strategy = cfg.get("watermark_strategy", "max_target")
        wm_field = cfg.get("watermark_field", "_snapshot_date")

        if lookback:
            return cls.from_lookback(lookback, partition_filters=merged_pf or None)
        if from_date:
            return cls.from_date_range(from_date, to_date, partition_filters=merged_pf or None)
        if strategy == "lookback":
            return cls.from_lookback(
                cfg.get("lookback_default", "7 days"),
                partition_filters=merged_pf or None,
            )
        if strategy == "pipeline_log":
            b = cls.from_pipeline_log(
                cfg.get("pipeline_name", "unknown"),
                log_table=cfg.get("pipeline_log_table", "pipeline_runs"),
                dataset=cfg.get("dataset"),
                data_layer=cfg.get("data_layer"),
                domain=cfg.get("domain"),
                system=cfg.get("system"),
            )
            b.partition_filters = merged_pf
            return b
        if strategy == "manifest":
            b = cls.from_manifest(cfg.get("manifest_path", ""), partition_field=wm_field)
            b.partition_filters = merged_pf
            return b
        if strategy == "date_range":
            return cls.from_date_range(
                cfg.get("from_date"),
                cfg.get("to_date"),
                partition_filters=merged_pf or None,
            )
        if strategy == "delta_version":
            return cls.from_delta_version(
                cfg.get("path", ""),
                cfg.get("target_path", ""),
                dataset=cfg.get("dataset"),
                data_layer=cfg.get("data_layer"),
                domain=cfg.get("domain"),
                system=cfg.get("system"),
                log_table=cfg.get("pipeline_log_table", "pipeline_runs"),
            )
        b = cls.from_max_target(cfg.get("target_path", ""), watermark_field=wm_field)
        b.partition_filters = merged_pf
        return b
