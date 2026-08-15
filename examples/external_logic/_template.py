"""
LakeLogic external-logic — the CANONICAL pattern.

Copy this file when you (or an agent) build an external-logic step. LakeLogic
hands your entrypoint the validated SOURCE frame plus any linked REFERENCE frames,
then GOVERNS whatever you return — schema, quality rules, PII, lineage, and
materialization all run on your output. So the standard shape is always:

    imports → receive frames → transform → (optional) local checks
            → (optional) test-case generation → return a frame

and LakeLogic validates + materializes what you return.

WHAT YOU RECEIVE
    good_df        the validated source frame.
    links          dict {link_name -> reference frame}, one entry per contract
                   `links:` entry, already column-projected / row-subset per the
                   contract's `columns:` / `filter:` / `query:`.
    engine         the engine this step runs against ("polars" | "spark" |
                   "duckdb" | ...) — from external_logic.engine (required).
    contract       the parsed DataContract (read-only; for metadata/args).
    **kwargs       your external_logic.args, plus add_trace / trace_step hooks.

    Frames arrive as whatever the engine produces: a Polars, Spark, or pandas
    frame. Detect once (``_frame_kind``) and branch your compute — never assume.

RETURN CONTRACT (pick ONE)
    • return a DataFrame  → LakeLogic runs quality gates + materializes it (default,
                            preferred: the contract's materialization.target handles
                            Delta / dlt / warehouse / cloud export for you).
    • return None         → you handled output yourself; set
                            external_logic.handles_output: true in the contract.
    • return a path (str) → LakeLogic loads that file as the output frame.

Returning the frame is the default because it keeps governance in LakeLogic: you
write compute, the contract owns schema/quality/PII/lineage/materialization.
"""
from typing import Any, Dict, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Entry point. The name must match external_logic.entrypoint (default "run").
# ─────────────────────────────────────────────────────────────────────────────
def run(
    good_df: Any,
    links: Optional[Dict[str, Any]] = None,
    engine: str = "polars",
    contract: Any = None,
    **kwargs: Any,
) -> Any:
    links = links or {}

    # 1) SOURCE + LINKS — your inputs. good_df is the validated source; each link
    #    is a reference frame already subset by the contract.
    kind = _frame_kind(good_df)
    drivers = links.get("drivers")  # ← rename to your link(s)

    # 2) TRANSFORM — engine-native compute. Keep it pure: in-frames → out-frame.
    out = _transform(good_df, drivers, kind)

    # 3) QUALITY (optional, local) — only checks the contract CAN'T express.
    #    Contract-level quality.row_rules / dataset_rules already run on `out`.
    _local_checks(out, kind)

    # 4) TEST-CASE GENERATION (optional) — emit fixtures for regression on request.
    if kwargs.get("emit_test_cases"):
        _emit_test_cases(good_df, out, kind, kwargs.get("test_case_dir", "."))

    # 5) MATERIALIZE — prefer returning the frame so LakeLogic governs + writes it
    #    (materialization.target → Delta / dlt / warehouse). Only write yourself for
    #    side-channels the contract can't model, then `return None` + handles_output.
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Engine-agnostic helpers. Branch on the frame kind so ONE script runs on any
# engine — this is what makes "one contract, any engine" hold for external logic.
# ─────────────────────────────────────────────────────────────────────────────
def _frame_kind(df: Any) -> str:
    """Return 'spark' | 'polars' | 'pandas' for the frame LakeLogic handed us."""
    mod = type(df).__module__
    if mod.startswith("pyspark"):
        return "spark"
    if mod.startswith("polars"):
        return "polars"
    return "pandas"


def _transform(source: Any, drivers: Any, kind: str) -> Any:
    """Left-join the source onto the `drivers` reference frame, engine-natively."""
    if drivers is None:
        return source
    if kind == "spark":
        return source.join(drivers, on="driver_id", how="left")
    if kind == "polars":
        import polars as pl

        s = source.collect() if isinstance(source, pl.LazyFrame) else source
        d = drivers.collect() if isinstance(drivers, pl.LazyFrame) else drivers
        return s.join(d, on="driver_id", how="left")
    return source.merge(drivers, on="driver_id", how="left")  # pandas


def _local_checks(out: Any, kind: str) -> None:
    """Cheap, step-specific asserts. Raise to fail the run loudly."""
    count = out.count() if kind == "spark" else len(out)
    if count == 0:
        raise ValueError("external_logic produced 0 rows — refusing to materialize empty output.")


def _emit_test_cases(source: Any, out: Any, kind: str, out_dir: str) -> None:
    """Persist a tiny input/expected fixture pair for regression testing."""
    from pathlib import Path

    Path(out_dir).mkdir(parents=True, exist_ok=True)
    if kind == "spark":
        source.limit(5).toPandas().to_json(Path(out_dir) / "input.sample.jsonl", orient="records", lines=True)
        out.limit(5).toPandas().to_json(Path(out_dir) / "expected.sample.jsonl", orient="records", lines=True)
    else:
        import polars as pl

        s = source.collect() if (kind == "polars" and hasattr(source, "collect")) else source
        o = out.collect() if (kind == "polars" and hasattr(out, "collect")) else out
        head = (lambda f: f.head(5)) if kind == "polars" else (lambda f: f.head(5))
        head(s).write_ndjson(Path(out_dir) / "input.sample.jsonl") if kind == "polars" else head(s).to_json(
            Path(out_dir) / "input.sample.jsonl", orient="records", lines=True
        )
        head(o).write_ndjson(Path(out_dir) / "expected.sample.jsonl") if kind == "polars" else head(o).to_json(
            Path(out_dir) / "expected.sample.jsonl", orient="records", lines=True
        )
