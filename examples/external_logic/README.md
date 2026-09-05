# External logic — the standard pattern

When a contract hands a step to external code (`external_logic:`), LakeLogic
passes it **every input frame** — the validated source **and** any linked
reference frames — then **governs whatever you return** (schema, quality, PII,
lineage, materialization). You write compute; the contract owns the rest.

Follow this shape (copy [`_template.py`](./_template.py)):

```
imports → receive frames → transform → (optional) quality → (optional) test-gen → return frame
```

## What the entrypoint receives

```python
def run(good_df, links=None, engine="polars", contract=None, **kwargs):
    drivers = links["drivers"]  # reference frame (already subset by the contract)
    return good_df.join(drivers, ...)  # LakeLogic validates + materializes this
```

| Arg | Meaning |
|---|---|
| `good_df` | validated **source** frame |
| `links`   | `{name: frame}` — one per contract `links:` entry, already column/row-subset |
| `engine`  | engine this step runs against (`polars`/`spark`/`duckdb`) — from `external_logic.engine`, **required** |
| `contract`| parsed contract (read-only) |
| `**kwargs`| your `external_logic.args`, plus `add_trace`/`trace_step` |

Frames arrive as whatever the engine produces — **Polars, Spark, or pandas**.
Detect the kind once and branch (see `_frame_kind`). One script, any engine.

## Return contract (pick one)

- **return a DataFrame** → LakeLogic runs quality gates + materializes it via
  `materialization.target` (Delta / dlt / warehouse / cloud export). **Default.**
- **return `None`** → you wrote output yourself; set `handles_output: true`.
- **return a path** → LakeLogic loads that file as the output frame.

Returning the frame is preferred: governance stays in the contract, and you don't
reimplement writers.

## Linking only a portion of a table

`links[]` subsets reference data at load time — you don't join the whole table:

```yaml
links:
  - name: drivers
    path: drivers.parquet
    columns: [driver_id, driver_name, driver_city]   # projection
    filter: "status = 'active'"                       # portable SQL WHERE (any engine)
    # query: "SELECT ... FROM {link} WHERE ..."       # engine-specific escape hatch
```

`filter` is portable across engines; `query` is a full `SELECT` power option whose
SQL dialect may not port. Engines that don't yet apply subsetting **fail loudly**
rather than silently loading everything.

## Run it

```bash
python examples/external_logic/run_demo.py           # polars + duckdb
LAKELOGIC_DEMO_SPARK=1 python .../run_demo.py         # + spark
```

Shows the same `enrich_trips.py` producing identical enriched output on every
engine, with the inactive driver filtered out of the link.

## Files

| File | Purpose |
|---|---|
| [`_template.py`](./_template.py) | canonical skeleton — copy this |
| [`enrich_trips.py`](./enrich_trips.py) | concrete step (source ⨝ drivers link) |
| [`trips_enriched.olc.yaml`](./trips_enriched.olc.yaml) | contract: source + link + governance |
| [`run_demo.py`](./run_demo.py) | runnable multi-engine demo |
