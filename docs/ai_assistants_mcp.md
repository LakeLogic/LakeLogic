# AI assistants (MCP)

`lakelogic-mcp` is an MCP (Model Context Protocol) server that lets an AI assistant, such as
Claude Code, Claude Desktop or Cursor, check the contracts it writes with LakeLogic itself, rather
than guess. It runs on your machine, needs no LakeLogic account, and reads only the project folder
it was started in.

## Install

```bash
claude mcp add lakelogic -- uvx lakelogic-mcp
```

Any other MCP client: run `uvx lakelogic-mcp` as a stdio server. `--root <folder>` serves a folder
other than the current one.

## Tools

| Tool | What it does |
|---|---|
| `validate_contract` | Is this a valid Open Lakehouse Contract? Each error's location and problem |
| `lint_contract` | Is it a good one? Governance findings from `lakelogic lint`, each with its fix |
| `explain_rule` | Why a lint rule exists: what it checks, why it matters, a failing and a fixed contract |
| `contract_reference` | How one part of a contract is written (`quality`, `materialization`, ...) |
| `export_odcs` | The contract as an Open Data Contract Standard document |
| `generate_test_data` | Synthetic data shaped by the contract, with a share of rows that deliberately break its rules |
| `run_contract` | Runs the contract on a local CSV or Parquet file with Polars or DuckDB: rows passed, rows quarantined, and why. Nothing is written to the contract's target. |

## Example

> Generate 200 rows of test data for `contracts/silver_trips.yaml` with 15% invalid rows, run the
> contract on it with Polars and DuckDB, and tell me what was quarantined and why.

The assistant writes the data, runs the contract on both engines and reports the good and
quarantined counts and each rejection reason.

## On the LakeLogic platform

With a LakeLogic account, an assistant can also work with your governed estate: assess a contract's
quality gaps, propose a rule, test it in a sandbox, and have a person approve it, with the evidence
recorded. See [Connect your AI assistant (MCP)](https://www.lakelogic.io/docs/ai-assistants) on the
platform docs.

Source: [github.com/LakeLogic/lakelogic-mcp](https://github.com/LakeLogic/lakelogic-mcp).
