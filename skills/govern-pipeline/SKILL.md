---
name: govern-pipeline
description: Govern a data pipeline's tables with LakeLogic data contracts. Use when the user adds or changes a table, builds a pipeline (dlt, dbt, Spark, SQL), changes a schema, adds a data quality rule, or asks to "govern", "add data quality", "add a data contract" or "make this pipeline production-ready". Drafts and validates Open Lakehouse Contracts, checks what a change affects, and tests quality rules on generated data with deliberately broken rows before they ship.
---

# Govern a pipeline with LakeLogic

LakeLogic Core runs **Open Lakehouse Contracts**: each table's schema, keys, quality rules and
delivery, in one YAML file. Install: `pip install lakelogic`. Full guide: `lakelogic agent-guide`.

## 1. Every table the pipeline writes gets a contract
- Write the contract before (or alongside) the pipeline code. Learn the keys with `olc guide`,
  then `olc reference <Section>` for the section you need.
- From existing files in a landing zone, `lakelogic bootstrap` drafts contracts.
- From a dbt project, `lakelogic import-dbt` converts `schema.yml` / `sources.yml`.
- Check every contract: `lakelogic validate --contract <file>`. Fix what it reports.

## 2. A schema change starts in the contract
- Change the contract first, then the pipeline code.
- Before renaming or retyping a column, find the contracts that read this table (their `source`
  or `upstream` names it) and tell the user what the change affects.

## 3. A quality rule is tested before it ships
1. `lakelogic generate --contract <file> --rows 500 --invalid-ratio 0.1 --output test.parquet`
   (rows that deliberately break the contract's rules, plus a generation report).
2. `lakelogic run --contract <file> --source test.parquet` (good rows and quarantined rows).
3. `lakelogic assert --report <generation report> --expect-all-invalid-quarantined`.

If a rule catches nothing, the rule is wrong. Fix it and test again before committing.
Rules that compare two columns (`dropoff_at >= pickup_at`) are broken too (LakeLogic 2.6.2+).

## 4. Business rules are the owner's decision
Propose allowed values, ranges and thresholds; never present values seen in a sample as the
rule. Say which rules came from the source's declared constraints, which from the sample, and
which the data owner must confirm.

## 5. Before you finish
- `lakelogic validate` passes on every changed contract.
- Generated output was regenerated from the contract, not edited by hand.
- Tell the user what you governed: tables with contracts, rules tested (with rows caught), and
  the decisions waiting for the data owner.

## Keep the rules in the repository
`lakelogic agent-guide --write AGENTS.md` (and `--write CLAUDE.md`) adds these standing rules to
the repository's agent instructions, inside a marked LakeLogic block, so every agent working
here follows them without being asked.
