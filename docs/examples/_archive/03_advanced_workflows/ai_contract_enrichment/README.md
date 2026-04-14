# AI-Powered Contract Enrichment & Edge-Case Generation

## Business Scenario

You're a **data platform team** onboarding 50+ data sources from a recent acquisition.
Each source has CSV/Parquet files with no documentation, no schema definitions, and no
quality rules. Manual contract writing for each dataset would take weeks.

**With LakeLogic AI, you can:**

1. **Auto-generate contracts** from raw files with field descriptions, PII detection,
   and SQL quality rules — in seconds, not days
2. **Generate realistic test data** with AI-powered edge cases to stress-test your
   quarantine logic before production deployment
3. **Validate contracts** against real data to catch quality issues early

## Business Value

| Without AI | With AI |
|:---|:---|
| Manual schema documentation (hours/table) | Auto-generated field descriptions |
| PII discovery via spreadsheets | LLM-powered PII detection with remediation |
| Quality rules written post-incident | Proactive SQL rules suggested from data patterns |
| Edge cases from imagination only | LLM suggests realistic boundary/format violations |
| Weeks to onboard new sources | Minutes per dataset |

## Prerequisites

```bash
pip install "lakelogic[all]"
pip install openai        # or: pip install anthropic
export OPENAI_API_KEY=sk-...
```

## Quick Start

### 1. AI-Enriched Contract from Raw Data (CLI)

```bash
# Standard bootstrap — schema only, no descriptions
lakelogic bootstrap \
    --landing data/ \
    --output-dir contracts/ \
    --registry contracts/_registry.yaml

# AI-enriched — adds descriptions, PII flags, SQL rules
lakelogic bootstrap \
    --landing data/ \
    --output-dir contracts/ \
    --registry contracts/_registry.yaml \
    --ai \
    --ai-provider openai \
    --ai-model gpt-4o-mini
```

### 2. AI Edge-Case Test Data (CLI)

```bash
# Standard generation — heuristic bad data
lakelogic generate \
    --contract contracts/ecommerce_orders.yaml \
    --rows 1000 \
    --invalid-ratio 0.1

# AI-powered — LLM suggests realistic edge cases
lakelogic generate \
    --contract contracts/ecommerce_orders.yaml \
    --rows 1000 \
    --invalid-ratio 0.1 \
    --ai \
    --preview 10
```

## Files

| File | Description |
|:---|:---|
| `contract_ecommerce.yaml` | E-commerce orders contract for enrichment demo |
| `data/raw_orders.csv` | Sample raw orders data (20 rows) |
| `ai_enrich_demo.py` | Python API: enrich a contract with AI |
| `ai_edge_cases_demo.py` | Python API: generate edge-case test data |
| `README.md` | This file |
