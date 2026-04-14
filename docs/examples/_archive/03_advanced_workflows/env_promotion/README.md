# Environment Promotion — Dev → Test → Prod

Promote contracts across environments without changing a single YAML line.

## Business Scenario

Your analytics platform has 40+ data contracts spread across Bronze, Silver, and Gold.
Each contract writes to an ADLS path that is environment-specific:

| Environment | ADLS root |
|-------------|-----------|
| `dev`       | `abfss://container@account.dfs.core.windows.net/dev/` |
| `test`      | `abfss://container@account.dfs.core.windows.net/test/` |
| `prod`      | `abfss://container@account.dfs.core.windows.net/prod/` |

Without a promotion mechanism, engineers must either maintain three copies of every
contract YAML, or manually search-and-replace paths before each deployment — both
patterns break at scale and introduce drift.

## Value Proposition

- **One YAML per contract** — no copies, no drift between environments
- **Zero code changes on promotion** — flip an env var in your CI/CD pipeline
- **`server.path` stays as the prod default** — no extra configuration for production
- **Per-environment `format` override** — run CSV locally in dev, Parquet in prod
- **Instant rollback** — change `LAKELOGIC_ENV` back; every contract re-routes
- **Works with `PipelineDriver`, `DataProcessor`, and `DataGenerator`** — consistent resolution everywhere

## Files

```
env_promotion/
├── README.md
├── contracts/
│   ├── bronze_orders.yaml          # Bronze layer — landing → bronze
│   ├── silver_orders.yaml          # Silver layer — bronze → cleaned/enriched
│   └── gold_daily_revenue.yaml     # Gold layer — aggregated KPI
├── data/
│   └── orders_raw.csv              # Sample source data (local dev)
└── env_promotion.ipynb             # Interactive walkthrough
```

## How It Works

Each contract carries a `server` block (production default) and an `environments` map:

```yaml
server:
  type: adls
  path: abfss://silver@myaccount.dfs.core.windows.net/prod/orders/
  format: parquet

environments:
  dev:
    path: ./data/silver_orders.parquet   # local file in dev
  test:
    path: abfss://silver@myaccount.dfs.core.windows.net/test/orders/
```

Activate an environment:

```bash
# Linux / macOS / GitHub Actions / ADF activity
export LAKELOGIC_ENV=dev
lakelogic-driver --registry contracts/_registry.yaml --layers bronze,silver,gold

# Windows CMD
set LAKELOGIC_ENV=test
lakelogic-driver --registry contracts/_registry.yaml --layers bronze,silver,gold
```

Or in Python:

```python
import os
os.environ["LAKELOGIC_ENV"] = "dev"

contract = DataContract(**yaml.safe_load(open("contracts/silver_orders.yaml")))
server = contract.effective_server()
print(server.path)   # → ./data/silver_orders.parquet (local dev path)
```

## When `LAKELOGIC_ENV` is unset (production)

When no environment variable is set, `contract.effective_server()` returns `contract.server`
unchanged — the production ADLS path is used automatically. No special production
configuration is needed.

## CI/CD Integration (GitHub Actions example)

```yaml
# .github/workflows/deploy_test.yml
- name: Run LakeLogic pipeline in TEST
  env:
    LAKELOGIC_ENV: test
    AZURE_STORAGE_ACCOUNT_KEY: ${{ secrets.AZURE_STORAGE_KEY_TEST }}
  run: |
    lakelogic-driver \
      --registry contracts/_registry.yaml \
      --layers bronze,silver,gold \
      --window last_success
```

## Next Steps

- Open `env_promotion.ipynb` for the interactive end-to-end walkthrough
- See `07_production/ci_cd/` for a full deployment pipeline template
- See `shared_governance_scale/` for running 500+ contracts with a shared policy pack
