# Installation

LakeLogic is designed to be lightweight — install only what you need and scale up anytime.

> **Analogy:** Think of LakeLogic like a Swiss Army knife. The base package gives you the blade (Polars engine). Extras snap on tools — Spark for big data, DuckDB for analytics, notifications for alerting, and database connectors for CDC.

---

## Quick Install

```bash
# Recommended — fast, conflict-free
uv pip install "lakelogic[polars]"

# Or with pip
pip install "lakelogic[polars]"
```

That's it. You're ready to run your first contract.

---

## Choose Your Extras

### Engines — How You Process Data

Pick the engine that matches your workload:

| Extra | What You Get | Best For |
| :--- | :--- | :--- |
| `[polars]` | Polars + Delta Lake + Excel/XML readers | **Start here** — fast local processing, notebooks, CI/CD |
| `[spark]` | PySpark | Petabyte-scale Lakehouse jobs on Databricks |
| `[duckdb]` | DuckDB + Pandas + PyArrow + Delta | Analytical SQL workloads, fast local queries |
| `[engines]` | All of the above | Want everything — switch engines freely |

### Data Sources — Where You Read From

| Extra | What You Get | Best For |
| :--- | :--- | :--- |
| `[databases]` | SQL Server, PostgreSQL, MySQL, MongoDB | CDC ingestion from operational databases |
| `[azuresql]` | SQL Server + Azure AD auth | Azure SQL Database / Managed Instance |
| `[postgresql]` | PostgreSQL + Azure AD auth | PostgreSQL CDC |
| `[mysql]` | MySQL connector | MySQL CDC · 🔜 *connector wrapper coming soon* |
| `[mongodb]` | MongoDB connector | Document store ingestion · 🔜 *connector wrapper coming soon* |
| `[api]` | REST API client | Pulling data from HTTP APIs |
| `[sftp]` | SFTP/SSH client | File-based ingestion from remote servers |
| `[dlt]` | dlt + PyArrow | Declarative API ingestion · 🔜 *contract integration coming soon* |

### Streaming — Real-Time Processing

| Extra | What You Get | Best For |
| :--- | :--- | :--- |
| `[streaming]` | Bytewax + Pathway + Kafka + SSE + WebSocket | Full real-time stack |
| `[bytewax]` | Bytewax (Rust-based stream processor) | High-performance streaming |
| `[pathway]` | Pathway (real-time SQL transforms) | SQL-first streaming |
| `[kafka]` | Apache Kafka client | Kafka-based event pipelines |
| `[sse]` | Server-Sent Events client | Wikimedia, live feeds |
| `[websocket]` | WebSocket client | Coinbase, Binance, live APIs |

### Cloud & Warehouses — Where You Deploy

| Extra | What You Get | Best For |
| :--- | :--- | :--- |
| `[delta]` | Delta Lake + Azure + AWS + GCP storage | Spark-free Delta table reads/writes |
| `[azure]` | Azure AD + Key Vault + Blob Storage + Databricks SDK | Azure-native deployments |
| `[snowflake]` | Snowflake connector | Run contracts directly in Snowflake |
| `[bigquery]` | BigQuery client | Run contracts directly in BigQuery |
| `[cloud]` | All cloud providers + messaging + warehouses | Multi-cloud deployments |

### Notifications & Secrets — Who Gets Alerted

| Extra | What You Get | Best For |
| :--- | :--- | :--- |
| `[notifications]` | Apprise + Jinja2 + Key Vault + Secrets Manager + HVAC | Slack, Teams, Email, PagerDuty alerts |
| `[notify]` | Apprise + HashiCorp Vault | Lightweight notification setup |
| `[azure_messaging]` | Azure Service Bus + Event Grid | Azure-native event routing |
| `[aws_messaging]` | AWS SQS/SNS via Boto3 | AWS-native event routing |
| `[gcp_messaging]` | Google Cloud Pub/Sub | GCP-native event routing |

### AI & Profiling — Smart Contract Generation

| Extra | What You Get | Best For |
| :--- | :--- | :--- |
| `[ai]` | OpenAI + Anthropic + Google GenAI | AI-powered contract bootstrap (`lakelogic bootstrap --ai`) |
| `[pii]` | DataProfiler + Presidio | Automatic PII detection, profiling, and anonymization |

### Bundles — Pre-Packaged Combinations

| Extra | What You Get | Best For |
| :--- | :--- | :--- |
| `[all]` | Base install (backwards compat) | Minimal footprint |
| `[enterprise]` | Spark + PII + Bytewax + notebooks | Full-stack enterprise deployment |
| `[cli]` | Typer CLI framework | `lakelogic` command-line tool |

---

## Engine Selection Tips

> **Rule of thumb:** Start with `[polars]`. If you hit scale limits, switch to `[spark]`. If you want SQL-native analytics, try `[duckdb]`. Your contracts work on all three — zero code changes.

| Scenario | Recommended Engine |
| :--- | :--- |
| Local development, notebooks, CI/CD | **Polars** — fastest startup, smallest footprint |
| Databricks, Unity Catalog, petabyte-scale | **Spark** — distributed, Delta-native |
| Analytical queries, fast local SQL | **DuckDB** — SQL-first, great for ad-hoc work |
| Don't know yet | **Polars** — you can always switch later |

After installing, warm your environment for Delta Lake support:

```bash
lakelogic setup-oss
```

---

## Developer Installation

If you want to contribute to LakeLogic:

```bash
git clone https://github.com/lakelogic/LakeLogic.git
cd lakelogic
uv sync
uv run pytest
```

**Requirements:** Python 3.9+ · Windows, macOS, or Linux
