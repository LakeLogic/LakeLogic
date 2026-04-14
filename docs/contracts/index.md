# LakeLogic Contract Reference

## What Is LakeLogic?

> **The short version:** You describe your data in YAML. LakeLogic builds the pipeline.

Think of LakeLogic like a **building blueprint system for data**. Just as an architect draws blueprints that builders follow to construct a house — with exact specifications for every room, door, and electrical outlet — you write YAML contracts that LakeLogic follows to build data pipelines.

No construction crew (developers writing Spark code) needed. The blueprint IS the building instruction.

---

## The Three Levels of Configuration

LakeLogic organizes your data estate like a company org chart:

```
🏢 Domain (Marketing, Sales, Finance)
│   "Who owns this data?"
│   → _domain.yaml — ownership, SLOs, contacts, alerts
│
├── 🏭 System (Google Analytics, Salesforce, SAP)
│   "Where does this data come from?"
│   → _system.yaml — storage, environments, settings
│
└── 📄 Data Product (events, customers, orders)
    "What does this specific table look like?"
    → entity_v1.0.yaml — schema, quality rules, transforms
```

> **Analogy:** A domain is like a department (Marketing). A system is like a tool that department uses (Google Analytics). A data product is like a specific report from that tool (website sessions).

### How Inheritance Works

Settings flow downward — configure once at the top, override only where needed:

```
Domain sets "alert the on-call team on failures"
  ↓ inherited by all systems in the domain
System sets "use Azure storage in production"
  ↓ inherited by all contracts in the system
Contract sets "this specific table needs SCD2 history tracking"
```

This means you **write less YAML** and **get consistent governance** across hundreds of data products.

---

## Which Page Do I Need?

| I'm a... | I want to... | Go to |
| --- | --- | --- |
| **Data Lead** | Set up domain ownership & SLOs | [Domain Config](domain_config.md) |
| **Platform Engineer** | Configure storage & cloud environments | [System Config](system_config.md) |
| **Data Engineer** | Create my first data contract | [Data Product Contracts](data_product_contracts/index.md) |
| **Data Engineer** | Ingest files or tables | [Ingestion](data_product_contracts/ingestion.md) |
| **Data Engineer** | Choose how to track "what's new" | [Watermark Strategies](data_product_contracts/watermark_strategies.md) |
| **Data Engineer** | Add transforms (rename, join, SQL) | [Transformations](data_product_contracts/transformations.md) |
| **Data Engineer** | Add quality checks | [Quality](data_product_contracts/quality.md) |
| **Data Engineer** | Configure how data is written | [Materialization](data_product_contracts/materialization.md) |
| **Data Modeller** | Build SCD2 dimensions or fact tables | [Dimensional Modeling](data_product_contracts/dimensional_modeling.md) |
| **Data Engineer** | Define schema, PII masking | [Schema & Model](schema_model.md) |
| **Data Lead** | Set up alerting | [Notifications](notifications.md) |
| **Compliance** | Add GDPR / EU AI Act metadata | [Compliance](compliance.md) |
| **Anyone** | Understand versioning | [Versioning](versioning.md) |
| **Power User** | See everything in one file | [Complete Template](../contract_template.md) |

---

## Why Data Mesh?

LakeLogic implements all four principles from [ThoughtWorks' Data Mesh framework](https://www.thoughtworks.com/en-gb/insights/articles/recommendations-for-a-successful-data-mesh-implementation):

| Principle | What It Means (Plain English) | How LakeLogic Enables This |
| --- | --- | --- |
| **Domain Ownership** | The people closest to the data own it | `_domain.yaml` names the owner, their contacts, and cost centre |
| **Data as a Product** | Treat each dataset like a product with quality guarantees | Each contract declares schema, quality rules, and SLOs |
| **Self-Serve Platform** | Give teams tools so they don't wait on a central team | Write YAML → run pipeline. No tickets, no handoffs |
| **Federated Governance** | Consistent rules without a bottleneck | Domain-level SLOs inherited automatically by every table |

> **Bottom line:** LakeLogic lets each team own their data, while the platform ensures nobody ships garbage to production.
