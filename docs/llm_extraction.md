# LLM Extraction — Unstructured Data Processing
<!-- markdownlint-disable MD013 -->

LakeLogic can extract structured data from unstructured text, PDFs, images, audio, and video using LLM providers. The extraction pipeline integrates with the same quality rules, materialization, and lineage as any other contract.

---

## Overview

```
  Raw Source          LLM Extraction         Quality Rules        Output
┌──────────┐      ┌─────────────────┐     ┌──────────────┐    ┌─────────┐
│ CSV/PDF/ │ ──→  │ Preprocess      │ ──→ │ Validate     │ ──→│ Delta/  │
│ Image/   │      │ → Prompt        │     │ Quarantine   │    │ Parquet │
│ Audio    │      │ → LLM API       │     │ Materialize  │    │         │
└──────────┘      │ → Parse JSON    │     └──────────────┘    └─────────┘
                  └─────────────────┘
```

---

## Quick Start

```yaml
# contracts/ticket_extraction.yaml
version: 1.0.0
info:
  title: "Support Ticket Extraction"
dataset: "support_tickets"

source:
  type: "landing"
  path: "data/tickets/*.csv"

extraction:
  provider: "openai"
  model: "gpt-4o-mini"
  temperature: 0.1
  text_column: "ticket_body"
  output_schema:
    - name: "sentiment"
      type: "string"
      accepted_values: ["positive", "neutral", "negative"]
    - name: "issue_category"
      type: "string"
      extraction_examples: ["billing", "technical", "account"]

model:
  fields:
    - name: ticket_id
      type: integer
    - name: ticket_body
      type: string
    - name: sentiment
      type: string
    - name: issue_category
      type: string

quality:
  row_rules:
    - not_null: ticket_id
    - accepted_values:
        field: sentiment
        values: ["positive", "neutral", "negative"]
```

```python
from lakelogic import DataProcessor

processor = DataProcessor(engine="polars", contract="contracts/ticket_extraction.yaml")
good_df, bad_df = processor.run_source("data/tickets/batch_001.csv")
```

---

## Supported Providers

### Cloud Providers

| Provider | Env Var | Example Models |
|----------|---------|----------------|
| `openai` | `OPENAI_API_KEY` | `gpt-4o`, `gpt-4o-mini` |
| `anthropic` | `ANTHROPIC_API_KEY` | `claude-sonnet-4-20250514`, `claude-3-haiku` |
| `azure_openai` | `AZURE_OPENAI_API_KEY` + `AZURE_OPENAI_ENDPOINT` | Azure-hosted OpenAI models |
| `google` | `GOOGLE_API_KEY` | `gemini-2.0-flash`, `gemini-pro` |
| `bedrock` | AWS credentials (boto3) | Amazon Bedrock models |

### Local Providers (No API Key, No Data Leaves Your Network)

| Provider | Setup | Example Models |
|----------|-------|----------------|
| `ollama` | Install [Ollama](https://ollama.com), pull a model | `llama3.1`, `mistral`, `phi3` |
| `local` | `pip install lakelogic[local]` | HuggingFace Transformers (Phi-3-mini default) |

```bash
# Override default provider globally
export LAKELOGIC_AI_PROVIDER=ollama
export LAKELOGIC_AI_MODEL=llama3.1

# Override Ollama URL
export OLLAMA_BASE_URL="http://localhost:11434"
```

---

## Configuration Reference

### Prompt Templates

Use Jinja2 templates with column names as variables:

```yaml
extraction:
  text_column: "description"
  context_columns: ["category", "date"]

  prompt_template: |
    Given this product description:
    {{ description }}

    Category: {{ category }}
    Date: {{ date }}

    Extract the following fields as JSON.

  system_prompt: "You are a product data extraction assistant."
```

### Output Schema

Define the fields to extract, with extraction task hints:

```yaml
extraction:
  output_schema:
    - name: "brand"
      type: "string"
      extraction_task: "ner"

    - name: "price"
      type: "float"
      extraction_task: "extraction"

    - name: "condition"
      type: "string"
      extraction_task: "classification"
      accepted_values: ["new", "used", "refurbished"]
      extraction_examples: ["new", "like new", "refurbished"]
```

| `extraction_task` | Description |
|-------------------|-------------|
| `classification` | Pick from `accepted_values` |
| `extraction` | Extract a specific value |
| `ner` | Named entity recognition |
| `summarization` | Summarize text |

### Confidence Scoring

```yaml
extraction:
  confidence:
    enabled: true
    method: "field_completeness"
    column: "_lakelogic_extraction_confidence"
```

| Method | Description |
|--------|-------------|
| `field_completeness` | Ratio of non-null extracted fields |
| `log_probs` | LLM log probabilities (OpenAI only) |
| `self_assessment` | Ask the LLM to rate its own confidence |
| `consistency` | Run twice, compare results |

---

## Cost & Safety Controls

```yaml
extraction:
  # Budget limits
  max_cost_per_run: 50.00       # USD cap
  max_rows_per_run: 10000       # Row limit

  # Throughput
  batch_size: 50                # Rows per API call
  concurrency: 5                # Parallel API calls

  # Retry
  retry:
    max_attempts: 3
    backoff: "exponential"
    initial_delay: 1.0

  # Fallback
  fallback_model: "gpt-4o-mini"
  fallback_provider: "openai"

  # PII safety — redact before sending to LLM
  redact_pii_before_llm: true
  pii_fields: ["email", "phone", "ssn"]
```

---

## Preprocessing: PDF, Image, Audio, Video

For non-text sources, add a `preprocessing` block to convert raw files into text before LLM extraction.

### PDF

```yaml
extraction:
  preprocessing:
    content_type: "pdf"
    ocr:
      enabled: true
      engine: "tesseract"          # or azure_di, textract, google_vision
      language: "eng"
    chunking:
      strategy: "page"             # or paragraph, sentence, fixed_size
      max_chunk_tokens: 4000
      overlap_tokens: 200
```

### Image

```yaml
extraction:
  preprocessing:
    content_type: "image"
    ocr:
      enabled: true
      engine: "tesseract"
```

### Supported Content Types

| Type | Preprocessing | Notes |
|------|--------------|-------|
| `pdf` | OCR + chunking | Page-level chunking recommended |
| `image` | OCR → text | tesseract, Azure DI, Textract |
| `audio` | Whisper transcription | Audio → text → extraction |
| `video` | Audio track → Whisper | Video → audio → text → extraction |
| `html` | Built-in parser | HTML → clean text |
| `email` | Built-in parser | Parse headers + body |
| `text` | None | Direct extraction |

---

## End-to-End Example: Invoice Processing

```yaml
version: 1.0.0
info:
  title: "Bronze Invoice Extraction"
  target_layer: "bronze"

dataset: "invoices"

source:
  type: "landing"
  path: "data/invoices/*.pdf"

extraction:
  provider: "openai"
  model: "gpt-4o"
  temperature: 0.0

  preprocessing:
    content_type: "pdf"
    ocr:
      enabled: true
      engine: "azure_di"
    chunking:
      strategy: "page"
      max_chunk_tokens: 4000

  output_schema:
    - name: "invoice_number"
      type: "string"
      extraction_task: "extraction"
    - name: "vendor_name"
      type: "string"
      extraction_task: "ner"
    - name: "total_amount"
      type: "float"
      extraction_task: "extraction"
    - name: "currency"
      type: "string"
      accepted_values: ["USD", "EUR", "GBP"]
    - name: "invoice_date"
      type: "date"
      extraction_task: "extraction"

  confidence:
    enabled: true
    method: "field_completeness"
    column: "_extraction_confidence"

  max_cost_per_run: 25.00
  batch_size: 10
  concurrency: 3
  redact_pii_before_llm: true
  pii_fields: ["bank_account", "tax_id"]

model:
  fields:
    - name: invoice_number
      type: string
    - name: vendor_name
      type: string
    - name: total_amount
      type: float
    - name: currency
      type: string
    - name: invoice_date
      type: date
    - name: _extraction_confidence
      type: float

quality:
  row_rules:
    - not_null: invoice_number
    - not_null: total_amount
    - accepted_values:
        field: currency
        values: ["USD", "EUR", "GBP"]

materialization:
  strategy: "append"
  path: "s3://bronze/invoices"
  format: "delta"

lineage:
  enabled: true
```

---

## Related Documentation

- [Contract Template — Section 19: LLM Extraction](contract_template.md)
- [Capabilities](capabilities.md)
- [Tutorial: LLM Extraction](tutorials/llm_extraction.md)

---

*Last Updated: March 2026*
