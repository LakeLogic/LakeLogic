# LLM Extraction — Unstructured Data Processing
<!-- markdownlint-disable MD013 -->

LakeLogic extracts structured data from PDFs, images, and free-text using a contract-first approach. Define *what* to extract in `model.fields` and *how* in `extraction:` — LakeLogic picks the right library, extracts, validates, and materialises. No custom parsers. No glue code.

---

## How It Works

```
  Contract                   DataProcessor.run()
┌────────────────────┐      ┌──────────────────────────────────────────────┐
│ model.fields       │      │ 1. Preprocess  (pdfplumber / OCR / spaCy)    │
│  → what to extract │─────▶│ 2. Extract     (LLM prompt / NER / tables)   │
│ extraction.config  │      │ 3. Validate    (quality rules + quarantine)   │
│  → how to extract  │      │ 4. Materialise (parquet / delta / CSV)        │
└────────────────────┘      └──────────────────────────────────────────────┘
```

The same pattern runs every provider. For text DataFrames pass directly; for binary
files (PDF, image) pass a DataFrame with a `file_path` column:

```python
import polars as pl
from lakelogic import DataProcessor

proc = DataProcessor("contract.yaml", engine="polars")

# Text / CSV source — pass the DataFrame directly
good, bad = proc.run(tickets_df)

# Binary source (PDF, image) — point the engine at the file via a DataFrame
files_df = pl.DataFrame({"file_path": ["invoice.pdf"]})
good, bad = proc.run(files_df)   # contract.extraction.preprocessing.file_column: file_path
```

---

## Supported Providers

| Provider | Install | Input | Cost |
| -------- | ------- | ----- | ---- |
| `regex` | built-in | Semi-structured text | $0, offline, **deterministic** |
| `local` (pdfplumber) | `lakelogic[extraction-ocr]` | PDF, DOCX | $0, offline |
| `rapidocr` | `lakelogic[extraction-ocr]` | Scanned images | $0, ONNX, no torch |
| `spacy` | `lakelogic[nlp]` | Free text | $0, local NER |
| `unstructured` | `lakelogic[extraction]` | PDF, DOCX, HTML | $0, offline |
| `openai` | `lakelogic[ai]` | Any | Per token |
| `anthropic` | `lakelogic[ai]` | Any | Per token |
| `ollama` | `lakelogic[ai]` | Any | $0, local LLM |
| `azure_openai` | `lakelogic[ai]` | Any | Per token |
| `bedrock` | `lakelogic[ai]` | Any | Per token |

The `extraction:` block is run by the pipeline itself — extracted fields are then
validated, quality-checked, and materialized by the contract like any other column.

### `regex` — deterministic offline extraction

For semi-structured text where an LLM is overkill, `provider: regex` maps each output
field to a regex (one capture group) applied to `text_column`. Fully deterministic and
offline — the same output on every engine (Polars, DuckDB, Spark), no API key, no cost.

```yaml
model:
  fields:
    - { name: note, type: string }
    - { name: temperature, type: integer }   # extracted, then cast + validated
    - { name: status, type: string }
extraction:
  provider: regex
  text_column: note
  patterns:
    temperature: 'temperature=(\d+)'
    status: 'status=(\w+)'
```

A field whose pattern does not match yields `null`. Because it is deterministic, `regex`
is the provider used to conformance-test the extraction path across engines; LLM providers
ride the same wiring but are non-deterministic and therefore out of conformance scope.

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

`model.fields` is the single source of truth for field names and types. `output_schema` only
lists fields that need extraction-specific hints — anything not listed defaults to
`extraction_task: extraction`. You never need to repeat `type` in `output_schema`.

```yaml
model:
  fields:
    - name: brand
      type: string
    - name: price
      type: float
    - name: condition
      type: string

extraction:
  # Only fields needing non-default hints appear here — types are inherited from model.fields
  output_schema:
    - name: brand
      extraction_task: ner           # named-entity recognition, not plain extraction

    - name: condition
      extraction_task: classification
      accepted_values: ["new", "used", "refurbished"]
      extraction_examples: ["new", "like new", "refurbished"]

    # 'price' omitted — defaults to extraction_task: extraction
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
- [Tutorial: LLM Extraction](index.md)

---

*Last Updated: March 2026*
