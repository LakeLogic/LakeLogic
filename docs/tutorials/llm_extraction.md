# Unstructured Data Processing via LLM Extraction

LakeLogic can extract structured data from unstructured text using LLM providers — turning free-text fields, PDFs, images, and audio into typed, validated columns.

---

## Quick Start

```yaml
version: 1.0.0
info:
  title: "Support Ticket Extraction"

dataset: "support_tickets"

source:
  type: "landing"
  path: "data/tickets/*.csv"

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
```

```python
from lakelogic import DataProcessor

processor = DataProcessor(engine="polars", contract="contracts/tickets.yaml")
good_df, bad_df = processor.run_source("data/tickets/batch_001.csv")
# → ticket_body is sent to GPT-4o-mini
# → sentiment and issue_category columns are populated
```

---

## Supported Providers

### Cloud Providers (Require API Key)

| Provider | Env Var | Models |
|----------|---------|--------|
| `openai` | `OPENAI_API_KEY` | `gpt-4o`, `gpt-4o-mini` |
| `anthropic` | `ANTHROPIC_API_KEY` | `claude-sonnet-4-20250514`, `claude-3-haiku` |
| `azure_openai` | `AZURE_OPENAI_API_KEY` + `AZURE_OPENAI_ENDPOINT` | Azure-hosted OpenAI models |
| `google` | `GOOGLE_API_KEY` | `gemini-2.0-flash`, `gemini-pro` |
| `bedrock` | AWS credentials (boto3) | Amazon Bedrock models |

### Local Providers (No API Key)

| Provider | Setup | Models |
|----------|-------|--------|
| `ollama` | Install [Ollama](https://ollama.com), pull a model | `llama3.1`, `mistral`, `phi3`, `codellama` |
| `local` | `pip install lakelogic[local]` | HuggingFace Transformers (Phi-3-mini default) |

Override Ollama URL:
```bash
export OLLAMA_BASE_URL="http://localhost:11434"
```

Global provider override (applies to all contracts):
```bash
export LAKELOGIC_AI_PROVIDER=ollama
export LAKELOGIC_AI_MODEL=llama3.1
```

---

## Extraction Configuration

### Prompt Templates

```yaml
extraction:
  provider: "openai"
  model: "gpt-4o-mini"
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

### Output Schema with Validation

```yaml
extraction:
  output_schema:
    - name: "brand"
      type: "string"
      extraction_task: "ner"           # Named entity recognition

    - name: "price"
      type: "float"
      extraction_task: "extraction"    # Value extraction

    - name: "condition"
      type: "string"
      extraction_task: "classification"
      accepted_values: ["new", "used", "refurbished"]
      extraction_examples: ["new", "like new", "refurbished"]
```

`extraction_task` options:
- `classification` — pick from `accepted_values`
- `extraction` — extract a specific value
- `ner` — named entity recognition
- `summarization` — summarize text

### Confidence Scoring

```yaml
extraction:
  confidence:
    enabled: true
    method: "field_completeness"
    column: "_lakelogic_extraction_confidence"
```

Methods:
- `field_completeness` — ratio of non-null extracted fields
- `log_probs` — LLM log probabilities (OpenAI only)
- `self_assessment` — ask the LLM to rate its confidence
- `consistency` — run extraction twice, compare results

---

## Cost Controls

```yaml
extraction:
  max_cost_per_run: 50.00      # USD budget cap
  max_rows_per_run: 10000      # Row limit per execution
  batch_size: 50               # Rows per API call
  concurrency: 5               # Parallel API calls

  retry:
    max_attempts: 3
    backoff: "exponential"
    initial_delay: 1.0

  fallback_model: "gpt-4o-mini"
  fallback_provider: "openai"
```

---

## PII Safety

```yaml
extraction:
  redact_pii_before_llm: true
  pii_fields: ["email", "phone", "ssn"]
```

When enabled, PII fields are redacted *before* the text is sent to the LLM. The extracted output never sees raw PII values.

---

## Preprocessing: PDF, Image, Audio, Video

For non-text sources, add a `preprocessing` block:

### PDF Extraction

```yaml
extraction:
  provider: "openai"
  model: "gpt-4o"

  preprocessing:
    content_type: "pdf"
    ocr:
      enabled: true
      engine: "tesseract"        # or azure_di, textract, google_vision
      language: "eng"
    chunking:
      strategy: "page"           # or paragraph, sentence, fixed_size
      max_chunk_tokens: 4000
      overlap_tokens: 200
```

### Image OCR

```yaml
extraction:
  preprocessing:
    content_type: "image"
    ocr:
      enabled: true
      engine: "tesseract"
```

### Supported Content Types

| Type | Engines | Notes |
|------|---------|-------|
| `pdf` | tesseract, azure_di, textract | Page-level chunking |
| `image` | tesseract, azure_di | OCR to text first |
| `audio` | whisper | Transcription → extraction |
| `video` | whisper | Audio track → transcription |
| `html` | built-in | HTML → clean text |
| `email` | built-in | Parse headers + body |
| `text` | built-in | Direct extraction |

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

  prompt_template: |
    Extract invoice details from this document page.
    Return JSON with the fields specified.

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
      extraction_task: "classification"
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
```

```python
from lakelogic import DataProcessor

processor = DataProcessor(engine="polars", contract="contracts/invoices.yaml")
good_df, bad_df = processor.run_source("data/invoices/")

print(f"Extracted {len(good_df)} invoices, {len(bad_df)} failed quality checks")
```
