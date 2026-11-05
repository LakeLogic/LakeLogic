"""Cloud model readers for unstructured extraction: each provider builds the right client and
sends the request its API accepts. Clients are faked — no network, no keys.

Found 2026-11-04: azure_openai sent the Azure key to api.openai.com (401), google had no
extraction function, bedrock was named in the docstring only.
"""

from types import SimpleNamespace

import pytest

from lakelogic.core.models import ExtractionConfig
from lakelogic.engines import llm


def _cfg(provider, model="m", **extra):
    return ExtractionConfig(provider=provider, model=model, prompt_template="p", response_format="json", **extra)


# ── Azure OpenAI ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://res.openai.azure.com",
        "https://res.openai.azure.com/",
        "https://res.openai.azure.com/openai/v1",
        "https://res.openai.azure.com/openai/v1/chat/completions",
    ],
)
def test_azure_base_url_is_the_resource_v1_api(endpoint):
    assert llm._azure_base_url(endpoint) == "https://res.openai.azure.com/openai/v1"


def test_azure_client_targets_the_resource_not_api_openai_com(monkeypatch):
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "k")
    monkeypatch.delenv("AZURE_OPENAI_ENDPOINT", raising=False)
    client = llm._init_azure_openai(_cfg("azure_openai", "gpt-5-mini", endpoint="https://res.openai.azure.com"))
    assert str(client.base_url).rstrip("/") == "https://res.openai.azure.com/openai/v1"


def test_azure_endpoint_from_environment(monkeypatch):
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "k")
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://env.openai.azure.com")
    client = llm._init_azure_openai(_cfg("azure_openai", "gpt-5-mini"))
    assert "env.openai.azure.com" in str(client.base_url)


@pytest.mark.parametrize(
    "env,model,needle",
    [
        ({"AZURE_OPENAI_API_KEY": "k"}, "gpt-5-mini", "endpoint"),
        ({"AZURE_OPENAI_ENDPOINT": "https://r.openai.azure.com"}, "gpt-5-mini", "AZURE_OPENAI_API_KEY"),
        ({"AZURE_OPENAI_API_KEY": "k", "AZURE_OPENAI_ENDPOINT": "https://r.openai.azure.com"}, "auto", "deployment"),
    ],
)
def test_azure_says_what_is_missing(monkeypatch, env, model, needle):
    for name in ("AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT"):
        monkeypatch.delenv(name, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    with pytest.raises(ValueError, match=needle):
        llm._init_azure_openai(_cfg("azure_openai", model))


class _FakeChat:
    """Records each request; refuses ``max_tokens`` the way a GPT-5 deployment does."""

    def __init__(self, refuse_max_tokens=False):
        self.calls = []
        self.refuse = refuse_max_tokens
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.calls.append(kwargs)
        if self.refuse and "max_tokens" in kwargs:
            raise RuntimeError("Unsupported parameter: 'max_tokens' is not supported with this model.")
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"total": "132.01"}'))],
            usage=SimpleNamespace(prompt_tokens=10, completion_tokens=5),
            model=kwargs["model"],
        )


def test_reasoning_model_gets_max_completion_tokens_and_no_temperature():
    fake = _FakeChat()
    out = llm._extract_azure_openai(fake, _cfg("azure_openai", "gpt-5-mini"), "prompt")
    sent = fake.calls[0]
    assert "max_completion_tokens" in sent and "max_tokens" not in sent and "temperature" not in sent
    assert out["total"] == "132.01" and out["_lakelogic_llm_provider"] == "azure_openai"


def test_classic_model_keeps_max_tokens_and_temperature():
    fake = _FakeChat()
    llm._extract_openai(fake, _cfg("openai", "gpt-4o-mini"), "prompt")
    assert {"max_tokens", "temperature"} <= set(fake.calls[0])


def test_deployment_name_that_hides_a_reasoning_model_is_retried_once():
    fake = _FakeChat(refuse_max_tokens=True)
    out = llm._extract_azure_openai(fake, _cfg("azure_openai", "invoices-prod"), "prompt")
    assert len(fake.calls) == 2
    assert "max_completion_tokens" in fake.calls[1] and "temperature" not in fake.calls[1]
    assert out["total"] == "132.01"


def test_other_errors_are_not_retried():
    def boom(**_):
        raise RuntimeError("401 Incorrect API key")

    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=boom)))
    with pytest.raises(RuntimeError, match="401"):
        llm._extract_openai(client, _cfg("openai", "gpt-4o-mini"), "prompt")


# ── Google ────────────────────────────────────────────────────────────────────


def test_google_extracts_through_generate_content():
    seen = {}

    def generate_content(**kwargs):
        seen.update(kwargs)
        return SimpleNamespace(
            text='```json\n{"total": "132.01"}\n```',
            usage_metadata=SimpleNamespace(prompt_token_count=7, candidates_token_count=3),
        )

    client = SimpleNamespace(models=SimpleNamespace(generate_content=generate_content))
    out = llm._extract_google(client, _cfg("google", "gemini-2.5-flash"), "prompt")
    assert seen["model"] == "gemini-2.5-flash"
    assert seen["config"]["response_mime_type"] == "application/json"
    assert out["total"] == "132.01" and out["_lakelogic_llm_token_input"] == 7


# ── Bedrock ───────────────────────────────────────────────────────────────────


def test_bedrock_uses_converse_with_the_contract_model():
    seen = {}

    def converse(**kwargs):
        seen.update(kwargs)
        return {
            "output": {"message": {"content": [{"text": '{"total": "132.01"}'}]}},
            "usage": {"inputTokens": 12, "outputTokens": 4},
        }

    out = llm._extract_bedrock(SimpleNamespace(converse=converse), _cfg("bedrock", "eu.amazon.nova-2-lite-v1:0"), "p")
    assert seen["modelId"] == "eu.amazon.nova-2-lite-v1:0"
    assert "JSON" in seen["messages"][0]["content"][0]["text"]
    assert out["total"] == "132.01" and out["_lakelogic_llm_provider"] == "bedrock"
    assert out["_lakelogic_llm_token_output"] == 4


def test_bedrock_region_from_contract_then_environment(monkeypatch):
    pytest.importorskip("boto3")
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    client = llm._init_bedrock(_cfg("bedrock", "anthropic.claude", region="eu-west-2"))
    assert client.meta.region_name == "eu-west-2"
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    assert llm._init_bedrock(_cfg("bedrock", "anthropic.claude")).meta.region_name == "us-east-1"


def test_bedrock_without_a_region_says_so(monkeypatch):
    pytest.importorskip("boto3")
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    with pytest.raises(ValueError, match="region"):
        llm._init_bedrock(_cfg("bedrock", "anthropic.claude"))


# ── Registry ──────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("provider", ["openai", "anthropic", "azure_openai", "google", "bedrock"])
def test_every_cloud_provider_has_a_client_and_an_extractor(provider):
    assert provider in llm._PROVIDER_INIT and provider in llm._PROVIDER_EXTRACT


def test_fenced_and_bare_json_both_parse():
    assert llm._parse_model_json('{"a": 1}') == {"a": 1}
    assert llm._parse_model_json('```json\n{"a": 1}\n```') == {"a": 1}
    assert llm._parse_model_json("not json")["_parse_error"] is True


# ── A documents contract read by a model, end to end (polars) ─────────────────


def test_model_reader_on_pdfs_runs_through_run_source(tmp_path, monkeypatch):
    """run_source reads each PDF to text ONCE and sends the contract's fields to the model."""
    from lakelogic import DataProcessor
    from lakelogic.core.models import DataContract

    for i in (1, 2):
        (tmp_path / f"inv{i}.pdf").write_bytes(b"%PDF-1.4 fake")
    monkeypatch.setitem(llm._PREPROCESSORS, "pdf", lambda path, opts: f"Invoice number: INV-{path[-5]}\nTotal: GBP 1{path[-5]}.50")
    prompts = []

    def fake_extract(client, config, prompt, system_prompt=None):
        prompts.append(prompt)
        n = prompt.split("INV-")[1][0]
        return {"invoice_number": f"INV-{n}", "total": float(f"1{n}.50")}

    monkeypatch.setitem(llm._PROVIDER_EXTRACT, "azure_openai", fake_extract)
    monkeypatch.setitem(llm._PROVIDER_INIT, "azure_openai", lambda cfg: object())
    contract = DataContract(**{
        "version": "1.0.0", "dataset": "bronze_invoices",
        "source": {"type": "landing", "path": str(tmp_path / "*.pdf"), "format": "pdf"},
        "extraction": {"provider": "azure_openai", "model": "gpt-5-mini",
                       "preprocessing": {"content_type": "pdf", "file_column": "file_path",
                                         "text_output_column": "document_text"},
                       "text_column": "document_text"},
        "model": {"fields": [{"name": "file_path", "type": "string"},
                             {"name": "invoice_number", "type": "string", "description": "The invoice id."},
                             {"name": "total", "type": "decimal"}]},
    })
    result = DataProcessor(contract, engine="polars").run_source()
    good = result.good
    assert sorted(good["invoice_number"].to_list()) == ["INV-1", "INV-2"]
    assert "document_text" not in good.columns  # dropped by default
    assert len(prompts) == 2
    assert "- invoice_number (string): The invoice id." in prompts[0] and "Document:" in prompts[0]


def test_fields_prompt_lists_fields_without_file_path():
    cfg = ExtractionConfig(provider="openai", model="m", output_schema=[
        {"name": "file_path", "type": "string"}, {"name": "total", "type": "decimal"}])
    p = llm._fields_prompt(cfg, "TEXT")
    assert "- total (decimal)" in p and "file_path" not in p and p.endswith("TEXT")


def test_prompt_hash_without_a_template():
    assert len(llm._prompt_hash(None)) == 16


# ── Cost: measured, priced by the contract, or null — never a confident 0 ─────


def test_unpriced_model_cost_is_null_not_zero():
    assert llm._cost_usd(_cfg("azure_openai", "gpt-5-mini"), 1000, 1000) is None


def test_contract_prices_win():
    cfg = _cfg("bedrock", "eu.amazon.nova-2-lite-v1:0", input_price_per_million=0.5, output_price_per_million=2)
    assert llm._cost_usd(cfg, 1_000_000, 500_000) == 1.5


def test_longest_table_key_wins():
    assert llm._table_price(llm._INPUT_PRICES, "gpt-4o-mini-2024") == llm._INPUT_PRICES["gpt-4o-mini"]
    assert llm._cost_usd(_cfg("openai", "gpt-4o-mini"), 1_000_000, 0) == llm._INPUT_PRICES["gpt-4o-mini"]


def test_batch_with_unpriced_calls_does_not_crash(monkeypatch):
    monkeypatch.setitem(llm._PROVIDER_INIT, "azure_openai", lambda cfg: object())
    monkeypatch.setattr(llm, "extract_row", lambda row, config, client=None: {**row, "_lakelogic_llm_cost_usd": None})
    contract = SimpleNamespace(extraction=_cfg("azure_openai", "gpt-5-mini", max_cost_per_run=1.0))
    out = llm.extract_batch([{"t": "a"}, {"t": "b"}], contract)
    assert len(out) == 2


# ── The document text: dropped by default, kept on request ────────────────────


def _doc_contract(**extraction_extra):
    from lakelogic.core.models import DataContract

    return DataContract(**{
        "version": "1.0.0", "dataset": "b",
        "source": {"type": "landing", "path": "x/*.pdf", "format": "pdf"},
        "extraction": {"provider": "azure_openai", "model": "m",
                       "preprocessing": {"content_type": "pdf", "file_column": "file_path",
                                         "text_output_column": "document_text"},
                       "text_column": "document_text", **extraction_extra},
        "model": {"fields": [{"name": "file_path", "type": "string"}, {"name": "total", "type": "decimal"}]},
    })


def test_read_text_dropped_by_default():
    from lakelogic.core.extraction import _read_text_column_to_drop

    c = _doc_contract()
    assert _read_text_column_to_drop(c, c.extraction) == "document_text"


def test_read_text_kept_when_asked():
    from lakelogic.core.extraction import _read_text_column_to_drop

    c = _doc_contract(keep_document_text=True)
    assert _read_text_column_to_drop(c, c.extraction) == ""


def test_a_source_text_column_is_never_dropped():
    from lakelogic.core.extraction import _read_text_column_to_drop

    cfg = _cfg("openai", "gpt-4o-mini", text_column="ticket_body")
    assert _read_text_column_to_drop(SimpleNamespace(model=None), cfg) == ""


def test_instruction_hash_follows_the_fields_not_the_document():
    a = ExtractionConfig(provider="openai", model="m", output_schema=[{"name": "total", "type": "decimal"}])
    b = ExtractionConfig(provider="openai", model="m", output_schema=[
        {"name": "total", "type": "decimal"}, {"name": "vat", "type": "decimal"}])
    assert llm._instruction_hash(a) != llm._instruction_hash(b)
    assert llm._instruction_hash(a) != llm._prompt_hash("")
    assert llm._instruction_hash(a) == llm._instruction_hash(a)
