"""The engine reads exactly OLC's source.type set — no more, no less.

The set is defined once, in olc.models._nested.SOURCE_TYPES. The engine does not
keep its own list of kinds; it keeps only how it reads each one. These tests pin
that every OLC kind has a reader, every reader is an OLC kind, and an unknown kind
fails on load naming the allowed ones.
"""

import pytest
from olc.models._nested import SOURCE_TYPES
from pydantic import ValidationError

from lakelogic.core import processor
from lakelogic.core.models import DataContract, SourceConfig
from lakelogic.core.schema_api import contract_schema, validate_contract


def test_dispatch_covers_every_olc_source_type_and_nothing_else():
    dedicated = set(processor.SOURCE_READERS)
    path_read = set(processor.PATH_READ_SOURCE_TYPES)
    assert dedicated.isdisjoint(path_read)
    assert dedicated | path_read == set(SOURCE_TYPES)


def test_every_dedicated_reader_is_a_processor_method():
    for kind, method in processor.SOURCE_READERS.items():
        assert callable(getattr(processor.DataProcessor, method, None)), (kind, method)


def test_run_source_hands_each_dedicated_kind_to_its_reader(monkeypatch):
    for kind, method in processor.SOURCE_READERS.items():
        dp = processor.DataProcessor.__new__(processor.DataProcessor)
        dp.contract = DataContract.model_validate(
            {"version": "1.0.0", "info": {"title": "t"}, "model": {"fields": []}, "source": {"type": kind}}
        )
        called = []
        monkeypatch.setattr(processor.DataProcessor, method, lambda self, k=kind: called.append(k) or "ok")
        try:
            dp.run_source()
        except Exception:
            pass
        assert called == [kind], kind


def test_the_engine_model_uses_the_olc_type():
    assert SourceConfig.model_fields["type"].annotation is SourceConfig.__mro__[1].model_fields["type"].annotation


@pytest.mark.parametrize("bad", ["api", "sql", "file", "parquet", "local", "event"])
def test_an_unknown_source_type_fails_on_load_naming_the_allowed_ones(bad):
    with pytest.raises(ValidationError) as exc:
        DataContract.model_validate(
            {"version": "1.0.0", "info": {"title": "t"}, "model": {"fields": []}, "source": {"type": bad}}
        )
    for kind in SOURCE_TYPES:
        assert f"'{kind}'" in str(exc.value)


def test_schema_api_rejects_an_unknown_source_type_and_lists_the_allowed():
    result = validate_contract({"version": "1.0.0", "info": {"title": "t"}, "source": {"type": "api"}})
    text = " ".join(e.message for e in result.errors)
    assert "Unknown source.type 'api'" in text
    assert "dlt" in text and "sftp" in text


def test_the_engine_schema_publishes_the_olc_enum():
    prop = contract_schema()["$defs"]["SourceConfig"]["properties"]["type"]
    assert prop["enum"] == list(SOURCE_TYPES)
