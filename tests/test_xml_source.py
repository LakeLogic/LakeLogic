"""XML sources read on Polars (the default engine).

Polars has no ``read_xml``; every XML source raised AttributeError (2026-10-07)."""

from lakelogic.core.processor import _read_xml_records

XML = """<?xml version="1.0"?>
<export xmlns="urn:x">
  <orders>
    <order id="1"><customer>Ada</customer><amount>12.50</amount></order>
    <order id="2"><customer>Bo</customer><amount/><address><city>Leeds</city></address></order>
  </orders>
</export>"""


def test_records_are_the_repeated_elements_with_attributes_and_children(tmp_path):
    p = tmp_path / "orders.xml"
    p.write_text(XML, encoding="utf-8")
    df = _read_xml_records(str(p))
    assert df.height == 2
    assert df["id"].to_list() == ["1", "2"]
    assert df["customer"].to_list() == ["Ada", "Bo"]
    assert df["amount"].to_list() == ["12.50", None]  # all text; empty is null
    assert df["address"][0] is None and "Leeds" in df["address"][1]  # nested kept as XML text
    assert all(str(t) == "String" for t in df.dtypes)


def test_a_contract_with_an_xml_source_runs_end_to_end_on_polars(tmp_path):
    from lakelogic import DataProcessor

    p = tmp_path / "orders.xml"
    p.write_text(XML, encoding="utf-8")
    contract = {
        "version": "1.0.0",
        "dataset": "orders",
        "source": {"type": "landing", "path": str(p)},
        "model": {
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "customer", "type": "string"},
                {"name": "amount", "type": "double"},
            ]
        },
    }
    good, bad = DataProcessor(engine="polars", contract=contract).run_source()
    assert len(good) + len(bad) == 2
    assert "customer" in good.columns
