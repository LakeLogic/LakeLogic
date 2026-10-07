# File formats example

One contract, four file formats: XML, Excel, JSON and JSON Lines.

```bash
pip install "lakelogic[polars]"   # includes openpyxl, which reads Excel
python run.py
```

`run.py` creates `data/orders.xlsx`, then runs the same contract over each file in `data/`
and prints how many rows passed and which were quarantined, with the reason. Each file has
one or two deliberately bad rows (missing customer, negative amount, non-numeric amount).

Requires LakeLogic with the XML and Excel reader fixes (2026-10-07). On an older release,
XML fails with `module 'polars' has no attribute 'read_xml'` and Excel asks for `fastexcel`.

## Complex formats: notebook

Open `complex_formats.ipynb` and run all cells. It builds its own sample files (`make_complex.py`) and runs nine sources:

- `orders_nested.json`: objects inside objects plus an `items` array, handled with `flatten_nested`, `explode` and `json_extract`.
- `orders_nested.xml`: the same data as XML with a namespace, attributes and repeated `<item>`. It uses the same contract shape as the JSON.
- `customers_fixed_width.txt`: mainframe-style fixed-width text with header and trailer lines, read with `format: fixed_width` and a column layout (`name`, `start`, `width`) in `options`.
- `orders_multitab.xlsx`: data on the second tab, a title on row 1, the header on row 2, a blank row and a TOTAL footer. Read with `options: {sheet_name: Orders, header_row: 2, skip_footer: 1}`.

- `orders.csv.gz` and `orders_bundle.zip`: compressed files. A zip's files of the contract's format are read and its README is skipped; `_source_file` shows `archive.zip!inner/file.csv`.
- `orders_eu.csv`: a title line, `;` separators, Latin-1 text, decimal commas and a line break inside quotes, read with `options: {skip_rows, delimiter, encoding, decimal_comma}`.
- `orders_events.avro`: Kafka-style typed records with a nested record and an array.
- `orders_legacy.xls`: Excel 97-2003 with the header on row 2 and real date cells. Needs `xlrd` (included in `lakelogic[polars]`).

To run against a local LakeLogic checkout, set `LAKELOGIC_SRC` in the first cell.

## Scenario suite

The end of the notebook runs 44 scenarios from `scenarios.py`. Each is one realistic problem per format (a truncated file, a byte-order mark, EBCDIC, a TOTAL row, a fake `.gz`, a schema change between Avro files, and so on) with its expected result checked automatically. Run them without Jupyter with `python scenarios.py`. Their input files are written to `data/scenarios/`.
