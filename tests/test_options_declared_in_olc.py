"""Every source option Core reads is declared (and described) in OLC's typed options.

OLC 0.21 rejects an option it does not declare, so a key Core reads but OLC does not declare
can never be set: the feature exists in code and nowhere a contract (or Zeus) can reach it.
This scans the readers for option lookups and fails the build on such a key.
"""

import re
from pathlib import Path

from olc.models.source_options import OPTIONS_MODELS

ROOT = Path(__file__).resolve().parents[1] / "lakelogic"
#: The files that read `source.options` (see the inventory in docs/specs).
READERS = [ROOT / "core" / "processor.py"]
#: Dicts in those files that hold source options (`opts`, `options`, `o`, `_o`, `_json_opts`).
#: `opts.get("x")` and `(options or {}).get("x")`.
_LOOKUP = re.compile(
    r"(?:\b(?:opts|options|o|_o|_json_opts)|\((?:opts|options) or \{\}\))\.get\(\s*[\"']([a-z_]+)[\"']"
)
#: `options.get` calls in processor.py that read something other than source.options.
NOT_SOURCE_OPTIONS = {
    "title", "table_name", "target_layer", "domain", "system", "description", "version",
    "updated", "tier", "layer", "last_modified", "mtime", "output", "field", "account_name",
}


def _declared() -> set:
    keys = set()
    for model in OPTIONS_MODELS:
        keys |= set(model.model_fields)
    return keys


def test_every_option_core_reads_is_declared_in_olc():
    read = set()
    for path in READERS:
        read |= set(_LOOKUP.findall(path.read_text(encoding="utf-8")))
    # The scan must actually see the readers (a renamed variable would make it pass vacuously).
    assert {"sheet_name", "record_length", "row_tag", "fetch_size", "cdc_provider", "batch_size"} <= read
    undeclared = sorted(read - _declared() - NOT_SOURCE_OPTIONS)
    assert undeclared == [], (
        f"Core reads source option(s) OLC does not declare: {undeclared}. Declare them (with a "
        "description) in olc/models/source_options.py, or add them to NOT_SOURCE_OPTIONS if the "
        "lookup is not on source.options."
    )


def test_the_csv_whitelist_matches_olc():
    from lakelogic.core.processor import _CSV_OPTION_KEYS
    from olc.models.source_options import CsvOptions

    assert set(_CSV_OPTION_KEYS) <= set(CsvOptions.model_fields)
