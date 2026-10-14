"""Read contract YAML the one way every part of LakeLogic agrees on.

YAML 1.1 (PyYAML's default) reads the bare words ``on``, ``off``, ``yes`` and ``no`` as booleans.
In a contract ``on`` is a FIELD NAME (``deduplicate: {on: [id]}``), so ``yaml.safe_load`` turned
it into the key ``True``: the runtime coped (its loader kept ``on`` as a string), but the linter
and ``lakelogic validate`` read files with ``yaml.safe_load``, so ``KEY-002`` never fired on a real
contract and validation saw a key called ``True`` (found 2026-10-14).

Only ``true``/``false`` are booleans here, as in YAML 1.2. Every contract reader uses this
function. The loader is a SUBCLASS with its own resolver table: editing ``SafeLoader``'s table in
place changes what ``yaml.safe_load`` means for the whole process.
"""

from __future__ import annotations

import re
from typing import IO, Any, Union

import yaml


class _ContractLoader(yaml.SafeLoader):
    pass


_ContractLoader.yaml_implicit_resolvers = {
    key: [(tag, regex) for tag, regex in mappings if tag != "tag:yaml.org,2002:bool"]
    for key, mappings in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
_ContractLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool",
    re.compile(r"^(?:true|false)$", re.IGNORECASE),
    list("tTfF"),
)


def load_contract_yaml(source: Union[str, bytes, IO[str], IO[bytes]]) -> Any:
    """Parse contract YAML text (or an open file) with ``on``/``off``/``yes``/``no`` as strings."""
    return yaml.load(source, Loader=_ContractLoader)  # noqa: S506 - a SafeLoader subclass
