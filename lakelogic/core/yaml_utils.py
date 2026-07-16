"""Shared YAML loading for LakeLogic contracts.

PyYAML's default ``SafeLoader`` follows YAML 1.1, which coerces the bare tokens
``on/off/yes/no`` (and ``true/false``) to booleans. That silently breaks data
contracts, where ``on`` is the idiomatic join key::

    transformations:
      - join:
          reference: silver_orders
          on: order_id        # <-- parsed as the boolean True, dropping the key!

``ContractSafeLoader`` keeps ``true/false`` as booleans but leaves
``on/off/yes/no`` as plain strings, so join keys (and any column literally named
on/off/yes/no) survive. Use :func:`load_yaml` everywhere contracts, domain
files, and registries are parsed.
"""

from __future__ import annotations

import re
from typing import IO, Any, Union

import yaml


class ContractSafeLoader(yaml.SafeLoader):
    """SafeLoader that does not coerce ``on/off/yes/no`` to booleans."""


# Build a resolver map that is OWNED by this subclass (a fresh dict), so the
# edits below never mutate the resolvers on the shared ``yaml.SafeLoader``.
ContractSafeLoader.yaml_implicit_resolvers = {
    ch: [(tag, regex) for (tag, regex) in mappings if tag != "tag:yaml.org,2002:bool"]
    for ch, mappings in yaml.SafeLoader.yaml_implicit_resolvers.items()
}

# Re-add a bool resolver restricted to true/false only.
ContractSafeLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool",
    re.compile(r"^(?:true|false)$", re.IGNORECASE),
    list("tTfF"),
)


def load_yaml(stream: Union[str, bytes, IO[Any]]) -> Any:
    """``yaml.safe_load`` replacement that preserves ``on/off/yes/no`` strings."""
    return yaml.load(stream, Loader=ContractSafeLoader)
