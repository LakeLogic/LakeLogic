from __future__ import annotations

import yaml

from lakelogic.core.yaml_utils import ContractSafeLoader, load_yaml


def test_load_yaml_preserves_on_join_key() -> None:
    # `on:` is the idiomatic join key. PyYAML's default SafeLoader (YAML 1.1)
    # coerces bare `on` to the boolean True, silently dropping the key. The
    # contract loader must keep it a string.
    data = load_yaml(
        """
        join:
          reference: silver_orders
          on: order_id
          type: left
        """
    )
    join = data["join"]
    assert "on" in join
    assert join["on"] == "order_id"
    assert True not in join  # not coerced to a boolean key


def test_load_yaml_keeps_true_false_as_bool_but_yes_no_on_off_as_strings() -> None:
    data = load_yaml(
        """
        a: true
        b: false
        c: yes
        d: no
        e: on
        f: off
        """
    )
    assert data["a"] is True
    assert data["b"] is False
    assert data["c"] == "yes"
    assert data["d"] == "no"
    assert data["e"] == "on"
    assert data["f"] == "off"


def test_contract_loader_owns_its_resolver_map() -> None:
    # ContractSafeLoader must build its OWN resolver dict rather than mutating
    # yaml.SafeLoader's in place — otherwise importing this module would change
    # yaml.safe_load behavior process-wide. (Asserting on global safe_load output
    # is unreliable because other modules in the codebase do pollute it.)
    assert ContractSafeLoader.yaml_implicit_resolvers is not yaml.SafeLoader.yaml_implicit_resolvers
    # No resolver on ContractSafeLoader may coerce the token "on" to a bool.
    for _ch, mappings in ContractSafeLoader.yaml_implicit_resolvers.items():
        for tag, regex in mappings:
            if tag == "tag:yaml.org,2002:bool":
                assert not regex.match("on")


def test_contract_safeloader_is_a_safeloader_subclass() -> None:
    assert issubclass(ContractSafeLoader, yaml.SafeLoader)
