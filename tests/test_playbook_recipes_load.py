"""Every Core playbook recipe (OLC `recipes/`) loads as a LakeLogic Core contract.

OLC validates a recipe's shape; this proves Core accepts it too, so a recipe an agent copies can
run. Recipes come from the installed `olc` bundle, or from a sibling open-lakehouse-contract
checkout while they are new (OLC_RECIPES_DIR overrides both).
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest
import yaml

PLACEHOLDER = re.compile(r"\{([a-z][a-z0-9_]*)\}")


def _recipe_dir() -> Path | None:
    candidates = [os.environ.get("OLC_RECIPES_DIR", "")]
    try:
        import olc

        candidates.append(str(Path(olc.__file__).parent / "_bundled" / "recipes"))
    except ImportError:  # pragma: no cover
        pass
    candidates.append(str(Path(__file__).resolve().parents[2] / "open-lakehouse-contract" / "recipes"))
    return next((Path(c) for c in candidates if c and Path(c).is_dir()), None)


RECIPES = sorted(_recipe_dir().rglob("*.yaml")) if _recipe_dir() else []


def _filled(obj, values):
    if isinstance(obj, dict):
        return {_filled(k, values): _filled(v, values) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_filled(v, values) for v in obj]
    if isinstance(obj, str):
        return PLACEHOLDER.sub(lambda m: values.get(m.group(1), m.group(0)), obj)
    return obj


@pytest.mark.skipif(not RECIPES, reason="no OLC recipes available")
@pytest.mark.parametrize("path", RECIPES, ids=lambda p: p.stem)
def test_recipe_loads_in_core(path):
    from lakelogic.core.models import DataContract

    recipe = yaml.safe_load(path.read_text(encoding="utf-8"))
    values = {p["name"]: str(p["examples"][0]) for p in recipe["parameters"]}
    contract = DataContract.model_validate(_filled(recipe["contract"], values))
    assert contract.source is not None
