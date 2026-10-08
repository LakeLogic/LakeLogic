"""Rewrite a contract's ``source`` to OLC 0.21 typed options (``lakelogic migrate-options``).

OLC 0.21 checks ``source.options`` against a model per format / source kind and rejects what
it does not declare. This moves an older contract onto that shape, keeping what it means:

* format settings on ``source`` (``record_length``, ``encoding``, ``skip_rows``,
  ``skip_footer``, ``strip``) move under ``source.options``;
* old spellings get the declared one: ``sep`` / ``separator`` → ``delimiter``,
  ``multiLine`` → ``multiline``; ``options.pattern`` / ``options.format`` move to ``source``;
* a literal credential is REMOVED and reported, so its owner can put it in an environment
  variable and write ``key: env:VAR`` — it is never carried into the new file;
* any other unknown option moves to ``options.engine_options`` (passed through, unchecked).

The result is checked with OLC's own ``check_source_options``; anything still wrong is reported.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from olc.models.source_options import (
    OPTIONS_ONLY_SOURCE_KEYS,
    check_source_options,
    is_env_reference,
    is_secret_key,
    options_models_for,
)

_RENAMED = {"sep": "delimiter", "separator": "delimiter", "multiLine": "multiline"}
_TO_SOURCE = ("pattern", "format")


def migrate_source(source: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """``(new_source, notes)``. ``source`` is not modified. ``notes`` says what moved, and
    names every credential removed."""
    src = dict(source)
    opts = dict(src.get("options") or {})
    notes: List[str] = []

    for key in sorted(OPTIONS_ONLY_SOURCE_KEYS & set(src)):
        val = src.pop(key)
        if key not in opts:
            opts[key] = val
        notes.append(f"moved source.{key} to source.options.{key}")
    for key in _TO_SOURCE:
        if key in opts:
            val = opts.pop(key)
            if src.get(key) in (None, ""):
                src[key] = val
            notes.append(f"moved source.options.{key} to source.{key}")
    for old, new in _RENAMED.items():
        if old in opts:
            val = opts.pop(old)
            opts.setdefault(new, val)
            notes.append(f"renamed source.options.{old} to {new}")
    for key in [k for k in opts if is_secret_key(k)]:
        if opts[key] is not None and not is_env_reference(opts[key]):
            opts.pop(key)
            notes.append(
                f"REMOVED the credential source.options.{key}: set it in an environment variable "
                f"and write `{key}: env:VAR` (it was not copied)"
            )

    if opts:
        declared = set()
        for model in options_models_for({**src, "options": opts}):
            declared |= set(model.model_fields)
        extra = {k: opts.pop(k) for k in [k for k in opts if k not in declared]}
        if extra:
            engine = dict(opts.get("engine_options") or {})
            engine.update(extra)
            opts["engine_options"] = engine
            notes.append(f"moved {', '.join(sorted(extra))} to source.options.engine_options (not checked)")
    if opts:
        src["options"] = opts
    else:
        src.pop("options", None)
    return src, notes


def migrate_contract(contract: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """Migrate ``contract['source']`` IN PLACE. ``(notes, remaining_problems)``."""
    source = contract.get("source")
    if not isinstance(source, dict):
        return [], []
    new, notes = migrate_source(source)
    if notes:
        # Keep the original mapping object (ruamel keeps comments on it): update in place.
        for key in list(source):
            if key not in new:
                del source[key]
        for key, val in new.items():
            if key == "options" and isinstance(source.get("options"), dict):
                opts = source["options"]
                for k in list(opts):
                    if k not in val:
                        del opts[k]
                for k, v in val.items():
                    opts[k] = v
            else:
                source[key] = val
    return notes, check_source_options(dict(source))
