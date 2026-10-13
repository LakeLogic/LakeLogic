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


def _tail_comment(mapping: Any, key: str) -> Any:
    """The comment ruamel hangs after ``key``'s value (the lines up to the next key), if any."""
    items = getattr(getattr(mapping, "ca", None), "items", None)
    tokens = items.get(key) if isinstance(items, dict) else None
    return tokens[2] if tokens and len(tokens) > 2 and tokens[2] is not None else None


def _last_scalar_slot(mapping: Any) -> Tuple[Any, Any]:
    """``(mapping, key)`` of the last line the mapping writes: its last key, followed into a
    nested mapping. A comment hung there is written after the whole block."""
    while isinstance(mapping, dict) and mapping:
        key = list(mapping)[-1]
        if isinstance(mapping[key], dict) and mapping[key] and hasattr(mapping[key], "ca"):
            mapping = mapping[key]
            continue
        return mapping, key
    return None, None


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
                # ruamel hangs the comment lines that FOLLOW a key on that key. A key that moves
                # (into engine_options) or is renamed took them with it: a two-line note between
                # `options:` and `load_mode:` vanished from a real contract (2026-10-13). Collect
                # them from every key that goes, and hang them where the block now ends.
                orphaned = [c for c in (_tail_comment(opts, k) for k in list(opts) if k not in val) if c]
                for k in list(opts):
                    if k not in val:
                        del opts[k]
                for k, v in val.items():
                    if isinstance(v, dict) and not hasattr(v, "ca") and hasattr(opts, "ca"):
                        v = type(opts)(v)  # a commented map, so the block's end can carry a comment
                    opts[k] = v
                if orphaned:
                    holder, last = _last_scalar_slot(opts)
                    if holder is not None and hasattr(holder, "ca"):
                        kept = _tail_comment(holder, last)
                        first = orphaned[0]
                        tail = "".join(c.value for c in orphaned[1:]) + (kept.value if kept else "")
                        first.value = first.value + tail
                        holder.ca.items[last] = [None, None, first, None]
            else:
                source[key] = val
    return notes, check_source_options(dict(source))
