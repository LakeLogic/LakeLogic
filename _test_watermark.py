"""Test _resolve_watermark_columns logic in isolation (no heavy imports)."""


class FakeLineage:
    def __init__(self, preserve_upstream=None, upstream_prefix="_upstream"):
        self.preserve_upstream = preserve_upstream or []
        self.upstream_prefix = upstream_prefix
        self.enabled = True


class FakeContract:
    def __init__(self, lineage=None):
        self.lineage = lineage


class Tester:
    """Minimal shim that contains _resolve_watermark_columns."""

    def __init__(self, contract):
        self.contract = contract

    def _resolve_watermark_columns(self, wm_field: str) -> tuple:
        _lin = getattr(self.contract, "lineage", None)
        _pres = list(getattr(_lin, "preserve_upstream", []) or []) if _lin else []
        _pfx = getattr(_lin, "upstream_prefix", "_upstream") or "_upstream" if _lin else "_upstream"

        def _to_renamed(col: str) -> str:
            if col.startswith("_lakelogic_"):
                return f"{_pfx}{col}"
            return f"{_pfx}_{col.lstrip('_')}"

        def _to_original(renamed: str) -> str:
            for src in _pres:
                if _to_renamed(src) == renamed:
                    return src
            return renamed

        if wm_field in _pres:
            return wm_field, _to_renamed(wm_field)

        original = _to_original(wm_field)
        if original != wm_field:
            return original, wm_field

        return wm_field, wm_field


# ── Setup ─────────────────────────────────────────────────────────────────────
lineage = FakeLineage(
    preserve_upstream=["_lakelogic_source", "_lakelogic_processed_at", "_lakelogic_run_id"]
)
contract = FakeContract(lineage=lineage)
t = Tester(contract)

# Test 1: watermark_field is the *renamed* target name
src, tgt = t._resolve_watermark_columns("_upstream_lakelogic_processed_at")
print(f"Test 1: src={src!r}  tgt={tgt!r}")
assert src == "_lakelogic_processed_at", f"FAIL: got {src}"
assert tgt == "_upstream_lakelogic_processed_at", f"FAIL: got {tgt}"
print("  PASSED")

# Test 2: watermark_field is the *original* source name
src2, tgt2 = t._resolve_watermark_columns("_lakelogic_processed_at")
print(f"Test 2: src={src2!r}  tgt={tgt2!r}")
assert src2 == "_lakelogic_processed_at", f"FAIL: got {src2}"
assert tgt2 == "_upstream_lakelogic_processed_at", f"FAIL: got {tgt2}"
print("  PASSED")

# Test 3: no preserve_upstream mapping — passthrough
src3, tgt3 = t._resolve_watermark_columns("updated_at")
print(f"Test 3: src={src3!r}  tgt={tgt3!r}")
assert src3 == "updated_at"
assert tgt3 == "updated_at"
print("  PASSED")

# Test 4: non-lakelogic column in preserve_upstream
lineage4 = FakeLineage(preserve_upstream=["event_date"])
t4 = Tester(FakeContract(lineage=lineage4))
src4, tgt4 = t4._resolve_watermark_columns("event_date")
print(f"Test 4: src={src4!r}  tgt={tgt4!r}")
assert src4 == "event_date"
assert tgt4 == "_upstream_event_date"
print("  PASSED")

# Test 5: reverse — renamed non-lakelogic column
src5, tgt5 = t4._resolve_watermark_columns("_upstream_event_date")
print(f"Test 5: src={src5!r}  tgt={tgt5!r}")
assert src5 == "event_date", f"FAIL: got {src5}"
assert tgt5 == "_upstream_event_date"
print("  PASSED")

# Test 6: no lineage at all
t6 = Tester(FakeContract(lineage=None))
src6, tgt6 = t6._resolve_watermark_columns("_lakelogic_processed_at")
print(f"Test 6: src={src6!r}  tgt={tgt6!r}")
assert src6 == "_lakelogic_processed_at"
assert tgt6 == "_lakelogic_processed_at"
print("  PASSED")

print("\nALL TESTS PASSED")
