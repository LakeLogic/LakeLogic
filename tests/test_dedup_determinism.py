"""Guards that `deduplicate` refuses to run without a declared row ordering.

A deduplicate discards rows, so "which duplicate survives" is a business decision.
A contract that does not state it has not been fully written, and any winner the
framework picks on the author's behalf — first row read, lexicographically
smallest, newest file — is that decision made silently and invisibly. So the
framework refuses to run instead of guessing.

The refusal is enforced twice on purpose: `lakelogic lint` catches it before a run
(KEY-001, critical), and the engine raises if an unlinted contract reaches execution.
"""

import polars as pl
import pytest

from lakelogic.core.contract_lint import check_dedup_no_timestamp
from lakelogic.core.models import DataContract
from lakelogic.engines.polars import PolarsAdapter


def _contract(dedup):
    return DataContract(version="1.0.0", transformations=[{"phase": "pre", "deduplicate": dedup}])


def _dedup(rows, dedup=None):
    adapter = PolarsAdapter(_contract(dedup or {"on": ["id"]}))
    return adapter._apply_pre_transformations(pl.DataFrame(rows).lazy()).collect()


_ROWS = [
    {"id": 1, "name": "zoe", "score": 9},
    {"id": 1, "name": "adam", "score": 3},
    {"id": 2, "name": "bo", "score": 1},
]


def test_dedup_without_ordering_is_rejected_at_parse_time():
    """`sort_by` is required by the OLC schema, so the refusal lands at parse."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as exc:
        _contract({"on": ["id"]})
    assert "sort_by" in str(exc.value)


def test_engine_still_refuses_an_unordered_dedup_that_reaches_it():
    """Defence in depth for lenient paths that bypass the strict model.

    The schema is the first gate, but contracts also arrive through lenient
    construction (adapters, scaffolds, hand-built config objects). The engine must
    not assume it was validated — a dedup that reaches it without ordering is still
    refused rather than resolved.
    """
    import types

    from lakelogic.engines.polars import PolarsAdapter as _PA

    adapter = _PA(_contract({"on": ["id"], "sort_by": ["name"]}))
    cfg = types.SimpleNamespace(on=["id"], sort_by=None, order="desc")
    with pytest.raises(ValueError) as exc:
        adapter._dedup_order(cfg, ["id", "name", "score"])

    message = str(exc.value)
    # The error has to be actionable on its own: name the offending keys and show
    # the fix. A bare "ordering required" sends the author back to the docs.
    assert "['id']" in message
    assert "timestamp_column" in message
    assert "sort_by" in message and "order: desc" in message


def test_declared_sort_by_still_runs_and_picks_the_latest():
    """Declaring the order is the whole contract — behaviour there is unchanged."""
    rows = [
        {"id": 1, "name": "old", "updated_at": "2026-01-01"},
        {"id": 1, "name": "new", "updated_at": "2026-06-01"},
    ]
    out = _dedup(rows, {"on": ["id"], "sort_by": ["updated_at"], "order": "desc"})
    assert out.to_dicts() == [{"id": 1, "name": "new", "updated_at": "2026-06-01"}]


def test_deprecated_by_latest_shorthand_still_works_and_warns():
    """Deprecated, not removed: contracts already published must keep running.

    The warning is the migration path — silently accepting it forever would leave
    two spellings of the ordering field in the wild, which is the hazard being
    retired.
    """
    rows = [
        {"id": 1, "name": "old", "updated_at": "2026-01-01"},
        {"id": 1, "name": "new", "updated_at": "2026-06-01"},
    ]
    adapter = PolarsAdapter(
        DataContract(
            version="1.0.0",
            transformations=[
                {
                    "phase": "pre",
                    "deduplicate_by_latest": {
                        "key_columns": ["id"],
                        "timestamp_column": "updated_at",
                    },
                }
            ],
        )
    )
    # The framework logs through loguru, which does NOT propagate to stdlib
    # logging — pytest's `caplog` silently captures nothing here and the assertion
    # would pass vacuously. Attach a real loguru sink instead.
    from loguru import logger

    warnings: list[str] = []
    sink = logger.add(warnings.append, level="WARNING")
    try:
        out = adapter._apply_pre_transformations(pl.DataFrame(rows).lazy()).collect()
    finally:
        logger.remove(sink)

    assert out.to_dicts() == [{"id": 1, "name": "new", "updated_at": "2026-06-01"}]
    assert any("deprecated" in w.lower() for w in warnings)
    # The warning must carry the exact replacement, not just scold.
    assert any("sort_by: [updated_at]" in w for w in warnings)


# ── lint: the same rule, enforced before a run rather than during one ──────────


def _lint(dedup):
    raw = {"transformations": [{"deduplicate": dedup}]}
    return check_dedup_no_timestamp(raw, "contract.yaml", None)


def test_lint_flags_unordered_dedup_as_critical_not_a_warning():
    findings = _lint({"by": ["id"]})
    assert [f.severity for f in findings] == ["critical"]
    assert findings[0].check_id == "KEY-001"


def test_lint_accepts_sort_by_on_the_plain_deduplicate_op():
    assert _lint({"by": ["id"], "sort_by": ["updated_at"], "order": "desc"}) == []


def test_lint_accepts_timestamp_column_on_the_by_latest_shorthand():
    raw = {"transformations": [{"deduplicate_by_latest": {"key_columns": ["id"], "timestamp_column": "updated_at"}}]}
    assert check_dedup_no_timestamp(raw, "contract.yaml", None) == []


def test_lint_and_engine_agree_on_timestamp_column_under_plain_deduplicate():
    """The divergence that made this rule dangerous, pinned shut.

    `timestamp_column` is a field of `deduplicate_by_latest`. On the plain
    `deduplicate` op the contract model drops it at parse time, so the engine still
    has no ordering and refuses. Lint must refuse it too — if lint passed it, an
    author would follow lint's own advice and still get a failed run.
    """
    dedup = {"by": ["id"], "timestamp_column": "updated_at"}

    # The model really does drop it — this is the premise the check rests on. With
    # `sort_by` now required, declaring ONLY timestamp_column fails outright.
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        _contract({"on": ["id"], "timestamp_column": "updated_at"})

    lint_rejects = bool(_lint(dedup))
    engine_rejects = True  # proven by the parse-time and _dedup_order tests above

    assert lint_rejects == engine_rejects
    # And the advice must not send the author in a circle.
    assert "sort_by" in _lint(dedup)[0].suggestion


# ── KEY-002: a sort_by that orders nothing ────────────────────────────────────


def _lint2(dedup):
    from lakelogic.core.contract_lint import check_dedup_sort_by_is_the_key

    return check_dedup_sort_by_is_the_key({"transformations": [{"deduplicate": dedup}]}, "contract.yaml", None)


def test_sort_by_inside_the_dedup_key_is_rejected():
    """Satisfying the schema is not the same as declaring a survivor.

    Every row in a dedup group shares the key's value, so ordering by the key
    orders nothing and the winner is arbitrary again — KEY-001's hole, reopened by
    a `sort_by` that technically validates. This pattern arises whenever a sort_by
    is back-filled mechanically from the keys.
    """
    findings = _lint2({"on": ["trip_id"], "sort_by": ["trip_id"]})
    assert [f.check_id for f in findings] == ["KEY-002"]
    assert findings[0].severity == "critical"


def test_sort_by_inside_the_key_is_caught_regardless_of_case():
    """Cross-platform estates differ in identifier casing (Snowflake uppercases)."""
    assert [f.check_id for f in _lint2({"on": ["trip_id"], "sort_by": ["TRIP_ID"]})] == ["KEY-002"]


def test_partial_overlap_with_the_key_is_still_rejected():
    """`sort_by: [a]` on key `[a, b]` still cannot separate rows within a group."""
    assert [f.check_id for f in _lint2({"on": ["a", "b"], "sort_by": ["a"]})] == ["KEY-002"]


def test_a_real_ordering_column_passes():
    assert _lint2({"on": ["trip_id"], "sort_by": ["updated_at"]}) == []
    # Key + tie-break is fine: the non-key column does the separating.
    assert _lint2({"on": ["a"], "sort_by": ["a", "updated_at"]}) == []


# ── TRF-001: an operation that no engine implements ───────────────────────────


def _lint_ops(transformations):
    from lakelogic.core.contract_lint import check_unknown_transformation_op

    return check_unknown_transformation_op({"transformations": transformations}, "contract.yaml", None)


def test_unimplemented_transformation_op_is_rejected():
    """`Transformation` is extra="allow", so an unknown op is stored and skipped.

    Found in three gold marts declaring `aggregate:` — no engine implements it, so
    the marts emitted un-aggregated rows while every run reported success. The
    contract described a step that never happened.
    """
    findings = _lint_ops([{"aggregate": {"group_by": ["day"]}}])
    assert [f.check_id for f in findings] == ["TRF-001"]
    assert findings[0].severity == "critical"
    assert "aggregate" in findings[0].message


def test_a_misspelled_op_is_caught_too():
    """The same silence hides typos — `dedupliacte` would just never run."""
    assert [f.check_id for f in _lint_ops([{"dedupliacte": {"on": ["id"]}}])] == ["TRF-001"]


def test_supported_ops_and_phase_are_accepted():
    assert _lint_ops([{"sql": "select 1", "phase": "pre"}]) == []
    assert _lint_ops([{"join": {"reference": "d", "key": "id"}}]) == []
    assert _lint_ops([{"deduplicate": {"on": ["id"], "sort_by": ["ts"]}}]) == []


def test_every_op_the_runtime_declares_passes_the_lint():
    """The known-op list must be derived from the model, never hand-maintained —
    a hardcoded list silently rots the moment a new op ships."""
    from lakelogic.core.models import Transformation

    for op in Transformation.model_fields:
        assert _lint_ops([{op: {}}]) == [], f"supported op {op} was flagged"
