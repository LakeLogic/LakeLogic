"""Spark row counts survive one transient failure, and say why when they cannot be measured.

A busy or local cluster can drop a Python worker mid-action. The counts block used to swallow it
silently and the run reported its counts as unknown (seen as a flaky conformance failure).
"""

import types

from lakelogic.core.processor import DataProcessor


class _Frame:
    """A Spark-like frame whose count() fails the first ``fail`` times."""

    def __init__(self, rows, fail=0):
        self.rows, self.fail = rows, fail

    def count(self):
        if self.fail:
            self.fail -= 1
            raise RuntimeError("Python worker failed to connect back")
        return self.rows


def _counts(source, good, bad):
    me = types.SimpleNamespace(engine_name="spark", _attribute_dropped_rows=lambda dropped: {})
    return DataProcessor._compute_counts(me, source, good, bad)


def test_one_transient_failure_is_retried():
    counts = _counts(_Frame(5, fail=1), _Frame(4), _Frame(1))
    assert (counts["source"], counts["good"], counts["quarantined"]) == (5, 4, 1)


def test_a_repeated_failure_reads_as_unknown_not_a_number():
    counts = _counts(_Frame(5, fail=2), _Frame(4), _Frame(1))
    assert counts["source"] is None
