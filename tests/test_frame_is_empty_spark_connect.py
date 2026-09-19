"""`_frame_is_empty` must not be fooled by a frame whose attribute probe lies.

A Spark Connect DataFrame with no columns -- what an incremental read hands back for an
empty slice, printed as `DataFrame[]` -- resolves ANY attribute to an unresolved Column
instead of raising AttributeError. The old check was
``hasattr(df, "is_empty") and df.is_empty()``; on such a frame `hasattr` answers True and
the call raises ``TypeError: 'Column' object is not callable``, from a line that says
nothing about the cause.

That failure took out every bronze contract in a Databricks mesh run. These tests pin the
fix without needing pyspark installed: the behaviour that matters is "an instance whose
__getattr__ answers every name", which a stand-in reproduces exactly.

The requirement is that the probe asks the CLASS. Check order is then not load-bearing --
a class that does not define `is_empty` cannot be asked for it however the checks are
ordered -- so no test here pins the order, only the class-probing.
"""
import pytest

from lakelogic.pipeline.runner import _frame_is_empty


class _FakeColumn:
    """Stands in for pyspark's Column: any attribute is another Column, and it is not callable."""

    def __getattr__(self, name):
        return _FakeColumn()


class _ZeroColumnConnectFrame:
    """A Spark Connect DataFrame with no columns.

    `isEmpty` is a real method on the class. Every OTHER name falls through to
    `__getattr__` and comes back as a Column, which is the trap.
    """

    def __init__(self, empty=True):
        self._empty = empty

    def isEmpty(self):
        return self._empty

    def __getattr__(self, name):
        return _FakeColumn()


class _LyingSizedFrame:
    """A frame whose CLASS offers only `__len__`, but whose instance answers anything.

    This is the shape that distinguishes probing the class from probing the instance.
    `_ZeroColumnConnectFrame` has a real `isEmpty`, so it is answered before any unsafe
    probe happens and cannot tell the two apart. Here there is no `isEmpty` and no
    `is_empty` on the class, so an instance probe finds the Column and calls it.
    """

    def __init__(self, rows):
        self._rows = rows

    def __len__(self):
        return len(self._rows)

    def __getattr__(self, name):
        return _FakeColumn()


class _PolarsLikeFrame:
    def __init__(self, empty):
        self._empty = empty

    def is_empty(self):
        return self._empty


class _ListLikeFrame:
    def __init__(self, rows):
        self._rows = rows

    def __len__(self):
        return len(self._rows)


def test_zero_column_spark_frame_does_not_raise():
    """The regression: probing the instance found a Column and called it."""
    frame = _ZeroColumnConnectFrame(empty=True)
    # Proves the trap is real in this stand-in, so the test is testing something.
    assert hasattr(frame, "is_empty") is True
    assert not callable(frame.is_empty) or isinstance(frame.is_empty, _FakeColumn)
    assert _frame_is_empty(frame) is True


def test_zero_column_spark_frame_with_rows_is_not_empty():
    assert _frame_is_empty(_ZeroColumnConnectFrame(empty=False)) is False


@pytest.mark.parametrize("rows,expected", [([], True), ([1, 2], False)])
def test_probe_asks_the_class_not_the_instance(rows, expected):
    """The actual requirement: never probe attributes on the instance.

    This frame's class offers only `__len__`. An instance probe would find `isEmpty`
    via `__getattr__`, get a Column back and call it -- the original bug. Asking the
    class falls through to `len()` and answers correctly.
    """
    frame = _LyingSizedFrame(rows)
    assert hasattr(frame, "isEmpty") is True, "stand-in does not reproduce the trap"
    assert _frame_is_empty(frame) is expected


@pytest.mark.parametrize("empty", [True, False])
def test_polars_frame_still_uses_is_empty(empty):
    assert _frame_is_empty(_PolarsLikeFrame(empty)) is empty


def test_none_is_empty():
    assert _frame_is_empty(None) is True


@pytest.mark.parametrize("rows,expected", [([], True), ([1], False)])
def test_plain_list(rows, expected):
    assert _frame_is_empty(rows) is expected


@pytest.mark.parametrize("rows,expected", [([], True), ([1, 2], False)])
def test_sized_frame_falls_back_to_len(rows, expected):
    assert _frame_is_empty(_ListLikeFrame(rows)) is expected


def test_unknown_frame_is_not_reported_empty():
    """An object that answers nothing must not be called empty -- that would silently
    drop a batch."""

    class _Opaque:
        pass

    assert _frame_is_empty(_Opaque()) is False
