"""SCD2 must slot a late-arriving change INTO history, not append it to the end.

`_scd2_frames` assumed chronological arrival: it only ever closed the row carrying
`is_current` and appended the incoming version after it. Nothing checked whether the
incoming change date fell inside an already-closed interval, so a record that arrived
late (a replayed feed, a reprocessed historic partition) corrupted the dimension three
ways at once:

    existing:
      d1 equire   2024-01-01 -> 2024-01-04  is_current=False
      d1 serius   2024-01-04 -> 9999-12-31  is_current=True
    incoming (LATE): d1 notified, change_date 2024-01-03

    result BEFORE the fix:
      d1 equire   2024-01-01 -> 2024-01-04  False
      d1 notified 2024-01-03 -> 9999-12-31  True    <- a 3 Jan fact became current
      d1 serius   2024-01-04 -> 2024-01-03  False   <- effective_to BEFORE effective_from

i.e. an inverted interval, `is_current` moving backwards onto an older fact, and two
versions both claiming 3 Jan. Reprocessing a historic partition therefore actively
corrupted history rather than merely failing to restate it.

The required semantics: split the version whose interval contains the change date, and
insert the late version CLOSED, leaving `is_current` on whichever version genuinely has
the latest `effective_from`.
"""

from __future__ import annotations

import pandas as pd
import pytest

from lakelogic.core.materialization import _scd2_frames


def _cfg(**overrides) -> dict:
    cfg = {
        "effective_from_field": "effective_from",
        "effective_to_field": "effective_to",
        "current_flag_field": "is_current",
        "change_date_field": "change_date",
        "track_columns": ["status"],
    }
    cfg.update(overrides)
    return cfg


def _existing_two_versions() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "driver_id": "d1",
                "status": "equire",
                "effective_from": "2024-01-01",
                "effective_to": "2024-01-04",
                "is_current": False,
            },
            {
                "driver_id": "d1",
                "status": "serius",
                "effective_from": "2024-01-04",
                "effective_to": "9999-12-31",
                "is_current": True,
            },
        ]
    )


def _rows(frame: pd.DataFrame) -> list[dict]:
    """Sorted (effective_from, status) view of the SCD2 control columns."""
    view = frame.sort_values(["effective_from", "status"])
    return [
        {
            "status": r["status"],
            "effective_from": str(r["effective_from"]),
            "effective_to": str(r["effective_to"]),
            "is_current": bool(r["is_current"]),
        }
        for _, r in view.iterrows()
    ]


def _assert_no_inverted_intervals(frame: pd.DataFrame) -> None:
    """No version may end before it begins — anywhere in the frame."""
    for _, row in frame.iterrows():
        ef = pd.Timestamp(row["effective_from"])
        et = pd.Timestamp(row["effective_to"])
        assert et >= ef, f"inverted interval effective_to {et} < effective_from {ef}:\n{row}"


def _assert_no_overlaps(frame: pd.DataFrame, key: str = "driver_id") -> None:
    """No two versions of the same key may both be valid at the same instant."""
    for key_value, group in frame.groupby(key):
        ordered = group.sort_values("effective_from")
        spans = [(pd.Timestamp(r["effective_from"]), pd.Timestamp(r["effective_to"])) for _, r in ordered.iterrows()]
        for (a_from, a_to), (b_from, b_to) in zip(spans, spans[1:]):
            assert a_to <= b_from, (
                f"overlapping versions for {key_value}: [{a_from}, {a_to}) overlaps [{b_from}, {b_to})"
            )


def test_late_arrival_is_slotted_into_history_not_appended():
    """The exact reported scenario: all three rows, and is_current stays on `serius`."""
    frame = _scd2_frames(
        _existing_two_versions(),
        pd.DataFrame([{"driver_id": "d1", "status": "notified", "change_date": "2024-01-03"}]),
        ["driver_id"],
        _cfg(),
    )

    assert _rows(frame) == [
        {"status": "equire", "effective_from": "2024-01-01", "effective_to": "2024-01-03", "is_current": False},
        {"status": "notified", "effective_from": "2024-01-03", "effective_to": "2024-01-04", "is_current": False},
        {"status": "serius", "effective_from": "2024-01-04", "effective_to": "9999-12-31", "is_current": True},
    ], f"late row was not slotted into history:\n{frame}"

    current = frame[frame["is_current"].astype(bool)]
    assert list(current["status"]) == ["serius"], (
        "is_current moved off the genuinely-latest version onto a late-arriving older fact"
    )


def test_late_arrival_produces_no_inverted_interval():
    frame = _scd2_frames(
        _existing_two_versions(),
        pd.DataFrame([{"driver_id": "d1", "status": "notified", "change_date": "2024-01-03"}]),
        ["driver_id"],
        _cfg(),
    )
    _assert_no_inverted_intervals(frame)


def test_late_arrival_produces_no_overlapping_versions():
    frame = _scd2_frames(
        _existing_two_versions(),
        pd.DataFrame([{"driver_id": "d1", "status": "notified", "change_date": "2024-01-03"}]),
        ["driver_id"],
        _cfg(),
    )
    _assert_no_overlaps(frame)


def test_late_row_matching_the_value_in_effect_cuts_no_version():
    """Change detection must compare against the version in effect AT the change date.

    `equire` was in effect on 3 Jan. A replay of that same value says nothing changed, so
    it must not insert a duplicate version. Comparing against the CURRENT row (`serius`)
    would wrongly report a change.
    """
    frame = _scd2_frames(
        _existing_two_versions(),
        pd.DataFrame([{"driver_id": "d1", "status": "equire", "change_date": "2024-01-03"}]),
        ["driver_id"],
        _cfg(),
    )

    assert len(frame) == 2, f"a replayed no-op cut a version:\n{frame}"
    assert _rows(frame) == [
        {"status": "equire", "effective_from": "2024-01-01", "effective_to": "2024-01-04", "is_current": False},
        {"status": "serius", "effective_from": "2024-01-04", "effective_to": "9999-12-31", "is_current": True},
    ]


def test_change_date_before_all_versions_becomes_the_earliest_version():
    """Earlier than everything → new earliest version, ending where the old earliest began.

    It starts at the origin sentinel, which is what the initial-load path gives a first
    version, and it is CLOSED — is_current stays put.
    """
    frame = _scd2_frames(
        _existing_two_versions(),
        pd.DataFrame([{"driver_id": "d1", "status": "prior", "change_date": "2023-06-01"}]),
        ["driver_id"],
        _cfg(),
    )

    assert _rows(frame) == [
        {"status": "prior", "effective_from": "1900-01-01", "effective_to": "2024-01-01", "is_current": False},
        {"status": "equire", "effective_from": "2024-01-01", "effective_to": "2024-01-04", "is_current": False},
        {"status": "serius", "effective_from": "2024-01-04", "effective_to": "9999-12-31", "is_current": True},
    ], f"a pre-history change was not placed as the earliest version:\n{frame}"
    _assert_no_inverted_intervals(frame)
    _assert_no_overlaps(frame)


def test_late_arrival_in_a_gap_does_not_overlap_the_next_version():
    """A change date landing in a HOLE in history runs only up to the next version."""
    existing = pd.DataFrame(
        [
            {
                "driver_id": "d1",
                "status": "equire",
                "effective_from": "2024-01-01",
                "effective_to": "2024-01-02",
                "is_current": False,
            },
            {
                "driver_id": "d1",
                "status": "serius",
                "effective_from": "2024-01-06",
                "effective_to": "9999-12-31",
                "is_current": True,
            },
        ]
    )
    frame = _scd2_frames(
        existing,
        pd.DataFrame([{"driver_id": "d1", "status": "notified", "change_date": "2024-01-04"}]),
        ["driver_id"],
        _cfg(),
    )

    assert _rows(frame) == [
        {"status": "equire", "effective_from": "2024-01-01", "effective_to": "2024-01-02", "is_current": False},
        {"status": "notified", "effective_from": "2024-01-04", "effective_to": "2024-01-06", "is_current": False},
        {"status": "serius", "effective_from": "2024-01-06", "effective_to": "9999-12-31", "is_current": True},
    ], f"a gap-filling version did not stop at the next version:\n{frame}"
    _assert_no_inverted_intervals(frame)
    _assert_no_overlaps(frame)


def test_split_does_not_change_any_existing_effective_from_or_surrogate_key():
    """`effective_from` is the SK input — a split must not renumber stored keys.

    The surrogate key is sha256(pk | effective_from). Slotting a version in only moves the
    split row's `effective_to`, so every stored SK still resolves for facts already
    holding it.
    """
    existing = _existing_two_versions()
    before = _scd2_frames(
        existing,
        pd.DataFrame([{"driver_id": "d1", "status": "serius", "change_date": "2024-01-04"}]),
        ["driver_id"],
        _cfg(),
    )
    sk_before = dict(zip(before["status"], before["_sk"]))
    ef_before = dict(zip(before["status"], before["effective_from"].astype(str)))

    after = _scd2_frames(
        existing,
        pd.DataFrame([{"driver_id": "d1", "status": "notified", "change_date": "2024-01-03"}]),
        ["driver_id"],
        _cfg(),
    )
    sk_after = dict(zip(after["status"], after["_sk"]))
    ef_after = dict(zip(after["status"], after["effective_from"].astype(str)))

    for status in ("equire", "serius"):
        assert ef_after[status] == ef_before[status], f"{status}: effective_from was mutated by a split"
        assert sk_after[status] == sk_before[status], f"{status}: surrogate key churned after a split"


def test_chronological_arrival_is_unchanged():
    """Regression guard, pinned to the output of the pre-fix implementation.

    Chronological arrival is the overwhelmingly common path. These values were captured
    from the implementation BEFORE late-arrival handling was added, so this test fails if
    that path drifts at all.
    """
    frame = _scd2_frames(
        _existing_two_versions(),
        pd.DataFrame([{"driver_id": "d1", "status": "notified", "change_date": "2024-01-09"}]),
        ["driver_id"],
        _cfg(),
    )

    assert list(frame.columns) == [
        "_sk",
        "effective_from",
        "effective_to",
        "is_current",
        "driver_id",
        "status",
        "change_date",
        "_change_reason",
        "_version",
    ]
    captured = [
        {
            "_sk": "aac8c42b60031404",
            "effective_from": "2024-01-01",
            "effective_to": "2024-01-04",
            "is_current": False,
            "status": "equire",
            "_change_reason": None,
            "_version": 1,
        },
        {
            "_sk": "08f2bc5b65852b05",
            "effective_from": "2024-01-04",
            "effective_to": "2024-01-09",
            "is_current": False,
            "status": "serius",
            "_change_reason": None,
            "_version": 2,
        },
        {
            "_sk": "3015df9a5d748939",
            "effective_from": "2024-01-09",
            "effective_to": "9999-12-31",
            "is_current": True,
            "status": "notified",
            "_change_reason": "status",
            "_version": 3,
        },
    ]
    actual = []
    for _, r in frame.iterrows():
        reason = r["_change_reason"]
        actual.append(
            {
                "_sk": r["_sk"],
                "effective_from": str(r["effective_from"]),
                "effective_to": str(r["effective_to"]),
                "is_current": bool(r["is_current"]),
                "status": r["status"],
                "_change_reason": None if pd.isna(reason) else reason,
                "_version": int(r["_version"]),
            }
        )
    assert actual == captured, f"chronological SCD2 behaviour drifted:\n{frame}"


def test_same_day_change_still_takes_the_chronological_path():
    """A change date EQUAL to the live version's start is not late — behaviour is unchanged."""
    frame = _scd2_frames(
        _existing_two_versions(),
        pd.DataFrame([{"driver_id": "d1", "status": "notified", "change_date": "2024-01-04"}]),
        ["driver_id"],
        _cfg(),
    )

    current = frame[frame["is_current"].astype(bool)]
    assert list(current["status"]) == ["notified"]
    _assert_no_inverted_intervals(frame)


@pytest.mark.parametrize("change_date", ["2023-06-01", "2024-01-02", "2024-01-03", "2024-01-09"])
def test_intervals_stay_sane_for_any_arrival_order(change_date):
    """Whatever the arrival order, the frame never inverts, never overlaps, has one current."""
    frame = _scd2_frames(
        _existing_two_versions(),
        pd.DataFrame([{"driver_id": "d1", "status": "notified", "change_date": change_date}]),
        ["driver_id"],
        _cfg(),
    )

    _assert_no_inverted_intervals(frame)
    _assert_no_overlaps(frame)
    assert int(frame["is_current"].astype(bool).sum()) == 1


class TestSparkLateArrival:
    """The native Spark path had the identical defect and gets the identical fix.

    Verified against a real local SparkSession, not by inspection. Before the fix this
    produced `serius 2024-01-04 -> 2024-01-03 (is_current=False)` and made the late
    `notified` row current — byte-for-byte the same three corruptions as pandas.
    """

    @staticmethod
    def _run(spark, tmp_path, existing_rows, incoming_rows, monkeypatch):
        """Run _spark_scd2_dataframe and capture the frame it would write.

        The writer is intercepted only because overwriting the very parquet directory
        being lazily read blows up on the local filesystem; the SCD2 logic under test
        runs for real.
        """
        import pyspark.sql.readwriter as rw
        from pyspark.sql import functions as F

        from lakelogic.core import materialization as mat

        captured = {}
        original_save = rw.DataFrameWriter.save

        def _capture(self, path=None, format=None, mode=None, partitionBy=None, **options):
            captured["pdf"] = self._df.toPandas()

        target = str(tmp_path / "dim").replace("\\", "/")
        existing = spark.createDataFrame(
            existing_rows, "driver_id string, status string, ef string, et string, is_current boolean"
        )
        existing = (
            existing.withColumn("effective_from", F.to_timestamp("ef"))
            .withColumn("effective_to", F.to_timestamp("et"))
            .drop("ef", "et")
        )
        original_save(existing.write.format("parquet").mode("overwrite"), target)

        incoming = spark.createDataFrame(incoming_rows, "driver_id string, status string, cd string")
        incoming = incoming.withColumn("change_date", F.to_timestamp("cd")).drop("cd")

        cfg = {
            "effective_from_field": "effective_from",
            "effective_to_field": "effective_to",
            "current_flag_field": "is_current",
            "change_date_field": "change_date",
            # 9999-12-31 is unrepresentable as a local datetime on Windows when
            # collected back to pandas; the sentinel value is irrelevant here.
            "effective_to_default": "2099-12-31",
            "track_columns": ["status"],
        }
        monkeypatch.setattr(rw.DataFrameWriter, "save", _capture)
        mat._spark_scd2_dataframe(spark, incoming, target, ["driver_id"], cfg, "parquet")
        return captured["pdf"]

    @pytest.mark.skipif(
        __import__("os").getenv("CI") is not None or __import__("os").getenv("SKIP_SPARK_TESTS") is not None,
        reason="Spark tests disabled in CI; set RUN_SPARK_TESTS=1 to enable locally",
    )
    def test_spark_late_arrival_is_slotted_into_history(self, tmp_path, monkeypatch):
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("scd2-late")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        try:
            pdf = self._run(
                spark,
                tmp_path,
                [
                    ("d1", "equire", "2024-01-01", "2024-01-04", False),
                    ("d1", "serius", "2024-01-04", "2099-12-31", True),
                ],
                [("d1", "notified", "2024-01-03")],
                monkeypatch,
            )
        finally:
            spark.stop()

        rows = [
            (
                r["status"],
                str(pd.Timestamp(r["effective_from"]).date()),
                str(pd.Timestamp(r["effective_to"]).date()),
                bool(r["is_current"]),
            )
            for _, r in pdf.sort_values("effective_from").iterrows()
        ]
        assert rows == [
            ("equire", "2024-01-01", "2024-01-03", False),
            ("notified", "2024-01-03", "2024-01-04", False),
            ("serius", "2024-01-04", "2099-12-31", True),
        ], f"Spark did not slot the late row into history:\n{pdf}"

        for _, r in pdf.iterrows():
            assert pd.Timestamp(r["effective_to"]) >= pd.Timestamp(r["effective_from"]), (
                f"Spark wrote an inverted interval:\n{r}"
            )
        assert int(pdf["is_current"].astype(bool).sum()) == 1
        assert list(pdf[pdf["is_current"].astype(bool)]["status"]) == ["serius"]
