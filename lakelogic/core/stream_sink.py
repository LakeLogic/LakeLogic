"""Engine-agnostic streaming micro-batch sink + resumable checkpoint store.

Implements the MVP of docs/specs/streaming-contracts.md: consume any connector's
``.stream()`` iterator (or any iterable of event dicts), micro-batch it, run each
batch through the ordinary batch contract engine (``DataProcessor.run``), write
good -> bronze / bad -> quarantine, and **commit a cursor to a durable checkpoint
after the write** so a killed run resumes from the last committed point.

Two lifetimes, one loop (the only difference is when it stops):

* ``mode="available_now"`` — drain everything currently available, commit, and
  **exit** (AvailableNow semantics; ideal for scheduled / serverless compute).
* ``mode="continuous"``    — keep looping, blocking for new records (always-on
  compute; use only when the freshness SLO is genuinely sub-minute).

Delivery is **at-least-once**: the commit happens after the durable write, so a
crash between write and commit reprocesses the in-flight batch on restart. Make
replays idempotent with the contract's ``materialization.strategy: merge`` +
``primary_key`` (or ``overwrite``) — never bare ``append`` on a resumable source.

The checkpoint **externalizes state** (cursor lives in the store, not process
memory), which is exactly what makes ephemeral / serverless compute safe.
"""

from __future__ import annotations

import abc
import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Union

from loguru import logger

# ── Checkpoint store ─────────────────────────────────────────────────────────

DEFAULT_CHECKPOINT_PATH = "logs/lakelogic_stream_checkpoints.sqlite"


@dataclass
class Checkpoint:
    """A committed position for one (source, contract) stream."""

    cursor: Any  # connector-native position: offset / event-ts / running count
    batch_id: int = 0
    committed_at: Optional[str] = None
    source_count: int = 0
    good_count: int = 0
    bad_count: int = 0


class CheckpointStore(abc.ABC):
    """Durable cursor store. Persistence is modelled on run_log.py's backends
    (the default is stdlib SQLite — zero extra dependency)."""

    @abc.abstractmethod
    def load(self, key: str) -> Optional[Checkpoint]:
        """Return the last committed checkpoint for ``key``, or None if never
        committed (first run)."""

    @abc.abstractmethod
    def commit(self, key: str, checkpoint: Checkpoint) -> None:
        """Upsert the checkpoint for ``key``. Called AFTER the durable write."""

    def close(self) -> None:  # pragma: no cover - trivial default
        pass


class SQLiteCheckpointStore(CheckpointStore):
    """Zero-dependency local checkpoint store (stdlib ``sqlite3``).

    A tiny key -> cursor upsert table; commits are small and fast (sub-ms), which
    is what keeps the micro-batch cycle tight enough for near-real-time on a
    single node (see streaming-contracts.md §7)."""

    def __init__(self, path: Union[str, Path] = DEFAULT_CHECKPOINT_PATH):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # ``check_same_thread=False`` so a sink used across a thread boundary
        # (e.g. a serverless handler) doesn't trip sqlite's thread guard.
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stream_checkpoints (
                key           TEXT PRIMARY KEY,
                cursor_json   TEXT NOT NULL,
                batch_id      INTEGER NOT NULL,
                committed_at  TEXT,
                source_count  INTEGER,
                good_count    INTEGER,
                bad_count     INTEGER
            )
            """
        )
        self._conn.commit()

    def load(self, key: str) -> Optional[Checkpoint]:
        row = self._conn.execute(
            "SELECT cursor_json, batch_id, committed_at, source_count, good_count, bad_count "
            "FROM stream_checkpoints WHERE key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        return Checkpoint(
            cursor=json.loads(row[0]),
            batch_id=row[1],
            committed_at=row[2],
            source_count=row[3] or 0,
            good_count=row[4] or 0,
            bad_count=row[5] or 0,
        )

    def commit(self, key: str, checkpoint: Checkpoint) -> None:
        self._conn.execute(
            """
            INSERT INTO stream_checkpoints
                (key, cursor_json, batch_id, committed_at, source_count, good_count, bad_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                cursor_json  = excluded.cursor_json,
                batch_id     = excluded.batch_id,
                committed_at = excluded.committed_at,
                source_count = excluded.source_count,
                good_count   = excluded.good_count,
                bad_count    = excluded.bad_count
            """,
            (
                key,
                json.dumps(checkpoint.cursor),
                checkpoint.batch_id,
                checkpoint.committed_at,
                checkpoint.source_count,
                checkpoint.good_count,
                checkpoint.bad_count,
            ),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


# ── Stream sink ──────────────────────────────────────────────────────────────


@dataclass
class StreamRunSummary:
    """Result of a drain/continuous run."""

    batches: int = 0
    source_count: int = 0
    good_count: int = 0
    bad_count: int = 0
    #: Events replaced by a later event for the same key in the same micro-batch (merge only).
    #: source_count == good_count + bad_count + superseded_count.
    superseded_count: int = 0
    cursor: Any = None
    resumed_from: Any = None
    per_batch: List[Dict[str, Any]] = field(default_factory=list)


class StreamSink:
    """Micro-batch a stream of event dicts through a contract, resumably.

    Parameters
    ----------
    contract:
        A contract path / YAML / dict / ``DataContract``. Used to build a
        ``DataProcessor`` unless ``processor`` is supplied.
    source:
        Either an object with a ``.stream() -> Iterator[dict]`` method (any
        connector in ``engines/streaming_connectors.py``) or a plain iterable of
        event dicts.
    engine:
        Contract engine for validation ("polars" | "duckdb" | "spark" | ...).
        Defaults to "polars" (always accepts a ``pl.DataFrame`` built from dicts).
    checkpoint:
        A ``CheckpointStore``; defaults to ``SQLiteCheckpointStore`` (zero-dep).
    checkpoint_key:
        Stable key for this (source, contract) stream. Defaults to the contract
        dataset/title. Resuming reads the cursor under this key.
    batch_size / window_seconds:
        Flush a micro-batch when it reaches ``batch_size`` records OR
        ``window_seconds`` have elapsed since the batch opened (whichever first).
    cursor_fn:
        ``callable(event) -> cursor`` extracting a connector-native position from
        the *last* event of a batch (e.g. Kafka offset, event timestamp). When
        omitted, the cursor is a running integer record count and resume is a
        best-effort *skip* of already-processed leading records (works for
        replayable / bounded sources).
    target_path:
        Optional bronze target override passed to ``materialize``.
    processor:
        Inject a pre-built ``DataProcessor`` (or any object exposing ``run(df)``
        and ``materialize(good, bad, target_path=...)``) — mainly for reuse/tests.
    """

    def __init__(
        self,
        contract: Any = None,
        source: Union[Iterable[Dict[str, Any]], Any] = (),
        *,
        engine: str = "polars",
        checkpoint: Optional[CheckpointStore] = None,
        checkpoint_key: Optional[str] = None,
        batch_size: int = 1000,
        window_seconds: Optional[float] = None,
        cursor_fn: Optional[Callable[[Dict[str, Any]], Any]] = None,
        target_path: Optional[Union[str, Path]] = None,
        processor: Any = None,
    ):
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self.source = source
        self.engine = engine
        self.batch_size = batch_size
        self.window_seconds = window_seconds
        self.cursor_fn = cursor_fn
        self.target_path = target_path
        self.checkpoint = checkpoint or SQLiteCheckpointStore()

        if processor is not None:
            self.processor = processor
        else:
            # Imported lazily so importing this module never drags in the whole
            # processor stack unless a sink is actually constructed without one.
            from lakelogic.core.processor import DataProcessor

            self.processor = DataProcessor(contract, engine=engine)

        self.checkpoint_key = checkpoint_key or self._default_key()

    # -- helpers ---------------------------------------------------------------

    def _default_key(self) -> str:
        contract = getattr(self.processor, "contract", None)
        for attr in ("dataset", "title"):
            val = getattr(contract, attr, None)
            if val:
                return f"stream::{val}"
        info = getattr(contract, "info", None)
        title = getattr(info, "title", None)
        return f"stream::{title or 'default'}"

    def _iter_source(self) -> Iterator[Dict[str, Any]]:
        src = self.source
        if hasattr(src, "stream") and callable(src.stream):
            return iter(src.stream())
        return iter(src)

    def _merge_keys(self) -> List[str]:
        contract = getattr(self.processor, "contract", None)
        mat = getattr(contract, "materialization", None)
        if str(getattr(mat, "strategy", "") or "").lower() not in ("merge", "upsert"):
            return []
        return list(getattr(contract, "primary_key", None) or [])

    def _latest_per_key(self, events: List[Dict[str, Any]]):
        """For a ``merge`` contract: the LAST event per primary key in this micro-batch.

        Two events for one key in one batch are an update arriving before the first version
        was written. Both used to reach the merge and BOTH were inserted — a merge target with
        duplicate keys (found against a real broker, 2026-10-07). Arrival order is the stream's
        order, so the last event wins; the earlier ones are counted as ``superseded``. Events
        with a missing key, or unreadable ones, are all kept — the contract quarantines them.
        """
        keys = self._merge_keys()
        if not keys:
            return events, 0
        last: Dict[Any, int] = {}
        for i, e in enumerate(events):
            k = tuple(e.get(c) for c in keys)
            if None in k or UNREADABLE_COLUMN in e:
                continue
            last[k] = i
        kept = [
            e
            for i, e in enumerate(events)
            if UNREADABLE_COLUMN in e
            or None in tuple(e.get(c) for c in keys)
            or last.get(tuple(e.get(c) for c in keys)) == i
        ]
        return kept, len(events) - len(kept)

    def _build_frame(self, events: List[Dict[str, Any]]):
        # Build a Polars frame from the batch — accepted by the polars & duckdb
        # adapters. (Spark converts internally.) One import, kept local.
        import polars as pl

        # infer_schema_length=None: a field (or the unreadable-message column) first seen
        # after the 100th event must not be dropped from the batch.
        return pl.from_dicts(events, infer_schema_length=None)

    # -- the loop --------------------------------------------------------------

    def run(
        self,
        mode: str = "available_now",
        *,
        max_batches: Optional[int] = None,
    ) -> StreamRunSummary:
        """Drain (``available_now``) or stream (``continuous``) the source.

        ``max_batches`` bounds the run (mainly for ``continuous`` control / tests).
        Returns a :class:`StreamRunSummary`.
        """
        if mode not in ("available_now", "continuous"):
            raise ValueError("mode must be 'available_now' or 'continuous'")

        # Push-source guard (§6): a pure-push source (raw WebSocket/Webhook) has no
        # snapshotable "end," so "drain to now, then stop" is undefined — it is
        # continuous-only. Fail fast rather than silently never-terminating.
        if mode == "available_now" and getattr(self.source, "continuous_only", False):
            raise ValueError(
                f"source {type(self.source).__name__} is continuous_only (no snapshotable "
                "end) — it cannot run in 'available_now'. Use mode='continuous'."
            )

        prior = self.checkpoint.load(self.checkpoint_key)
        summary = StreamRunSummary(resumed_from=prior.cursor if prior else None)

        # An offset-aware source (e.g. KafkaOffsetSource) drives resume/commit from
        # its native broker cursor instead of the count-skip fallback.
        offset_aware = hasattr(self.source, "current_cursor") and hasattr(self.source, "seek")

        # Align the source's drain boundary with the run mode (AvailableNow stops
        # when caught up; continuous blocks for new records).
        if hasattr(self.source, "drain"):
            self.source.drain = mode == "available_now"

        batch_id = prior.batch_id if prior else 0
        running_count = 0
        skip = 0

        if offset_aware:
            self.source.seek(prior.cursor if prior else None)
            logger.info(
                f"[StreamSink {self.checkpoint_key}] resuming from broker cursor "
                f"{prior.cursor if prior else '(earliest)'}."
            )
        elif prior is not None and self.cursor_fn is None and isinstance(prior.cursor, int):
            # Default (count) cursor → resume by skipping already-processed records.
            skip = prior.cursor
            running_count = prior.cursor
            logger.info(f"[StreamSink {self.checkpoint_key}] resuming — skipping {skip} already-committed record(s).")
        else:
            logger.info(f"[StreamSink {self.checkpoint_key}] starting from the beginning.")

        stream = self._iter_source()

        # Best-effort resume for the default count cursor: drop leading records.
        for _ in range(skip):
            try:
                next(stream)
            except StopIteration:
                break

        buf: List[Dict[str, Any]] = []
        window_open = time.monotonic()

        def _flush() -> bool:
            """Validate + write + commit one micro-batch. Returns False if empty."""
            nonlocal batch_id, running_count
            if not buf:
                return False
            batch_id += 1
            received = len(buf)
            events, superseded = self._latest_per_key(buf)
            frame = self._build_frame(events)
            result = self.processor.run(frame)
            # Durable write FIRST …
            self.processor.materialize(result.good, result.bad, target_path=self.target_path)
            # … THEN advance the cursor (at-least-once boundary).
            running_count += received
            if offset_aware:
                cursor = self.source.current_cursor()
            elif self.cursor_fn is not None:
                cursor = self.cursor_fn(buf[-1])
            else:
                cursor = running_count
            ckpt = Checkpoint(
                cursor=cursor,
                batch_id=batch_id,
                committed_at=_utc_now_iso(),
                source_count=result.source_count,
                good_count=result.good_count,
                bad_count=result.bad_count,
            )
            self.checkpoint.commit(self.checkpoint_key, ckpt)

            summary.batches += 1
            summary.source_count += received
            summary.superseded_count += superseded
            summary.good_count += result.good_count
            summary.bad_count += result.bad_count
            summary.cursor = cursor
            summary.per_batch.append(
                {
                    "batch_id": batch_id,
                    "source": received,
                    "superseded": superseded,
                    "good": result.good_count,
                    "bad": result.bad_count,
                    "cursor": cursor,
                }
            )
            logger.info(
                f"[StreamSink {self.checkpoint_key}] batch {batch_id} committed — "
                f"good={result.good_count} bad={result.bad_count} superseded={superseded} cursor={cursor}"
            )
            buf.clear()
            return True

        while True:
            if max_batches is not None and summary.batches >= max_batches:
                break
            try:
                event = next(stream)
            except StopIteration:
                # Source exhausted → drain the final partial batch and, for
                # available_now, stop. For continuous a truly unbounded source
                # never exhausts; a bounded one ending is treated as "drained".
                _flush()
                break

            buf.append(event)
            window_elapsed = self.window_seconds is not None and (time.monotonic() - window_open) >= self.window_seconds
            if len(buf) >= self.batch_size or window_elapsed:
                _flush()
                window_open = time.monotonic()
                if max_batches is not None and summary.batches >= max_batches:
                    break

        return summary


#: The per-row error column the engines quarantine on (``lakelogic.core.processor``).
UNREADABLE_COLUMN = "__type_err__record"


def _unreadable(reason: str, raw: Any = None, where: str = "") -> Dict[str, Any]:
    """A message that could not be decoded, as a row the contract engine QUARANTINES.

    One bad message used to raise out of the stream and kill the run — and, because the
    checkpoint was never committed, every restart died on the same message: a poison pill
    that stopped the stream for good (found against a real broker, 2026-10-07). Now it is a
    row whose only content is the reason (with the first bytes, for diagnosis), and the
    stream moves on. Every engine quarantines a row carrying this column.
    """
    preview = ""
    if raw is not None:
        text = bytes(raw).decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
        preview = f": {text[:80]!r}"
    return {UNREADABLE_COLUMN: f"{reason}{where}{preview}"}


def _utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ── Offset-aware Kafka source ─────────────────────────────────────────────────
#
# An "offset-aware source" is any object exposing BOTH:
#   * seek(cursor)        -> position the source at a stored cursor (resume)
#   * current_cursor()    -> the source's position after the records yielded so far
# When StreamSink sees these, it drives resume/commit from the *broker* cursor
# instead of the best-effort count-skip. KafkaOffsetSource is the reference
# implementation; SQS/Pub/Sub ack-based sources can implement the same protocol.


def _tp_key(topic: str, partition: int) -> str:
    """JSON-friendly checkpoint key for a topic-partition."""
    return f"{topic}:{partition}"


class KafkaOffsetSource:
    """Kafka source with native, checkpoint-committed offsets for StreamSink.

    Auto-commit is **disabled** so LakeLogic controls the commit: StreamSink calls
    ``current_cursor()`` after each durable write and stores
    ``{"topic:partition": next_offset}``; on restart it calls ``seek(cursor)`` to
    resume exactly where the last batch committed (at-least-once — pair with an
    idempotent sink for effectively-once). See docs/specs/streaming-contracts.md §6.

    ``drain`` controls the AvailableNow boundary: when True (set automatically by
    StreamSink for ``available_now``), ``stream()`` ends once the consumer is
    caught up (an empty poll); when False (``continuous``) it blocks for new data.

    The ``consumer`` (and ``tp_factory``) are injectable so the offset/resume logic
    is testable without a live broker.
    """

    def __init__(
        self,
        topic: str,
        *,
        brokers: Optional[Union[str, List[str]]] = None,
        group_id: Optional[str] = None,
        auto_offset_reset: str = "earliest",
        value_deserializer: Optional[Callable[[bytes], Any]] = None,
        poll_timeout_ms: int = 1000,
        drain_idle_timeout_s: float = 60.0,
        max_poll_records: int = 500,
        drain: bool = False,
        consumer: Any = None,
        tp_factory: Optional[Callable[[str, int], Any]] = None,
        **kafka_config: Any,
    ):
        self.topic = topic
        self.value_deserializer = value_deserializer
        self.poll_timeout_ms = poll_timeout_ms
        self.drain_idle_timeout_s = drain_idle_timeout_s
        self.max_poll_records = max_poll_records
        self.drain = drain
        self._positions: Dict[str, int] = {}  # "topic:partition" -> next_offset
        self._assigned = False

        if consumer is not None:
            self.consumer = consumer
            # Default to a simple tuple-based TP for injected (test/fake) consumers.
            self._tp = tp_factory or _SimpleTP
        else:  # pragma: no cover - requires a live broker / kafka-python
            try:
                from kafka import KafkaConsumer, TopicPartition
            except ImportError as exc:
                raise ImportError(
                    "KafkaOffsetSource needs kafka-python. Install with `pip install 'lakelogic[kafka]'`."
                ) from exc
            self.consumer = KafkaConsumer(
                bootstrap_servers=brokers,
                group_id=group_id,
                enable_auto_commit=False,  # LakeLogic commits after the write
                auto_offset_reset=auto_offset_reset,
                **kafka_config,
            )
            self._tp = tp_factory or TopicPartition

    # -- offset-aware protocol -------------------------------------------------

    def seek(self, cursor: Optional[Dict[str, int]]) -> None:
        """Assign all partitions of the topic and seek to the stored offsets.
        A partition absent from ``cursor`` is left at ``auto_offset_reset``."""
        partitions = self.consumer.partitions_for_topic(self.topic) or set()
        tps = [self._tp(self.topic, p) for p in sorted(partitions)]
        if not tps:
            self._assigned = True
            return
        self.consumer.assign(tps)
        self._tps = tps
        cursor = cursor or {}
        for tp in tps:
            offset = cursor.get(_tp_key(self.topic, tp.partition))
            if offset is not None:
                self.consumer.seek(tp, offset)
                self._positions[_tp_key(self.topic, tp.partition)] = offset
        self._assigned = True

    def current_cursor(self) -> Dict[str, int]:
        return dict(self._positions)

    def _end_offsets(self) -> Optional[Dict[str, int]]:
        """``{"topic:partition": end_offset}`` now, or None when the consumer cannot say."""
        tps = getattr(self, "_tps", None) or []
        fn = getattr(self.consumer, "end_offsets", None)
        if not tps or fn is None:
            return None
        try:
            ends = fn(tps)
        except Exception:  # pragma: no cover - a broker that cannot answer falls back to empty-poll
            return None
        out = {}
        for tp in tps:
            out[_tp_key(self.topic, tp.partition)] = int(ends.get(tp, 0))
            if _tp_key(self.topic, tp.partition) not in self._positions:
                try:
                    self._positions[_tp_key(self.topic, tp.partition)] = int(self.consumer.position(tp))
                except Exception:  # pragma: no cover
                    pass
        return out

    def _caught_up(self, end: Dict[str, int]) -> bool:
        return all(self._positions.get(k, 0) >= v for k, v in end.items())

    def stream(self) -> Iterator[Dict[str, Any]]:
        if not self._assigned:
            self.seek(None)  # direct use (no StreamSink resume): assign from start
        # AvailableNow = "everything that was in the topic when this run started". Snapshot each
        # partition's END offset now and drain until every partition reaches it. An empty poll is
        # NOT "caught up": on Azure Event Hubs the first polls after a reconnect can be empty while
        # messages are waiting, and a restart read 0 of 100 waiting events (found 2026-10-07).
        end = self._end_offsets() if self.drain else None
        last_data = time.monotonic()
        while True:
            if self.drain and end is not None and self._caught_up(end):
                return  # reached the end offsets snapshotted at the start → AvailableNow stop
            batch = self.consumer.poll(timeout_ms=self.poll_timeout_ms, max_records=self.max_poll_records)
            if not batch:
                if self.drain and end is None:
                    return  # no end offsets available (e.g. a test consumer): an empty poll = caught up
                if self.drain and time.monotonic() - last_data > self.drain_idle_timeout_s:
                    logger.warning(
                        f"[KafkaOffsetSource {self.topic}] stopped draining after {self.drain_idle_timeout_s:.0f}s "
                        f"with no data, before reaching the end offsets {end}; the next run continues from here."
                    )
                    return
                continue  # continuous, or not yet at the end offsets: keep polling
            last_data = time.monotonic()
            for _tp_obj, records in batch.items():
                for rec in records:
                    self._positions[_tp_key(rec.topic, rec.partition)] = rec.offset + 1
                    yield self._decode(rec.value, f" (partition {rec.partition}, offset {rec.offset})")

    def _decode(self, value: Any, where: str = "") -> Any:
        try:
            if isinstance(value, (bytes, bytearray)):
                if self.value_deserializer:
                    decoded = self.value_deserializer(value)
                else:
                    decoded = json.loads(bytes(value).decode("utf-8"))
            else:
                decoded = value
        except Exception as exc:  # noqa: BLE001 - any decode failure is one bad message
            return _unreadable(f"Message is not valid JSON ({type(exc).__name__})", value, where)
        if not isinstance(decoded, dict):
            return _unreadable(f"Message is not a JSON object (got {type(decoded).__name__})", value, where)
        return decoded

    def close(self) -> None:  # pragma: no cover - trivial
        try:
            self.consumer.close()
        except Exception:
            pass


class SSEOffsetSource:
    """Server-Sent Events source with native ``Last-Event-ID`` resume for StreamSink.

    A second reference implementation of the offset-aware protocol (see
    KafkaOffsetSource). Where Kafka's cursor is a per-partition offset dict, an
    SSE stream's cursor is the **id of the last event seen** — the exact value the
    SSE spec says a client must replay via the ``Last-Event-ID`` request header on
    reconnect (https://html.spec.whatwg.org/multipage/server-sent-events.html). So:

      * ``current_cursor()`` -> the last event ``id`` yielded (a string), and
      * ``seek(cursor)``      -> arm that id as the ``Last-Event-ID`` for the next
                                connect, so the server resumes after it.

    StreamSink commits the id **after** the durable write, so a crash replays from
    the last committed event id (at-least-once — pair with an idempotent sink).
    Events without an ``id`` don't advance the cursor (per the SSE spec), so resume
    lands on the last *identified* event.

    ``drain`` controls the AvailableNow boundary: when True (set by StreamSink for
    ``available_now``) ``stream()`` ends when the feed reports it is caught up (a
    ``None`` / empty event); when False it blocks for the next event.

    ``connect`` is an injectable ``callable(last_event_id) -> Iterable[event]`` where
    each event is a mapping with at least a ``data`` payload and an optional ``id``
    (mirrors ``sseclient``'s Event). Injectable so the resume logic is testable with
    no live HTTP feed.
    """

    def __init__(
        self,
        url: Optional[str] = None,
        *,
        connect: Optional[Callable[[Optional[str]], Iterable[Any]]] = None,
        id_field: str = "id",
        data_field: str = "data",
        drain: bool = False,
        headers: Optional[Dict[str, str]] = None,
        value_deserializer: Optional[Callable[[str], Any]] = None,
    ):
        self.url = url
        self._connect = connect
        self.id_field = id_field
        self.data_field = data_field
        self.drain = drain
        self.headers = dict(headers or {})
        self.value_deserializer = value_deserializer
        self._last_id: Optional[str] = None

    # -- offset-aware protocol -------------------------------------------------

    def seek(self, cursor: Optional[str]) -> None:
        """Arm ``cursor`` as the ``Last-Event-ID`` for the next connect."""
        self._last_id = cursor

    def current_cursor(self) -> Optional[str]:
        return self._last_id

    def _open(self) -> Iterable[Any]:
        if self._connect is not None:
            return self._connect(self._last_id)
        # Live feed: sseclient replays from Last-Event-ID on our behalf.
        try:  # pragma: no cover - requires sseclient + a live feed
            import requests  # type: ignore
            import sseclient  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "SSEOffsetSource needs sseclient-py + requests. Install with `pip install 'lakelogic[sse]'`."
            ) from exc
        headers = dict(self.headers)  # pragma: no cover
        headers.setdefault("Accept", "text/event-stream")
        if self._last_id is not None:
            headers["Last-Event-ID"] = self._last_id
        resp = requests.get(self.url, stream=True, headers=headers)
        return sseclient.SSEClient(resp).events()

    def stream(self) -> Iterator[Dict[str, Any]]:
        for event in self._open():
            if event is None:  # sentinel: feed is caught up
                if self.drain:
                    return
                continue
            data = self._field(event, self.data_field)
            if data is None:
                continue  # comment / keep-alive line — nothing to process
            event_id = self._field(event, self.id_field)
            if event_id is not None:
                self._last_id = str(event_id)  # only identified events advance
            yield self._decode(data)
        # Feed ended: for a bounded/drain feed that's "caught up"; for continuous
        # a reconnect is the caller's concern (sseclient auto-reconnects live).

    @staticmethod
    def _field(event: Any, name: str) -> Any:
        if isinstance(event, dict):
            return event.get(name)
        return getattr(event, name, None)

    def _decode(self, data: Any) -> Any:
        if self.value_deserializer is not None:
            return self.value_deserializer(data)
        if isinstance(data, (bytes, bytearray)):
            data = bytes(data).decode("utf-8")
        if isinstance(data, str):
            try:
                return json.loads(data)
            except (json.JSONDecodeError, ValueError):
                return {"data": data}
        return data


# ── Spark Structured Streaming path (stateful silver/gold) ─────────────────────
#
# StreamSink is the engine-agnostic single-node loop (bronze ingest). For stateful
# transforms — windowed aggregations, stream-stream joins, watermarked dedup — the
# right tool is Spark Structured Streaming, which owns state + resumability. Rather
# than reimplement any of that, LakeLogic rides *inside* Spark's micro-batch via
# ``writeStream.foreachBatch``: each micro-batch DataFrame runs through the SAME
# contract engine (``DataProcessor.run`` + ``materialize``) as every other layer.
#
# Resumability is **Spark's** here, not our SQLite store: Spark's offset log +
# commit log under ``checkpointLocation`` is the single source of truth (the
# "shared-checkpoint path"). We don't double-book cursors. Delivery is still
# at-least-once — ``batch_id`` is deterministic across restarts, so pair with an
# idempotent contract (merge / overwrite-by-batch) for effectively-once. See
# docs/specs/streaming-contracts.md §5/§7/§8.


@dataclass
class SparkBatchResult:
    """Per micro-batch outcome from the foreachBatch handler."""

    batch_id: int
    source_count: int = 0
    good_count: int = 0
    bad_count: int = 0
    #: Events replaced by a later event for the same key in this micro-batch (merge only);
    #: source_count == good_count + bad_count + superseded_count.
    superseded_count: int = 0


#: Kafka bookkeeping columns ``kafka_json_stream`` adds for ordering; dropped before the contract.
KAFKA_ORDER_COLUMNS = ("__kafka_timestamp", "__kafka_partition", "__kafka_offset")


def kafka_json_stream(
    spark: Any,
    topic: str,
    fields: List[str],
    *,
    brokers: str,
    starting_offsets: str = "earliest",
    **kafka_options: Any,
) -> Any:
    """A Spark Structured Streaming DataFrame of JSON Kafka messages, ready for ``SparkStreamSink``.

    Each message value must be a JSON object. The contract's ``fields`` come out as TEXT (the
    engine's typed cast then quarantines what does not fit, as for every landing source). A message
    that is not a JSON object becomes a row carrying ``UNREADABLE_COLUMN`` — the same reason, with
    partition, offset and the first bytes, that ``StreamSink`` gives — so the contract quarantines
    it. (Spark's own ``from_json`` returns a row of nulls for invalid JSON, silently.) Kafka
    timestamp/partition/offset ride along in ``KAFKA_ORDER_COLUMNS`` for ``SparkStreamSink`` to
    order updates by; they never reach the contract. Needs Spark's Kafka connector
    (``org.apache.spark:spark-sql-kafka-0-10_2.12:<spark version>``).
    """
    from pyspark.sql import functions as F

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
    )
    for k, v in kafka_options.items():
        reader = reader.option(k, v)
    raw = reader.load()
    # decode(), not cast: a plain CAST keeps invalid UTF-8 bytes, and the reason text built from
    # them could not be read back (UnicodeDecodeError). decode() replaces them with U+FFFD.
    text = F.decode(F.col("value"), "UTF-8")
    obj = F.from_json(text, "map<string,string>")  # null for invalid JSON AND for non-object JSON
    where = F.concat(
        F.lit(" (partition "),
        F.col("partition").cast("string"),
        F.lit(", offset "),
        F.col("offset").cast("string"),
        F.lit("): '"),
        F.substring(text, 1, 80),
        F.lit("'"),
    )
    reason = F.when(
        obj.isNull(),
        F.concat(
            F.when(F.get_json_object(text, "$").isNull(), F.lit("Message is not valid JSON")).otherwise(
                F.lit("Message is not a JSON object")
            ),
            where,
        ),
    )
    return raw.select(
        *[F.when(obj.isNotNull(), obj.getItem(f)).alias(f) for f in fields],
        reason.alias(UNREADABLE_COLUMN),
        F.col("timestamp").alias("__kafka_timestamp"),
        F.col("partition").alias("__kafka_partition"),
        F.col("offset").alias("__kafka_offset"),
    )


class SparkStreamSink:
    """Run a LakeLogic contract inside Spark Structured Streaming via foreachBatch.

    Parameters
    ----------
    contract:
        Contract path / YAML / dict / ``DataContract`` (used to build a Spark-engine
        ``DataProcessor`` unless ``processor`` is injected).
    stream_df:
        A streaming Spark ``DataFrame`` (from ``spark.readStream...``). Duck-typed:
        anything exposing ``.writeStream`` works (so the handler is unit-testable
        with a fake).
    checkpoint_location:
        Spark's checkpoint dir — the **single** resumability source of truth for
        this stream. Required (a resumable stream without one silently reprocesses).
    trigger:
        ``"available_now"`` (drain to current end + stop; scheduled/serverless) or
        ``"processing_time"`` (micro-batch every ``processing_time`` interval;
        always-on). Mirrors StreamSink's two lifetimes.
    processing_time:
        Interval string (e.g. ``"30 seconds"``) for the ``processing_time`` trigger.
    output_mode:
        Spark output mode for the stream (``"append"`` default; ``"update"`` /
        ``"complete"`` for aggregations).
    target_path / processor:
        As ``StreamSink``.
    on_batch:
        Optional ``callable(SparkBatchResult)`` — hook for per-window run-log
        emission / metrics (§8/§12.E) without coupling this class to run_log.
    """

    def __init__(
        self,
        contract: Any = None,
        stream_df: Any = None,
        *,
        checkpoint_location: Optional[str] = None,
        engine: str = "spark",
        trigger: str = "available_now",
        processing_time: Optional[str] = None,
        output_mode: str = "append",
        target_path: Optional[Union[str, Path]] = None,
        processor: Any = None,
        on_batch: Optional[Callable[["SparkBatchResult"], None]] = None,
    ):
        if not checkpoint_location:
            raise ValueError(
                "checkpoint_location is required — it is Spark's resumability source of truth for this stream."
            )
        if trigger not in ("available_now", "processing_time"):
            raise ValueError("trigger must be 'available_now' or 'processing_time'")
        if trigger == "processing_time" and not processing_time:
            raise ValueError("processing_time interval required for the 'processing_time' trigger")

        self.stream_df = stream_df
        self.checkpoint_location = checkpoint_location
        self.trigger = trigger
        self.processing_time = processing_time
        self.output_mode = output_mode
        self.target_path = target_path
        self.on_batch = on_batch
        self.batches: List[SparkBatchResult] = []

        if processor is not None:
            self.processor = processor
        else:
            from lakelogic.core.processor import DataProcessor

            self.processor = DataProcessor(contract, engine=engine)

    # -- the foreachBatch handler (the unit-testable core) ---------------------

    def process_micro_batch(self, batch_df: Any, batch_id: int) -> SparkBatchResult:
        """Validate + write ONE Spark micro-batch through the contract engine.

        Spark calls this per micro-batch with a *deterministic* ``batch_id``; on
        restart it replays the last uncommitted batch with the same id, so an
        idempotent write (merge/overwrite-by-batch) yields effectively-once."""
        # Fresh run_id per micro-batch so lineage/run reporting isn't reused.
        if hasattr(self.processor, "last_run_id"):
            self.processor.last_run_id = None
        batch_df, received, superseded = self._latest_per_key(batch_df)
        result = self.processor.run(batch_df)
        self.processor.materialize(result.good, result.bad, target_path=self.target_path)
        summary = SparkBatchResult(
            batch_id=batch_id,
            source_count=received if received is not None else result.source_count,
            good_count=result.good_count,
            bad_count=result.bad_count,
            superseded_count=superseded,
        )
        self.batches.append(summary)
        if self.on_batch is not None:
            self.on_batch(summary)
        logger.info(
            f"[SparkStreamSink] micro-batch {batch_id} — good={summary.good_count} "
            f"bad={summary.bad_count} (checkpoint={self.checkpoint_location})"
        )
        return summary

    def _latest_per_key(self, batch_df: Any):
        """For a ``merge`` contract: the LAST event per primary key in this micro-batch.

        Same rule as ``StreamSink._latest_per_key`` (two events for one key in one batch both
        reached the merge and both were inserted). Order: Kafka timestamp, then partition and
        offset (``KAFKA_ORDER_COLUMNS``, added by ``kafka_json_stream``); those columns are dropped
        before the contract runs. Rows with a missing key or unreadable rows are all kept.
        Returns ``(df, received, superseded)``; counts are None/0 for a non-Spark (test) frame.
        """
        cols = list(getattr(batch_df, "columns", []) or [])
        kafka_cols = [c for c in KAFKA_ORDER_COLUMNS if c in cols]
        if not hasattr(batch_df, "sparkSession"):
            return batch_df, None, 0
        contract = getattr(self.processor, "contract", None)
        mat = getattr(contract, "materialization", None)
        keys = list(getattr(contract, "primary_key", None) or [])
        is_merge = str(getattr(mat, "strategy", "") or "").lower() in ("merge", "upsert")
        if not is_merge or not keys or not set(keys) <= set(cols):
            return batch_df.drop(*kafka_cols), None, 0
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        received = batch_df.count()
        order = [F.col(c).desc() for c in kafka_cols] or [F.monotonically_increasing_id().desc()]
        keyed = F.lit(True)
        for k in keys:
            keyed = keyed & F.col(k).isNotNull()
        if UNREADABLE_COLUMN in cols:
            keyed = keyed & F.col(UNREADABLE_COLUMN).isNull()
        w = Window.partitionBy(*keys).orderBy(*order)
        ranked = batch_df.withColumn("__lakelogic_rank", F.when(keyed, F.row_number().over(w)).otherwise(F.lit(1)))
        latest = ranked.filter(F.col("__lakelogic_rank") == 1).drop("__lakelogic_rank", *kafka_cols)
        kept = latest.count()
        return latest, received, received - kept

    def _apply_trigger(self, writer: Any) -> Any:
        if self.trigger == "available_now":
            return writer.trigger(availableNow=True)
        return writer.trigger(processingTime=self.processing_time)

    def run(self, await_termination: Optional[bool] = None) -> Any:
        """Start the Structured Streaming query. Returns the ``StreamingQuery``.

        For ``available_now`` the default is to block until the drain completes
        (``awaitTermination``); for ``processing_time`` the default returns the live
        query. Pass ``await_termination`` to override."""
        writer = (
            self.stream_df.writeStream.foreachBatch(self.process_micro_batch)
            .option("checkpointLocation", self.checkpoint_location)
            .outputMode(self.output_mode)
        )
        writer = self._apply_trigger(writer)
        query = writer.start()
        should_wait = await_termination if await_termination is not None else self.trigger == "available_now"
        if should_wait:
            query.awaitTermination()
        return query


class WatermarkChunkSource:
    """Offset-aware **keyset-paged** source for large DB/file loads (§15.3).

    Delivers the intent of "make bounded ``fetch_size`` loads resumable + memory-
    bounded" *without* touching the batch processor: it pages a source by a
    monotonic watermark/key column in bounded chunks and plugs into StreamSink as a
    third offset-aware source (cursor = the last watermark value seen). Because each
    chunk flows through the normal micro-batch loop, memory stays flat (one chunk at
    a time) and a crash resumes from the last committed watermark — the exact
    opposite of ``fetch_size``'s accumulate-then-write-once, all-or-nothing load.

    ``fetch_chunk`` is an injectable ``callable(after, limit) -> list[dict]`` that
    returns up to ``limit`` rows with watermark strictly greater than ``after``
    (``after is None`` on the first page), ordered by the watermark ascending. This
    keeps the source driver-agnostic: back it with a keyset SQL query
    (``WHERE wm > :after ORDER BY wm LIMIT :n``), a Delta version scan, or a sorted
    file read.

    ``drain`` (set by StreamSink for ``available_now``): stop when a short page
    signals the source is caught up; when False (``continuous``) keep polling for
    rows past the current watermark.
    """

    def __init__(
        self,
        fetch_chunk: Callable[[Optional[Any], int], List[Dict[str, Any]]],
        *,
        watermark_field: str,
        chunk_size: int = 1000,
        drain: bool = False,
    ):
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        self._fetch_chunk = fetch_chunk
        self.watermark_field = watermark_field
        self.chunk_size = chunk_size
        self.drain = drain
        self._watermark: Optional[Any] = None

    # -- offset-aware protocol -------------------------------------------------

    def seek(self, cursor: Optional[Any]) -> None:
        """Resume paging *after* the committed watermark value."""
        self._watermark = cursor

    def current_cursor(self) -> Optional[Any]:
        return self._watermark

    def stream(self) -> Iterator[Dict[str, Any]]:
        while True:
            page = self._fetch_chunk(self._watermark, self.chunk_size)
            if not page:
                if self.drain:
                    return  # caught up → AvailableNow stop
                continue  # continuous: wait for rows past the watermark
            for row in page:
                if self.watermark_field in row:
                    self._watermark = row[self.watermark_field]
                yield row
            if len(page) < self.chunk_size and self.drain:
                return  # short final page → drained


class _SimpleTP:
    """Minimal topic-partition for injected consumers (tests). Hashable."""

    __slots__ = ("topic", "partition")

    def __init__(self, topic: str, partition: int):
        self.topic = topic
        self.partition = partition

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, _SimpleTP) and self.topic == other.topic and self.partition == other.partition

    def __hash__(self) -> int:
        return hash((self.topic, self.partition))

    def __repr__(self) -> str:  # pragma: no cover
        return f"_SimpleTP({self.topic!r}, {self.partition})"
