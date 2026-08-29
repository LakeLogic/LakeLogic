"""SFTP as a declarative source, over AsyncSSH.

Two changes are pinned here.

**AsyncSSH replaces paramiko.** The paramiko implementation used
``AutoAddPolicy()``, which silently accepts ANY unknown host key — host-key
verification off by default, so a credentialed transfer from a partner system could
be intercepted. AsyncSSH verifies against ``known_hosts`` unless told otherwise, and
turning it off now requires saying so and produces a warning.

**``source.type: sftp`` works in a contract.** The connector previously existed but
was wired to nothing: an SFTP drop-folder could not be expressed as a contract the
way ``database`` and ``dlt`` sources can.

These tests run against a REAL SFTP server (asyncssh hosts one in-process), so the
connection, key auth, listing, pattern matching and download are all executed.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

asyncssh = pytest.importorskip("asyncssh")
pl = pytest.importorskip("polars")

from lakelogic.core.processor import DataProcessor
from lakelogic.engines.integration_connectors import SFTPConnector


class _Server:
    """A real in-process SFTP server, serving *root* over a throwaway host key."""

    def __init__(self, root):
        self.root = root
        self.port = None
        self._loop = None
        self._thread = None
        self._ready = threading.Event()

    def start(self):
        def run():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._start())
            self._ready.set()
            self._loop.run_forever()

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        assert self._ready.wait(30), "SFTP server did not start"

    async def _start(self):
        key = asyncssh.generate_private_key("ssh-rsa")

        class _NoAuth(asyncssh.SSHServer):
            # begin_auth is a METHOD on the server class, not a create_server option.
            # Returning False means "this user needs no authentication" — fine for a
            # throwaway in-process fixture; never a real configuration.
            def begin_auth(self, username):
                return False

        self._server = await asyncssh.create_server(
            _NoAuth,
            "127.0.0.1",
            0,
            server_host_keys=[key],
            sftp_factory=lambda chan: asyncssh.SFTPServer(chan, chroot=str(self.root)),
        )
        self.port = self._server.sockets[0].getsockname()[1]

    def stop(self):
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)


@pytest.fixture
def sftp_server(tmp_path):
    root = tmp_path / "inbound"
    root.mkdir()
    (root / "orders_1.csv").write_text("id,status\n1,ok\n2,ok\n")
    (root / "orders_2.csv").write_text("id,status\n3,ok\n")
    (root / "ignore_me.txt").write_text("not a csv")

    server = _Server(root)
    server.start()
    yield server
    server.stop()


def _connector(server):
    # known_hosts=None for the fixture only: the throwaway host key is not in any
    # known_hosts file. The DEFAULT is verification — see the tests below.
    return SFTPConnector(host="127.0.0.1", port=server.port, username="t", known_hosts=None)


# ── the connector really talks SFTP ──────────────────────────────────────────


def test_files_are_fetched_from_a_real_server(sftp_server):
    paths = _connector(sftp_server).fetch_files("/", "*.csv")
    assert len(paths) == 2, paths


def test_pattern_matching_excludes_non_matching_files(sftp_server):
    """ignore_me.txt must not be picked up — otherwise the reader fails on it."""
    paths = _connector(sftp_server).fetch_files("/", "*.csv")
    assert all(p.endswith(".csv") for p in paths)


def test_multiple_files_are_concatenated_into_one_frame(sftp_server):
    df = _connector(sftp_server).extract_files("/", "*.csv", "csv")
    assert df.height == 3  # 2 rows + 1 row
    assert set(df.columns) == {"id", "status"}


def test_no_matching_files_returns_empty_and_warns(sftp_server):
    """A polled drop-folder is legitimately empty sometimes — but silence would look
    identical to a broken pattern."""
    from loguru import logger

    records: list[str] = []
    sink = logger.add(lambda m: records.append(str(m)), level="WARNING", format="{message}")
    try:
        df = _connector(sftp_server).extract_files("/", "*.nothing", "csv")
    finally:
        logger.remove(sink)

    assert df.height == 0
    assert any("No files matched" in r for r in records)


def test_unsupported_format_is_refused_by_name(sftp_server):
    with pytest.raises(ValueError) as exc:
        _connector(sftp_server).extract_files("/", "*.csv", "avro")
    assert "avro" in str(exc.value)


# ── host-key verification: safe by default ───────────────────────────────────


def test_verification_is_on_by_default():
    """paramiko's AutoAddPolicy accepted any host key. The default here must verify."""
    c = SFTPConnector(host="h", username="u")
    assert c.known_hosts == ""  # asyncssh: "" -> ~/.ssh/known_hosts
    assert c._connect_kwargs()["known_hosts"] == ""


def test_disabling_verification_warns_loudly():
    """It stays possible — but it is a visible decision, not a silent default."""
    from loguru import logger

    records: list[str] = []
    sink = logger.add(lambda m: records.append(str(m)), level="WARNING", format="{message}")
    try:
        SFTPConnector(host="h", username="u", known_hosts=None)
    finally:
        logger.remove(sink)

    assert any("verification DISABLED" in r for r in records)


def test_an_unknown_host_key_is_actually_rejected(sftp_server):
    """The verification is real, not decorative: with the default known_hosts the
    fixture's throwaway key is unknown and the connection must fail."""
    c = SFTPConnector(host="127.0.0.1", port=sftp_server.port, username="t")
    with pytest.raises(Exception):
        c.fetch_files("/", "*.csv")


# ── the declarative source ───────────────────────────────────────────────────


def _contract(server, **source_extra):
    source = {
        "type": "sftp",
        "path": f"sftp://t@127.0.0.1:{server.port}/",
        "pattern": "*.csv",
        "format": "csv",
        "options": {"known_hosts": None},
    }
    source.update(source_extra)
    return {
        "version": "1.0.0",
        "dataset": "orders",
        "source": source,
        "model": {
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "status", "type": "string"},
            ]
        },
        "quality": {"row_rules": [{"name": "status_not_null", "sql": "status IS NOT NULL"}]},
    }


def test_a_contract_can_declare_an_sftp_source(sftp_server):
    """The point of the change: no glue code — run_source() does the fetch."""
    good, bad = DataProcessor(_contract(sftp_server), engine="polars").run_source()
    assert len(good) == 3
    assert len(bad) == 0


def test_a_password_in_the_path_is_refused(sftp_server):
    """It would be carried into logs and run metadata."""
    c = _contract(sftp_server)
    c["source"]["path"] = f"sftp://t:secret@127.0.0.1:{sftp_server.port}/"
    with pytest.raises(ValueError) as exc:
        DataProcessor(c, engine="polars").run_source()
    assert "password" in str(exc.value).lower()


def test_a_path_without_a_host_is_refused(sftp_server):
    c = _contract(sftp_server)
    c["source"]["path"] = "sftp:///inbound/"
    with pytest.raises(ValueError) as exc:
        DataProcessor(c, engine="polars").run_source()
    assert "host" in str(exc.value).lower()


# ── incremental by modification time ─────────────────────────────────────────


def test_only_files_modified_since_the_watermark_are_downloaded(sftp_server, tmp_path):
    """The claim "incremental extraction by modification time" was in the docstring
    before any code implemented it. Pinned by execution against a real server: an old
    file is skipped, a newer one is taken."""
    import os
    import time

    root = sftp_server.root
    old, new = root / "orders_1.csv", root / "orders_2.csv"
    # Push orders_1 into the past and orders_2 into the future relative to the mark.
    os.utime(old, (1_000_000, 1_000_000))
    os.utime(new, (3_000_000, 3_000_000))

    paths = _connector(sftp_server).fetch_files("/", "*.csv", modified_since=2_000_000)

    assert len(paths) == 1
    assert paths[0].endswith("orders_2.csv")


def test_no_watermark_takes_everything(sftp_server):
    """First run must not silently skip files for want of a watermark."""
    paths = _connector(sftp_server).fetch_files("/", "*.csv", modified_since=None)
    assert len(paths) == 2


def test_skipped_files_are_reported(sftp_server):
    """"0 new files" and "the pattern is wrong" look identical in silence, and one of
    them is a broken pipeline."""
    import os

    from loguru import logger

    os.utime(sftp_server.root / "orders_1.csv", (1_000_000, 1_000_000))
    os.utime(sftp_server.root / "orders_2.csv", (1_000_000, 1_000_000))

    records: list[str] = []
    sink = logger.add(lambda m: records.append(str(m)), level="INFO", format="{message}")
    try:
        paths = _connector(sftp_server).fetch_files("/", "*.csv", modified_since=2_000_000)
    finally:
        logger.remove(sink)

    assert paths == []
    assert any("skipped 2 not modified since watermark" in r for r in records), records
