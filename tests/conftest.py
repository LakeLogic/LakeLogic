"""One Spark session per test process, and it is Delta-enabled.

A JVM holds ONE SparkContext, and ``SparkSession.builder...getOrCreate()`` returns whatever
session already exists — silently IGNORING the config it was asked for. So the first Spark
test to run decided the session for every test after it:

* a plain ``getOrCreate()`` in an early module (``test_core``, ``test_ddl``, …) left the Delta
  modules a session without the Delta jars, and every ``.save(format="delta")`` failed;
* four tests ``stop()``-ed the shared session on the way out, and everything after them died
  with "connection refused" constructing a context on a JVM that was gone;
* and any context built fresh advertised the machine's hostname — see ``SPARK_LOCAL_IP`` below.

Each module passed alone and the full suite failed 19 (2026-09-21). The fix is ordering, not
code: before the first test in a file that uses ``SparkSession``, make sure the process's
session is the Delta one. A Delta session is a superset of a plain one, so every other Spark
test still gets what it asked for. Files that never touch Spark pay nothing.
"""
from __future__ import annotations

import functools
import inspect
import os
import sys
import tempfile
from pathlib import Path

import pytest

# LOOPBACK, before any JVM starts. The driver advertises itself by HOSTNAME, and on a machine
# running Docker Desktop that resolves to `host.docker.internal` (192.168.0.70), where the
# driver's port is refused — so every context failed to construct with "Failed to connect to
# host.docker.internal". `spark.driver.bindAddress` alone does not help; the advertised address
# is what executors dial. `setdefault`, so a deliberate setting still wins.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


#: Real-Spark tests are OPT-IN (2026-09-21): they need Java, a JVM per process and a
#: minute of start-up, and they failed the release gate on machine setup, not on code.
#: `RUN_SPARK_TESTS=1` runs them. Tests on the fake pyspark (`_install_fake_pyspark`) need no
#: JVM and always run.
RUN_SPARK = os.environ.get("RUN_SPARK_TESTS", "").strip().lower() in ("1", "true", "yes")
_SPARK_FIXTURES = {"spark", "spark_session"}
_REAL_SPARK_MARKERS = ("SparkSession.builder", "getOrCreate(")


def _needs_real_spark(item) -> bool:
    if _SPARK_FIXTURES & set(getattr(item, "fixturenames", ())):
        return True
    fn = getattr(item, "function", None)
    try:
        src = inspect.getsource(fn) if fn else ""
    except (OSError, TypeError):
        return False
    return "_install_fake_pyspark" not in src and any(m in src for m in _REAL_SPARK_MARKERS)


def pytest_collection_modifyitems(config, items):
    if RUN_SPARK:
        return
    skip = pytest.mark.skip(reason="needs a real Spark JVM; set RUN_SPARK_TESTS=1 to run")
    for item in items:
        if _needs_real_spark(item):
            item.add_marker(skip)


@functools.lru_cache(maxsize=None)
def _uses_spark(path: str) -> bool:
    try:
        return "SparkSession" in Path(path).read_text(encoding="utf-8")
    except OSError:
        return False


def _windows_hadoop_home() -> None:
    """PySpark on Windows wants HADOOP_HOME/bin/winutils.exe; an empty stub satisfies it."""
    if sys.platform != "win32" or os.environ.get("HADOOP_HOME"):
        return
    hadoop_home = os.path.join(tempfile.gettempdir(), "hadoop_lakelogic")
    os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)
    winutils = os.path.join(hadoop_home, "bin", "winutils.exe")
    if not os.path.exists(winutils):
        with open(winutils, "wb"):
            pass
    os.environ["HADOOP_HOME"] = hadoop_home


_tried = False


def _ensure_delta_session(tmp_path_factory) -> None:
    global _tried
    if _tried:
        return
    _tried = True
    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
    except ImportError:
        return  # no Spark here; the Spark tests skip themselves
    if SparkSession.getActiveSession() is not None:
        return  # something built one already; not ours to replace
    _windows_hadoop_home()
    builder = (
        SparkSession.builder.appName("lakelogic-tests")
        .master("local[1]")
        .config("spark.sql.warehouse.dir", tmp_path_factory.mktemp("warehouse").as_posix())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
    )
    try:
        configure_spark_with_delta_pip(builder).getOrCreate().sparkContext.setLogLevel("ERROR")
    except Exception:
        return  # no Java / no Delta jars: each Spark test reports its own skip or failure


@pytest.fixture(autouse=True)
def _delta_spark_session_first(request, tmp_path_factory):
    if RUN_SPARK and _uses_spark(str(request.node.path)):
        _ensure_delta_session(tmp_path_factory)
    yield
