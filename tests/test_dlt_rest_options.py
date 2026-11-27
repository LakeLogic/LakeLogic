"""The REST API mode end to end, against a local HTTP server (no network).

Covers the endpoint options an HR API needs (2026-11-17): a paginator WITH settings (Frappe's
limit_start / limit_page_length), where the records sit (``data``), an API key sent in a named
header, and incremental loading - a second run fetches only rows changed since the first.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

pytest.importorskip("dlt")

from lakelogic.adapters.dlt_adapter import DltAdapter  # noqa: E402
from lakelogic.core.models import DltEndpointConfig, DltSourceConfig, SourceConfig  # noqa: E402

ROWS = [
    {"name": f"HR-EMP-{i:03d}", "employee_name": f"Person {i}", "modified": f"2026-11-{i:02d} 10:00:00"}
    for i in range(1, 8)
]
SEEN = {"auth": [], "filters": []}


class _Frappe(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        SEEN["auth"].append(self.headers.get("Authorization"))
        q = parse_qs(urlparse(self.path).query)
        start = int(q.get("limit_start", ["0"])[0])
        size = int(q.get("limit_page_length", ["20"])[0])
        rows = ROWS
        flt = q.get("filters", [None])[0]
        SEEN["filters"].append(flt)
        if flt:
            after = json.loads(flt)[0][2]
            rows = [r for r in rows if r["modified"] > after]
        body = json.dumps({"data": rows[start : start + size]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


@pytest.fixture()
def server():
    srv = HTTPServer(("127.0.0.1", 0), _Frappe)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    SEEN["auth"].clear()
    SEEN["filters"].clear()
    yield f"http://127.0.0.1:{srv.server_port}/api/resource/"
    srv.shutdown()


def _source(base_url):
    return SourceConfig(
        type="dlt",
        dlt=DltSourceConfig(
            base_url=base_url,
            credentials={"api_key": "${FRAPPE_TEST_TOKEN}"},
            auth={"type": "api_key", "name": "Authorization", "location": "header"},
            endpoints=[
                DltEndpointConfig(
                    name="employee",
                    path="Employee",
                    params={
                        "fields": '["name","employee_name","modified"]',
                        "filters": '[["modified",">","{incremental.start_value}"]]',
                    },
                    paginator_config={
                        "type": "offset",
                        "limit": 3,
                        "offset_param": "limit_start",
                        "limit_param": "limit_page_length",
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                    data_selector="data",
                    incremental={"cursor_path": "modified", "initial_value": "2000-01-01 00:00:00"},
                    primary_key="name",
                )
            ],
        ),
    )


def test_pages_header_key_records_path_and_incremental(server, monkeypatch, tmp_path):
    monkeypatch.setenv("FRAPPE_TEST_TOKEN", "token abc:def")
    monkeypatch.chdir(tmp_path)
    first = DltAdapter(_source(server), "frappe_employee")
    table = first.extract()
    assert table.num_rows == len(ROWS)  # 3 pages of 3, 3, 1
    assert set(SEEN["auth"]) == {"token abc:def"}  # the key in the named header
    assert json.loads(SEEN["filters"][0])[0][2] == "2000-01-01 00:00:00"

    # A new employee arrives; the second run asks only for rows after the last cursor.
    ROWS.append({"name": "HR-EMP-099", "employee_name": "New Starter", "modified": "2026-11-20 09:00:00"})
    try:
        SEEN["filters"].clear()
        second = DltAdapter(_source(server), "frappe_employee")
        table2 = second.extract(previous_state=first.dlt_state_json)
        assert json.loads(SEEN["filters"][0])[0][2] == "2026-11-07 10:00:00"
        assert table2.column("name").to_pylist() == ["HR-EMP-099"]
    finally:
        ROWS.pop()


def test_nested_data_stays_one_json_column_and_child_tables_are_not_stacked(monkeypatch, tmp_path):
    rows = [
        {"id": 1, "employeeName": "A", "address": {"city": "Leeds"}, "skills": ["sql", "python"]},
        {"id": 2, "employeeName": "B", "address": {"city": "York"}, "skills": []},
    ]

    class _Api(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = json.dumps({"data": rows}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), _Api)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    monkeypatch.chdir(tmp_path)
    try:
        src = SourceConfig(
            type="dlt",
            dlt=DltSourceConfig(
                base_url=f"http://127.0.0.1:{srv.server_port}/",
                max_table_nesting=0,
                endpoints=[
                    DltEndpointConfig(
                        name="people", path="people", data_selector="data", paginator_config={"type": "single_page"}
                    )
                ],
            ),
        )
        table = DltAdapter(src, "people_api").extract()
    finally:
        srv.shutdown()
    assert table.num_rows == 2  # no child rows stacked on
    cols = set(table.column_names)
    assert {"id", "employee_name", "address", "skills"} <= cols  # dlt's snake_case names
    assert not any("__" in c for c in cols)  # nothing flattened


def test_a_contract_title_with_spaces_reads_only_this_runs_rows(server, monkeypatch, tmp_path):
    """dlt normalises the dataset name; Core must not re-read the last run's files (2026-11-17:
    `Bronze — Frappe Employees` returned 26 rows for 1 change)."""
    monkeypatch.setenv("FRAPPE_TEST_TOKEN", "token abc:def")
    monkeypatch.chdir(tmp_path)
    title = "Bronze — Frappe Employee"
    first = DltAdapter(_source(server), title)
    assert first.extract().num_rows == len(ROWS)
    ROWS.append({"name": "HR-EMP-098", "employee_name": "Late Joiner", "modified": "2026-11-21 09:00:00"})
    try:
        second = DltAdapter(_source(server), title)
        table = second.extract(previous_state=first.dlt_state_json)
        assert table.column("name").to_pylist() == ["HR-EMP-098"]
    finally:
        ROWS.pop()


def test_parameters_fill_the_url_and_the_first_run_start(server, monkeypatch, tmp_path):
    """`${NAME}` is a pipeline parameter: the tenant URL and the first run's start per run."""
    monkeypatch.setenv("FRAPPE_TEST_TOKEN", "token abc:def")
    monkeypatch.setenv("HR_BASE", server)
    monkeypatch.setenv("HR_START", "2026-11-05 00:00:00")
    monkeypatch.chdir(tmp_path)
    src = _source("${HR_BASE}")
    inc = src.dlt.endpoints[0].incremental
    # Typed on OLC >= 0.29 (DltIncrementalConfig), a dict before.
    if isinstance(inc, dict):
        inc["initial_value"] = "${HR_START}"
    else:
        inc.initial_value = "${HR_START}"
    table = DltAdapter(src, "params").extract()
    assert json.loads(SEEN["filters"][0])[0][2] == "2026-11-05 00:00:00"
    assert sorted(table.column("name").to_pylist()) == ["HR-EMP-005", "HR-EMP-006", "HR-EMP-007"]


def test_a_missing_parameter_is_named(monkeypatch, tmp_path):
    monkeypatch.delenv("HR_BASE", raising=False)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="HR_BASE"):
        DltAdapter(_source("${HR_BASE}"), "params").extract()


def test_start_from_reloads_from_a_date_instead_of_the_saved_position(server, monkeypatch, tmp_path):
    monkeypatch.setenv("FRAPPE_TEST_TOKEN", "token abc:def")
    monkeypatch.chdir(tmp_path)
    first = DltAdapter(_source(server), "backfill")
    first.extract()
    SEEN["filters"].clear()
    again = DltAdapter(_source(server), "backfill")
    table = again.extract(previous_state=first.dlt_state_json, start_from="2026-11-03 00:00:00")
    assert json.loads(SEEN["filters"][0])[0][2] == "2026-11-03 00:00:00"
    assert table.num_rows == 5  # 03..07, not 0


def test_a_temporary_job_disk_resumes_from_the_run_log(server, monkeypatch, tmp_path):
    """Databricks/Fabric jobs start on an empty disk: the run-log state is the only memory."""
    import shutil

    monkeypatch.setenv("FRAPPE_TEST_TOKEN", "token abc:def")
    monkeypatch.chdir(tmp_path)
    first = DltAdapter(_source(server), "frappe_employee")
    first.extract()
    shutil.rmtree(tmp_path / ".lakelogic")  # the next job's disk is empty
    SEEN["filters"].clear()
    second = DltAdapter(_source(server), "frappe_employee")
    table = second.extract(previous_state=first.dlt_state_json)
    assert json.loads(SEEN["filters"][0])[0][2] == "2026-11-07 10:00:00"
    assert table.num_rows == 0
