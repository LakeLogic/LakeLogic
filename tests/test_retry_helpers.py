from __future__ import annotations

import pytest

from lakelogic.core import retry as retry_mod


def test_retry_call_succeeds_after_retries(monkeypatch):
    sleeps = []
    infos = []
    warnings = []
    monkeypatch.setattr(retry_mod.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(retry_mod.logger, "info", infos.append)
    monkeypatch.setattr(retry_mod.logger, "warning", warnings.append)

    attempts = {"count": 0}

    def flaky(value):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ValueError("transient")
        return value * 2

    result = retry_mod.retry_call(flaky, args=(5,), attempts=3, base_wait_seconds=2, label="orders")

    assert result == 10
    assert attempts["count"] == 3
    assert sleeps == [2, 4]
    assert any("Attempt 1/3 failed for orders" in message for message in warnings)
    assert any("orders succeeded on attempt 3/3" in message for message in infos)


def test_retry_call_non_matching_exception_and_exhaustion(monkeypatch):
    sleeps = []
    errors = []
    monkeypatch.setattr(retry_mod.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(retry_mod.logger, "error", errors.append)

    with pytest.raises(TypeError, match="fatal"):
        retry_mod.retry_call(lambda: (_ for _ in ()).throw(TypeError("fatal")), attempts=3, retry_on=(ValueError,))

    attempts = {"count": 0}

    def always_fails():
        attempts["count"] += 1
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        retry_mod.retry_call(always_fails, attempts=2, base_wait_seconds=-5, label="dataset")

    assert attempts["count"] == 2
    assert sleeps == [0]
    assert any("All 2 attempts exhausted for dataset" in message for message in errors)


def test_retry_call_attempts_zero_and_with_retry_decorator(monkeypatch):
    monkeypatch.setattr(retry_mod.time, "sleep", lambda seconds: None)

    calls = {"count": 0}

    def once():
        calls["count"] += 1
        raise ValueError("single")

    with pytest.raises(ValueError, match="single"):
        retry_mod.retry_call(once, attempts=0)

    assert calls["count"] == 1

    decorated_calls = {"count": 0}

    @retry_mod.with_retry(attempts=2, base_wait_seconds=0, retry_on=(ValueError,))
    def decorated(value):
        decorated_calls["count"] += 1
        if decorated_calls["count"] == 1:
            raise ValueError("retry me")
        return value + 1

    assert decorated(3) == 4
    assert decorated.__name__ == "decorated"