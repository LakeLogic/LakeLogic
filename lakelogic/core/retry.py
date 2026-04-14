"""
Generic retry utility with exponential backoff.

Preserves the *original* exception and full traceback on every
attempt — nothing is wrapped or hidden (unlike tenacity's RetryError).

Usage::

    from lakelogic.core.retry import retry_call

    # Retry a function up to 3 times with 30s base wait
    result = retry_call(
        my_function, args=(arg1, arg2), kwargs={"key": "val"},
        attempts=3, base_wait_seconds=30,
    )

    # Or use as a decorator
    @with_retry(attempts=3, base_wait_seconds=10)
    def flaky_network_call():
        ...
"""

from __future__ import annotations

import time
import traceback as _tb
from typing import Any, Callable, Tuple, Type

from loguru import logger


def retry_call(
    func: Callable,
    *,
    args: tuple = (),
    kwargs: dict = None,
    attempts: int = 3,
    base_wait_seconds: int = 30,
    retry_on: Tuple[Type[Exception], ...] = (Exception,),
    label: str = "",
) -> Any:
    """Call *func* with exponential-backoff retry.

    Parameters
    ----------
    func : callable
        The function to call.
    args : tuple
        Positional arguments for *func*.
    kwargs : dict
        Keyword arguments for *func*.
    attempts : int
        Maximum number of attempts (default 3).
    base_wait_seconds : int
        Base wait between retries — doubles each time (30 → 60 → 120…).
    retry_on : tuple of exception types
        Only retry on these exceptions.  Non-matching exceptions
        propagate immediately.
    label : str
        Human-readable label for log messages (e.g. ``"events"``).
        Defaults to ``func.__name__``.

    Returns
    -------
    Any
        The return value of *func* on success.

    Raises
    ------
    Exception
        The original exception from the last failed attempt, with its
        full traceback intact.
    """
    kwargs = kwargs or {}
    label = label or getattr(func, "__name__", str(func))

    # Ensure we always attempt at least once, even if user sets retry_attempts=0
    max_total_attempts = max(1, attempts)

    attempt = 0
    while attempt < max_total_attempts:
        attempt += 1
        try:
            result = func(*args, **kwargs)
            if attempt > 1:
                logger.info(f"✅ {label} succeeded on attempt {attempt}/{max_total_attempts}")
            return result
        except retry_on as e:
            tb_str = _tb.format_exc()
            if attempt < max_total_attempts:
                # Coerce to at least 0 to prevent ValueError on time.sleep if user passes negative
                safe_base_wait = max(0, base_wait_seconds)
                wait = safe_base_wait * (2 ** (attempt - 1))
                logger.warning(
                    f"⚠️ Attempt {attempt}/{max_total_attempts} failed for "
                    f"{label} ({type(e).__name__}): {e}. "
                    f"Retrying in {wait}s...\n{tb_str}"
                )
                time.sleep(wait)
            else:
                logger.error(
                    f"❌ All {max_total_attempts} attempts exhausted for {label} ({type(e).__name__}): {e}\n{tb_str}"
                )
                raise  # Re-raises the original exception — no wrapping


def with_retry(
    *,
    attempts: int = 3,
    base_wait_seconds: int = 30,
    retry_on: Tuple[Type[Exception], ...] = (Exception,),
    label: str = "",
) -> Callable:
    """Decorator form of :func:`retry_call`.

    Usage::

        @with_retry(attempts=3, base_wait_seconds=10)
        def flaky_api_call(url):
            ...
    """

    def decorator(func: Callable) -> Callable:
        import functools

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return retry_call(
                func,
                args=args,
                kwargs=kwargs,
                attempts=attempts,
                base_wait_seconds=base_wait_seconds,
                retry_on=retry_on,
                label=label or func.__name__,
            )

        return wrapper

    return decorator
