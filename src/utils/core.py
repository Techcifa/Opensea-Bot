import sys
import asyncio
import functools
import traceback


class SystemCompliance:
    MIN_PYTHON = (3, 10)

    @classmethod
    def assert_version(cls):
        if sys.version_info < cls.MIN_PYTHON:
            print(
                f"[FATAL] Python {cls.MIN_PYTHON[0]}.{cls.MIN_PYTHON[1]}+ required. "
                f"Current: {sys.version}"
            )
            sys.exit(1)


def async_error_handler(retries: int = 3, delay: float = 2.0):
    """Decorator that retries an async function up to `retries` times on exception."""

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    attempt += 1
                    if attempt >= retries:
                        raise
                    await asyncio.sleep(delay)

        return wrapper

    return decorator
