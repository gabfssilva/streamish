"""Internal utility helpers."""

import inspect
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
)
from typing import Any, TypeGuard

__all__ = ["is_async_iterable", "is_awaitable", "ensure_async_iterator"]


def is_async_iterable[T](
    obj: Iterable[T] | AsyncIterable[T],
) -> TypeGuard[AsyncIterable[T]]:
    """Check if object is an async iterable."""
    return hasattr(obj, "__aiter__")


def is_awaitable(fn: Callable[..., Any]) -> bool:
    """Check if function is async (returns awaitable)."""
    return inspect.iscoroutinefunction(fn)


async def ensure_async_iterator[T](
    it: Iterator[T] | AsyncIterator[T],
) -> AsyncGenerator[T]:
    """Convert sync iterator to async iterator if needed."""
    if isinstance(it, AsyncIterator):
        async for item in it:
            yield item
    else:
        for item in it:
            yield item
