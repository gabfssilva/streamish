"""Filter operations."""

import time
from collections import deque
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Hashable,
    Iterable,
    Iterator,
)
from typing import overload

from streamish._util import is_async_iterable

__all__ = ["take", "skip", "take_while", "skip_while", "distinct", "distinct_by"]


@overload
def take[T](n: int, it: Iterable[T]) -> Iterator[T]: ...


@overload
def take[T](n: int, it: AsyncIterable[T]) -> AsyncIterator[T]: ...


def take[T](
    n: int, it: Iterable[T] | AsyncIterable[T]
) -> Iterator[T] | AsyncIterator[T]:
    """Take first n elements."""
    if is_async_iterable(it):
        return _take_async(n, it)  # type: ignore[arg-type]
    return _take_sync(n, it)  # type: ignore[arg-type, return-value]


def _take_sync[T](n: int, it: Iterable[T]) -> Iterator[T]:
    count = 0
    for item in it:
        if count >= n:
            break
        yield item
        count += 1


async def _take_async[T](n: int, it: AsyncIterable[T]) -> AsyncIterator[T]:
    count = 0
    async for item in it:
        if count >= n:
            break
        yield item
        count += 1


@overload
def skip[T](n: int, it: Iterable[T]) -> Iterator[T]: ...


@overload
def skip[T](n: int, it: AsyncIterable[T]) -> AsyncIterator[T]: ...


def skip[T](
    n: int, it: Iterable[T] | AsyncIterable[T]
) -> Iterator[T] | AsyncIterator[T]:
    """Skip first n elements."""
    if is_async_iterable(it):
        return _skip_async(n, it)  # type: ignore[arg-type]
    return _skip_sync(n, it)  # type: ignore[arg-type, return-value]


def _skip_sync[T](n: int, it: Iterable[T]) -> Iterator[T]:
    count = 0
    for item in it:
        if count < n:
            count += 1
            continue
        yield item


async def _skip_async[T](n: int, it: AsyncIterable[T]) -> AsyncIterator[T]:
    count = 0
    async for item in it:
        if count < n:
            count += 1
            continue
        yield item


@overload
def take_while[T](pred: Callable[[T], bool], it: Iterable[T]) -> Iterator[T]: ...


@overload
def take_while[T](
    pred: Callable[[T], bool], it: AsyncIterable[T]
) -> AsyncIterator[T]: ...


def take_while[T](
    pred: Callable[[T], bool], it: Iterable[T] | AsyncIterable[T]
) -> Iterator[T] | AsyncIterator[T]:
    """Take elements while predicate is true."""
    if is_async_iterable(it):
        return _take_while_async(pred, it)  # type: ignore[arg-type]
    return _take_while_sync(pred, it)  # type: ignore[arg-type, return-value]


def _take_while_sync[T](pred: Callable[[T], bool], it: Iterable[T]) -> Iterator[T]:
    for item in it:
        if not pred(item):
            break
        yield item


async def _take_while_async[T](
    pred: Callable[[T], bool], it: AsyncIterable[T]
) -> AsyncIterator[T]:
    async for item in it:
        if not pred(item):
            break
        yield item


@overload
def skip_while[T](pred: Callable[[T], bool], it: Iterable[T]) -> Iterator[T]: ...


@overload
def skip_while[T](
    pred: Callable[[T], bool], it: AsyncIterable[T]
) -> AsyncIterator[T]: ...


def skip_while[T](
    pred: Callable[[T], bool], it: Iterable[T] | AsyncIterable[T]
) -> Iterator[T] | AsyncIterator[T]:
    """Skip elements while predicate is true."""
    if is_async_iterable(it):
        return _skip_while_async(pred, it)  # type: ignore[arg-type]
    return _skip_while_sync(pred, it)  # type: ignore[arg-type, return-value]


def _skip_while_sync[T](pred: Callable[[T], bool], it: Iterable[T]) -> Iterator[T]:
    skipping = True
    for item in it:
        if skipping and pred(item):
            continue
        skipping = False
        yield item


async def _skip_while_async[T](
    pred: Callable[[T], bool], it: AsyncIterable[T]
) -> AsyncIterator[T]:
    skipping = True
    async for item in it:
        if skipping and pred(item):
            continue
        skipping = False
        yield item


@overload
def distinct[T: Hashable](
    it: Iterable[T],
    *,
    window: int | None = None,
    timeout: float | None = None,
) -> Iterator[T]: ...


@overload
def distinct[T: Hashable](
    it: AsyncIterable[T],
    *,
    window: int | None = None,
    timeout: float | None = None,
) -> AsyncIterator[T]: ...


def distinct[T: Hashable](
    it: Iterable[T] | AsyncIterable[T],
    *,
    window: int | None = None,
    timeout: float | None = None,
) -> Iterator[T] | AsyncIterator[T]:
    """Remove duplicates.

    Args:
        it: Input iterable
        window: Only consider last N elements as "seen" (LRU-style)
        timeout: Elements expire from "seen" after N seconds

    Without parameters, keeps all seen elements in memory (original behavior).
    With window and/or timeout, elements can reappear after leaving the window.
    """
    if is_async_iterable(it):
        return _distinct_async(it, window, timeout)  # type: ignore[arg-type]
    return _distinct_sync(it, window, timeout)  # type: ignore[arg-type, return-value]


def _distinct_sync[T: Hashable](
    it: Iterable[T],
    window: int | None,
    timeout: float | None,
) -> Iterator[T]:
    if window is None and timeout is None:
        # Original behavior: infinite memory
        seen: set[T] = set()
        for item in it:
            if item not in seen:
                seen.add(item)
                yield item
    elif timeout is None:
        # Window only: use deque for LRU
        seen_deque: deque[T] = deque(maxlen=window)
        seen_set: set[T] = set()
        for item in it:
            if item not in seen_set:
                if len(seen_deque) == window:
                    # Remove oldest from set
                    oldest = seen_deque[0]
                    seen_set.discard(oldest)
                seen_deque.append(item)
                seen_set.add(item)
                yield item
    else:
        # Timeout (with optional window): use dict with timestamps
        seen_times: dict[T, float] = {}
        seen_order: deque[T] = deque(maxlen=window) if window else deque()
        for item in it:
            now = time.monotonic()
            # Clean expired entries
            expired = [k for k, t in seen_times.items() if now - t > timeout]
            for k in expired:
                del seen_times[k]
            # Check window limit
            if window and len(seen_order) == window:
                oldest = seen_order[0]
                seen_times.pop(oldest, None)
            # Check if seen
            if item not in seen_times:
                seen_times[item] = now
                if window:
                    seen_order.append(item)
                yield item


async def _distinct_async[T: Hashable](
    it: AsyncIterable[T],
    window: int | None,
    timeout: float | None,
) -> AsyncIterator[T]:
    if window is None and timeout is None:
        # Original behavior: infinite memory
        seen: set[T] = set()
        async for item in it:
            if item not in seen:
                seen.add(item)
                yield item
    elif timeout is None:
        # Window only: use deque for LRU
        seen_deque: deque[T] = deque(maxlen=window)
        seen_set: set[T] = set()
        async for item in it:
            if item not in seen_set:
                if len(seen_deque) == window:
                    # Remove oldest from set
                    oldest = seen_deque[0]
                    seen_set.discard(oldest)
                seen_deque.append(item)
                seen_set.add(item)
                yield item
    else:
        # Timeout (with optional window): use dict with timestamps
        seen_times: dict[T, float] = {}
        seen_order: deque[T] = deque(maxlen=window) if window else deque()
        async for item in it:
            now = time.monotonic()
            # Clean expired entries
            expired = [k for k, t in seen_times.items() if now - t > timeout]
            for k in expired:
                del seen_times[k]
            # Check window limit
            if window and len(seen_order) == window:
                oldest = seen_order[0]
                seen_times.pop(oldest, None)
            # Check if seen
            if item not in seen_times:
                seen_times[item] = now
                if window:
                    seen_order.append(item)
                yield item


@overload
def distinct_by[T, K: Hashable](
    key_fn: Callable[[T], K], it: Iterable[T]
) -> Iterator[T]: ...


@overload
def distinct_by[T, K: Hashable](
    key_fn: Callable[[T], K], it: AsyncIterable[T]
) -> AsyncIterator[T]: ...


def distinct_by[T, K: Hashable](
    key_fn: Callable[[T], K], it: Iterable[T] | AsyncIterable[T]
) -> Iterator[T] | AsyncIterator[T]:
    """Remove duplicates by key function."""
    if is_async_iterable(it):
        return _distinct_by_async(key_fn, it)  # type: ignore[arg-type]
    return _distinct_by_sync(key_fn, it)  # type: ignore[arg-type, return-value]


def _distinct_by_sync[T, K: Hashable](
    key_fn: Callable[[T], K], it: Iterable[T]
) -> Iterator[T]:
    seen: set[K] = set()
    for item in it:
        key = key_fn(item)
        if key not in seen:
            seen.add(key)
            yield item


async def _distinct_by_async[T, K: Hashable](
    key_fn: Callable[[T], K], it: AsyncIterable[T]
) -> AsyncIterator[T]:
    seen: set[K] = set()
    async for item in it:
        key = key_fn(item)
        if key not in seen:
            seen.add(key)
            yield item
