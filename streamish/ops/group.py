"""Group operations."""

import asyncio
from collections import deque
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable, Iterator
from typing import overload

from streamish._util import ensure_async_iterator, is_async_iterable

__all__ = ["batch", "window", "partition", "partition_async"]


@overload
def batch[T](
    size: int, it: Iterable[T], *, timeout: float | None = None
) -> Iterator[list[T]]: ...


@overload
def batch[T](
    size: int, it: AsyncIterable[T], *, timeout: float | None = None
) -> AsyncIterator[list[T]]: ...


def batch[T](
    size: int,
    it: Iterable[T] | AsyncIterable[T],
    *,
    timeout: float | None = None,
) -> Iterator[list[T]] | AsyncIterator[list[T]]:
    """Group elements into batches by size or timeout."""
    if size <= 0:
        raise ValueError("size must be positive")
    if is_async_iterable(it) or timeout is not None:
        return _batch_async(size, it, timeout)  # type: ignore[arg-type]
    return _batch_sync(size, it)  # type: ignore[arg-type, return-value]


def _batch_sync[T](size: int, it: Iterable[T]) -> Iterator[list[T]]:
    current: list[T] = []
    for item in it:
        current.append(item)
        if len(current) >= size:
            yield current
            current = []
    if current:
        yield current


async def _batch_async[T](
    size: int,
    it: Iterable[T] | AsyncIterable[T],
    timeout: float | None,
) -> AsyncIterator[list[T]]:
    current: list[T] = []

    ait: AsyncIterator[T]
    if is_async_iterable(it):
        ait = it.__aiter__()  # type: ignore[union-attr]
    else:
        sync_it: Iterable[T] = it  # type: ignore[assignment]
        ait = ensure_async_iterator(iter(sync_it))

    if timeout is None:
        async for item in ait:
            current.append(item)
            if len(current) >= size:
                yield current
                current = []
        if current:
            yield current
    else:
        # Use a task-based approach to avoid cancelling the iterator
        async def get_next() -> T:
            return await ait.__anext__()

        pending_task: asyncio.Task[T] | None = None
        while True:
            try:
                if pending_task is None:
                    pending_task = asyncio.create_task(get_next())
                item = await asyncio.wait_for(
                    asyncio.shield(pending_task), timeout=timeout
                )
                pending_task = None  # Task completed, clear it
                current.append(item)
                if len(current) >= size:
                    yield current
                    current = []
            except TimeoutError:
                if current:
                    yield current
                    current = []
                # pending_task is still running, will be awaited next iteration
            except StopAsyncIteration:
                if current:
                    yield current
                break


@overload
def window[T](size: int, it: Iterable[T], *, step: int = 1) -> Iterator[list[T]]: ...


@overload
def window[T](
    size: int, it: AsyncIterable[T], *, step: int = 1
) -> AsyncIterator[list[T]]: ...


def window[T](
    size: int, it: Iterable[T] | AsyncIterable[T], *, step: int = 1
) -> Iterator[list[T]] | AsyncIterator[list[T]]:
    """Sliding window over elements."""
    if size <= 0:
        raise ValueError("size must be positive")
    if step <= 0:
        raise ValueError("step must be positive")
    if is_async_iterable(it):
        return _window_async(size, it, step)  # type: ignore[arg-type]
    return _window_sync(size, it, step)  # type: ignore[arg-type, return-value]


def _window_sync[T](size: int, it: Iterable[T], step: int) -> Iterator[list[T]]:
    buf: deque[T] = deque(maxlen=size)
    skip = 0
    for item in it:
        if skip > 0:
            skip -= 1
            buf.append(item)
            continue
        buf.append(item)
        if len(buf) == size:
            yield list(buf)
            skip = step - 1
            for _ in range(min(step, size)):
                if buf:
                    buf.popleft()


async def _window_async[T](
    size: int, it: AsyncIterable[T], step: int
) -> AsyncIterator[list[T]]:
    buf: deque[T] = deque(maxlen=size)
    skip = 0
    async for item in it:
        if skip > 0:
            skip -= 1
            buf.append(item)
            continue
        buf.append(item)
        if len(buf) == size:
            yield list(buf)
            skip = step - 1
            for _ in range(min(step, size)):
                if buf:
                    buf.popleft()


def partition[T](pred: Callable[[T], bool], it: Iterable[T]) -> tuple[list[T], list[T]]:
    """Split into (matches, non_matches). Terminal operation."""
    matches: list[T] = []
    non_matches: list[T] = []
    for item in it:
        if pred(item):
            matches.append(item)
        else:
            non_matches.append(item)
    return matches, non_matches


async def partition_async[T](
    pred: Callable[[T], bool], it: AsyncIterable[T]
) -> tuple[list[T], list[T]]:
    """Split into (matches, non_matches). Terminal operation (async)."""
    matches: list[T] = []
    non_matches: list[T] = []
    async for item in it:
        if pred(item):
            matches.append(item)
        else:
            non_matches.append(item)
    return matches, non_matches
